#!/usr/bin/env bash
# =============================================================================
#  SoundLab Analytics — Bloc 6 Big Data
#  Provisionnement de l'entrepôt Redshift Serverless
#
#  Crée : 1 rôle IAM de chargement | 1 namespace | 1 workgroup | 1 limite
#         d'usage | les variables d'environnement associées
#
#  ---------------------------------------------------------------------------
#  TROIS GARDE-FOUS DE COÛT
#
#  Redshift est le seul service de la pile qui puisse facturer sans qu'aucun
#  traitement ne soit lancé. En eu-north-1 la capacité minimale est de 8 RPU,
#  soit environ 3 $/heure d'activité (facturation à la seconde, 60 s minimum,
#  rien à l'arrêt).
#
#   1. Capacité de base au minimum régional : 8 RPU.
#   2. Limite d'usage mensuelle avec désactivation automatique au dépassement.
#      C'est un plafond dur, pas une alerte : au-delà, l'entrepôt refuse de
#      travailler. Contrairement à AWS Budgets, qui se contente de prévenir.
#   3. Point d'accès NON public. On interroge l'entrepôt par la Data API,
#      qui passe par le plan de contrôle AWS et l'identité IAM de l'appelant.
#      Aucun port ouvert, aucun mot de passe à manipuler, aucun client SQL à
#      installer — et un point de moins dans la surface d'attaque.
#  ---------------------------------------------------------------------------
#
#  Usage :  ./infra/04_redshift.sh
# =============================================================================

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "${ROOT}/.soundlab.env"

NAMESPACE="soundlab-ns"
WORKGROUP="soundlab-wg"
BASE_DB="soundlab"
ROLE_REDSHIFT="SoundLabRedshiftS3Role"
RPU_BASE=8
LIMITE_RPU_HEURES=60          # ≈ 22 $ — bien au-delà du besoin réel

log()  { printf '\033[1;34m[%s]\033[0m %s\n' "$(date +%H:%M:%S)" "$*"; }
ok()   { printf '\033[1;32m  ✓\033[0m %s\n' "$*"; }
skip() { printf '\033[1;33m  =\033[0m %s (déjà présent)\n' "$*"; }
die()  { printf '\033[1;31m  ✗ %s\033[0m\n' "$*" >&2; exit 1; }

TMP="$(mktemp -d)"; trap 'rm -rf "${TMP}"' EXIT

# ----------------------------------------------------------------------------
# 0. Garde-fous de contexte
# ----------------------------------------------------------------------------
EFF_REGION="${AWS_REGION:-${AWS_DEFAULT_REGION:-$(aws configure get region 2>/dev/null || true)}}"
[[ "${EFF_REGION}" == "${SL_REGION}" ]] \
  || die "Région effective '${EFF_REGION}' ≠ '${SL_REGION}'"
ok "Compte ${SL_ACCOUNT_ID} / région ${SL_REGION}"

# ----------------------------------------------------------------------------
# 1. Réseau
#    Redshift Serverless exige au moins trois sous-réseaux répartis sur trois
#    zones de disponibilité distinctes — contrainte de haute disponibilité du
#    service, pas un choix de notre part. eu-north-1 en compte exactement trois.
# ----------------------------------------------------------------------------
log "Découverte du réseau"

VPC_ID="$(aws ec2 describe-vpcs --filters Name=isDefault,Values=true \
          --query 'Vpcs[0].VpcId' --output text)"
[[ "${VPC_ID}" != "None" ]] || die "Aucun VPC par défaut. Il faut en créer un."
ok "VPC ${VPC_ID}"

# Un sous-réseau par zone de disponibilité, pour garantir la répartition.
# Boucle `read` plutôt que `mapfile` : ce dernier exige bash 4, absent du
# bash 3.2 livré avec macOS.
SUBNETS=()
while IFS= read -r s; do
  [[ -n "${s}" ]] && SUBNETS+=("${s}")
done < <(
  aws ec2 describe-subnets --filters "Name=vpc-id,Values=${VPC_ID}" \
    --query 'Subnets[].[AvailabilityZone,SubnetId]' --output text \
  | sort -u -k1,1 | awk '{print $2}'
)
(( ${#SUBNETS[@]} >= 3 )) \
  || die "Seulement ${#SUBNETS[@]} zone(s) disponible(s), 3 requises."
ok "Sous-réseaux : ${SUBNETS[*]}"

SG_NAME="soundlab-redshift-sg"
SG_ID="$(aws ec2 describe-security-groups \
         --filters "Name=vpc-id,Values=${VPC_ID}" "Name=group-name,Values=${SG_NAME}" \
         --query 'SecurityGroups[0].GroupId' --output text 2>/dev/null || echo "None")"
if [[ "${SG_ID}" == "None" || -z "${SG_ID}" ]]; then
  SG_ID="$(aws ec2 create-security-group --group-name "${SG_NAME}" \
           --description "Redshift Serverless SoundLab - aucun flux entrant" \
           --vpc-id "${VPC_ID}" --query GroupId --output text)"
  # Aucune règle entrante : le point d'accès n'étant pas public et les requêtes
  # passant par la Data API, aucun flux réseau direct n'est nécessaire.
  ok "Groupe de sécurité ${SG_ID} créé (aucune règle entrante)"
else
  skip "groupe de sécurité ${SG_ID}"
fi

# ----------------------------------------------------------------------------
# 2. Rôle IAM de chargement
#    Redshift assume ce rôle pour lire les fichiers Parquet dans S3 lors des
#    commandes COPY. Il lui faut aussi kms:Decrypt : le bucket curated est
#    chiffré par la CMK du projet, et sans ce droit le COPY échoue avec une
#    erreur d'accès peu explicite.
# ----------------------------------------------------------------------------
log "Rôle IAM de chargement"

cat > "${TMP}/trust.json" <<'JSON'
{"Version":"2012-10-17","Statement":[
 {"Effect":"Allow",
  "Principal":{"Service":["redshift.amazonaws.com","redshift-serverless.amazonaws.com"]},
  "Action":"sts:AssumeRole"}]}
JSON

if aws iam get-role --role-name "${ROLE_REDSHIFT}" >/dev/null 2>&1; then
  skip "rôle ${ROLE_REDSHIFT}"
  aws iam update-assume-role-policy --role-name "${ROLE_REDSHIFT}" \
    --policy-document "file://${TMP}/trust.json"
else
  aws iam create-role --role-name "${ROLE_REDSHIFT}" \
    --description "Chargement COPY depuis S3 vers Redshift Serverless" \
    --assume-role-policy-document "file://${TMP}/trust.json" \
    --tags Key=Project,Value=SoundLab Key=Env,Value=dev >/dev/null
  ok "rôle ${ROLE_REDSHIFT} créé"
fi

cat > "${TMP}/policy.json" <<JSON
{"Version":"2012-10-17","Statement":[
 {"Sid":"LectureCurated","Effect":"Allow",
  "Action":["s3:GetObject","s3:ListBucket","s3:GetBucketLocation"],
  "Resource":["arn:aws:s3:::${SL_B_CUR}","arn:aws:s3:::${SL_B_CUR}/*"]},
 {"Sid":"EcritureUnload","Effect":"Allow",
  "Action":["s3:PutObject","s3:DeleteObject"],
  "Resource":["arn:aws:s3:::${SL_B_LOG}/redshift-unload/*"]},
 {"Sid":"Dechiffrement","Effect":"Allow",
  "Action":["kms:Decrypt","kms:DescribeKey","kms:GenerateDataKey"],
  "Resource":"${SL_KMS_KEY_ARN}"},
 {"Sid":"CatalogueGlue","Effect":"Allow",
  "Action":["glue:GetDatabase*","glue:GetTable*","glue:GetPartition*"],
  "Resource":["arn:aws:glue:${SL_REGION}:${SL_ACCOUNT_ID}:catalog",
              "arn:aws:glue:${SL_REGION}:${SL_ACCOUNT_ID}:database/${SL_GLUE_DB}",
              "arn:aws:glue:${SL_REGION}:${SL_ACCOUNT_ID}:table/${SL_GLUE_DB}/*"]}
]}
JSON

aws iam put-role-policy --role-name "${ROLE_REDSHIFT}" \
  --policy-name "SoundLabRedshiftAccess" \
  --policy-document "file://${TMP}/policy.json"
ok "politique appliquée (moindre privilège)"

ROLE_ARN="$(aws iam get-role --role-name "${ROLE_REDSHIFT}" --query 'Role.Arn' --output text)"

# ----------------------------------------------------------------------------
# 3. Namespace — le conteneur logique : base, utilisateur, chiffrement, rôles
#    On tente d'abord avec la CMK du projet ; en cas de refus lié à la politique
#    de clé, on retombe sur la clé gérée par AWS. Les données restent chiffrées
#    dans les deux cas ; seule la maîtrise de la clé diffère.
# ----------------------------------------------------------------------------
log "Namespace ${NAMESPACE}"

if aws redshift-serverless get-namespace --namespace-name "${NAMESPACE}" >/dev/null 2>&1; then
  skip "namespace ${NAMESPACE}"
else
  creer_namespace() {
    aws redshift-serverless create-namespace \
      --namespace-name "${NAMESPACE}" \
      --db-name "${BASE_DB}" \
      --admin-username soundlabadmin \
      --manage-admin-password \
      --default-iam-role-arn "${ROLE_ARN}" \
      --iam-roles "${ROLE_ARN}" \
      --tags key=Project,value=SoundLab key=Env,value=dev \
      "$@" >/dev/null
  }
  if creer_namespace --kms-key-id "${SL_KMS_KEY_ARN}" 2>"${TMP}/err"; then
    ok "namespace créé, chiffré par la CMK du projet"
  else
    printf '\033[1;33m  !\033[0m CMK refusée, repli sur la clé gérée par AWS\n'
    sed 's/^/      /' "${TMP}/err" >&2 || true
    creer_namespace
    ok "namespace créé (clé gérée par AWS)"
  fi
fi

# ----------------------------------------------------------------------------
# 4. Workgroup — la capacité de calcul
# ----------------------------------------------------------------------------
log "Workgroup ${WORKGROUP}"

if aws redshift-serverless get-workgroup --workgroup-name "${WORKGROUP}" >/dev/null 2>&1; then
  skip "workgroup ${WORKGROUP}"
else
  aws redshift-serverless create-workgroup \
    --workgroup-name "${WORKGROUP}" \
    --namespace-name "${NAMESPACE}" \
    --base-capacity "${RPU_BASE}" \
    --no-publicly-accessible \
    --subnet-ids "${SUBNETS[@]:0:3}" \
    --security-group-ids "${SG_ID}" \
    --tags key=Project,value=SoundLab key=Env,value=dev >/dev/null
  ok "workgroup créé — ${RPU_BASE} RPU, point d'accès non public"
fi

log "Attente de la disponibilité (compter 3 à 8 minutes)"
DEBUT="$(date +%s)"
while true; do
  ETAT="$(aws redshift-serverless get-workgroup --workgroup-name "${WORKGROUP}" \
          --query 'workgroup.status' --output text)"
  printf '\r  %-12s  %3ds écoulées ' "${ETAT}" "$(( $(date +%s) - DEBUT ))"
  [[ "${ETAT}" == "AVAILABLE" ]] && { echo; break; }
  [[ "${ETAT}" == "MODIFYING" || "${ETAT}" == "CREATING" ]] || { echo; die "État inattendu : ${ETAT}"; }
  sleep 15
done
ok "workgroup disponible"

# ----------------------------------------------------------------------------
# 5. Limite d'usage — plafond dur
#    breach-action=deactivate suspend l'entrepôt au dépassement. C'est la
#    différence essentielle avec AWS Budgets, qui envoie un courriel et laisse
#    la facture courir.
# ----------------------------------------------------------------------------
log "Limite d'usage mensuelle"

WG_ARN="$(aws redshift-serverless get-workgroup --workgroup-name "${WORKGROUP}" \
          --query 'workgroup.workgroupArn' --output text)"

EXISTANTE="$(aws redshift-serverless list-usage-limits --resource-arn "${WG_ARN}" \
             --query 'usageLimits[0].usageLimitId' --output text 2>/dev/null || echo "None")"
if [[ "${EXISTANTE}" != "None" && -n "${EXISTANTE}" ]]; then
  skip "limite d'usage ${EXISTANTE}"
else
  aws redshift-serverless create-usage-limit \
    --resource-arn "${WG_ARN}" \
    --usage-type serverless-compute \
    --amount "${LIMITE_RPU_HEURES}" \
    --period monthly \
    --breach-action deactivate >/dev/null
  ok "plafond de ${LIMITE_RPU_HEURES} RPU-heures/mois — désactivation au dépassement"
fi

# ----------------------------------------------------------------------------
# 6. Vérification de bout en bout par la Data API
# ----------------------------------------------------------------------------
log "Test de connexion (Data API, identité IAM)"

ID_REQ="$(aws redshift-data execute-statement \
          --workgroup-name "${WORKGROUP}" --database "${BASE_DB}" \
          --sql "SELECT current_user AS utilisateur, version() AS version;" \
          --query Id --output text)"

for _ in $(seq 1 20); do
  ETAT_REQ="$(aws redshift-data describe-statement --id "${ID_REQ}" --query Status --output text)"
  [[ "${ETAT_REQ}" == "FINISHED" || "${ETAT_REQ}" == "FAILED" ]] && break
  sleep 3
done

if [[ "${ETAT_REQ}" == "FINISHED" ]]; then
  aws redshift-data get-statement-result --id "${ID_REQ}" \
    --query 'Records[0][].stringValue' --output text | sed 's/^/      /'
  ok "connexion établie sans mot de passe ni port ouvert"
else
  aws redshift-data describe-statement --id "${ID_REQ}" --query Error --output text >&2
  die "Échec du test de connexion"
fi

# ----------------------------------------------------------------------------
# 7. Variables d'environnement
# ----------------------------------------------------------------------------
# sed -i portable : GNU exige `-i`, BSD/macOS exige `-i ''`
sed_inplace() {
  if sed --version >/dev/null 2>&1; then sed -i "$@"; else sed -i '' "$@"; fi
}
maj_env() {
  local cle="$1" val="$2"
  if grep -q "^export ${cle}=" "${ROOT}/.soundlab.env"; then
    sed_inplace "s|^export ${cle}=.*|export ${cle}=${val}|" "${ROOT}/.soundlab.env"
  else
    echo "export ${cle}=${val}" >> "${ROOT}/.soundlab.env"
  fi
}
maj_env SL_RS_NAMESPACE "${NAMESPACE}"
maj_env SL_RS_WORKGROUP "${WORKGROUP}"
maj_env SL_RS_DB        "${BASE_DB}"
maj_env SL_RS_ROLE_ARN  "${ROLE_ARN}"

echo
log "Entrepôt prêt"
cat <<REC

  Récapitulatif
  ─────────────
  Namespace       ${NAMESPACE}   (base : ${BASE_DB})
  Workgroup       ${WORKGROUP}   — ${RPU_BASE} RPU, non public
  Rôle COPY       ${ROLE_REDSHIFT}
  Plafond dur     ${LIMITE_RPU_HEURES} RPU-heures/mois, désactivation au dépassement
  Accès           Data API, identité IAM — aucun mot de passe, aucun port ouvert

  Recharge l'environnement :  source .soundlab.env

REC
