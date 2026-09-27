#!/usr/bin/env bash
# =============================================================================
#  SoundLab Analytics - Tache 14, etape 2
#  Cree le role IAM que le conteneur Airflow assumera, et le VERIFIE.
#
#  Principe : la cle permanente reste sur le poste et ne sert qu'a une chose,
#  assumer ce role. Le conteneur ne recoit que des identifiants temporaires
#  portant des droits reduits au strict necessaire du DAG.
#
#      cle permanente (poste)
#            | sts:AssumeRole
#            v
#      identifiants temporaires, 1 h, droits restreints  ->  conteneur
#
#  Ferme l'action A4 de l'AIPD : separation du role d'exploitation et du
#  role d'administration.
#
#  Usage :  bash 01_role_airflow.sh
#  Idempotent : relancable. Met a jour la politique et la relation de
#  confiance si elles existent deja.
# =============================================================================
set -euo pipefail

RACINE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEPOT="$(cd "${RACINE}/.." && pwd)"
TMP="$(mktemp -d)"
trap 'rm -rf "${TMP}"' EXIT

bleu()  { printf '\033[0;36m%s\033[0m\n' "$*"; }
vert()  { printf '\033[0;32m%s\033[0m\n' "$*"; }
jaune() { printf '\033[0;33m%s\033[0m\n' "$*"; }
rouge() { printf '\033[0;31m%s\033[0m\n' "$*"; }

NOM_ROLE="SoundLabAirflowRole"
NOM_POLITIQUE="SoundLabAirflowPolicy"

# --- 0. Chargement de la configuration du projet ------------------------------
bleu "== Chargement de la configuration =="
if [[ ! -f "${DEPOT}/.soundlab.env" ]]; then
  rouge "Fichier ${DEPOT}/.soundlab.env introuvable."
  exit 1
fi
# shellcheck disable=SC1091
source "${DEPOT}/.soundlab.env"

for v in SL_REGION SL_ACCOUNT_ID SL_B_RAW SL_B_CUR SL_B_MOD SL_B_SCR SL_B_LOG \
         SL_EMR_APP_ID SL_EMR_ROLE_ARN SL_KMS_KEY_ARN SL_RS_WORKGROUP; do
  if [[ -z "${!v:-}" ]]; then
    rouge "Variable ${v} absente de .soundlab.env"
    exit 1
  fi
done
vert "  Configuration complete"

# --- 1. Garde-fou de region ---------------------------------------------------
EFF_REGION="${AWS_REGION:-${AWS_DEFAULT_REGION:-$(aws configure get region 2>/dev/null || true)}}"
if [[ "${EFF_REGION}" != "${SL_REGION}" ]]; then
  rouge "Region effective '${EFF_REGION}' differente de SL_REGION='${SL_REGION}'."
  exit 1
fi
vert "  Region : ${SL_REGION}"

# --- 2. Identite appelante ----------------------------------------------------
ARN_APPELANT="$(aws sts get-caller-identity --query Arn --output text)"
bleu ""
bleu "== Identite souche =="
vert "  ${ARN_APPELANT}"
if [[ "${ARN_APPELANT}" != arn:aws:iam::*:user/* ]]; then
  jaune "  Cette identite n'est pas un utilisateur IAM. La relation de confiance"
  jaune "  ci-dessous designe un utilisateur ; adapte-la si besoin."
fi

# --- 3. ARN des ressources ----------------------------------------------------
# Toutes les interpolations utilisent ${...} avec accolades : sans elles, zsh
# interprete $VAR:role comme un modificateur de parametre et corrompt l'ARN
# silencieusement. Incident 10.7 du journal technique.
ARN_APP_EMR="arn:aws:emr-serverless:${SL_REGION}:${SL_ACCOUNT_ID}:/applications/${SL_EMR_APP_ID}"

bleu ""
bleu "== Resolution de l'ARN du groupe de travail Redshift =="
ARN_WG="$(aws redshift-serverless get-workgroup \
            --workgroup-name "${SL_RS_WORKGROUP}" \
            --query 'workgroup.workgroupArn' --output text 2>/dev/null || echo "")"
if [[ -z "${ARN_WG}" || "${ARN_WG}" == "None" ]]; then
  rouge "Groupe de travail '${SL_RS_WORKGROUP}' introuvable."
  exit 1
fi
vert "  ${ARN_WG}"

# --- 4. Politique de permissions ---------------------------------------------
bleu ""
bleu "== Redaction de la politique de permissions =="
cat > "${TMP}/politique.json" <<FIN_POLITIQUE
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "SoumettreEtSuivreLesJobsEMR",
      "Effect": "Allow",
      "Action": [
        "emr-serverless:StartJobRun",
        "emr-serverless:GetJobRun",
        "emr-serverless:CancelJobRun",
        "emr-serverless:ListJobRuns",
        "emr-serverless:GetApplication",
        "emr-serverless:StartApplication"
      ],
      "Resource": [
        "${ARN_APP_EMR}",
        "${ARN_APP_EMR}/jobruns/*"
      ]
    },
    {
      "Sid": "TransmettreLeRoleDExecutionAEmrUniquement",
      "Effect": "Allow",
      "Action": "iam:PassRole",
      "Resource": "${SL_EMR_ROLE_ARN}",
      "Condition": {
        "StringEquals": { "iam:PassedToService": "emr-serverless.amazonaws.com" }
      }
    },
    {
      "Sid": "ListerLesCompartimentsDeSortieUniquement",
      "Effect": "Allow",
      "Action": [ "s3:ListBucket", "s3:GetBucketLocation" ],
      "Resource": [
        "arn:aws:s3:::${SL_B_CUR}",
        "arn:aws:s3:::${SL_B_MOD}",
        "arn:aws:s3:::${SL_B_SCR}",
        "arn:aws:s3:::${SL_B_LOG}"
      ]
    },
    {
      "Sid": "LireLesSortiesEtLesJournauxDExecution",
      "Effect": "Allow",
      "Action": "s3:GetObject",
      "Resource": [
        "arn:aws:s3:::${SL_B_CUR}/*",
        "arn:aws:s3:::${SL_B_MOD}/*",
        "arn:aws:s3:::${SL_B_SCR}/*",
        "arn:aws:s3:::${SL_B_LOG}/*"
      ]
    },
    {
      "Sid": "PublierLeCodeDesJobsUniquement",
      "Effect": "Allow",
      "Action": "s3:PutObject",
      "Resource": "arn:aws:s3:::${SL_B_SCR}/*"
    },
    {
      "Sid": "UtiliserLaCleDuProjet",
      "Effect": "Allow",
      "Action": [ "kms:Decrypt", "kms:DescribeKey", "kms:GenerateDataKey" ],
      "Resource": "${SL_KMS_KEY_ARN}"
    },
    {
      "Sid": "ExecuterDuSqlSurLeGroupeDeTravail",
      "Effect": "Allow",
      "Action": [
        "redshift-data:ExecuteStatement",
        "redshift-data:BatchExecuteStatement"
      ],
      "Resource": "${ARN_WG}"
    },
    {
      "Sid": "RelireSesPropresInstructions",
      "Effect": "Allow",
      "Action": [
        "redshift-data:DescribeStatement",
        "redshift-data:GetStatementResult",
        "redshift-data:ListStatements",
        "redshift-data:CancelStatement"
      ],
      "Resource": "*"
    },
    {
      "Sid": "ObtenirDesIdentifiantsRedshiftTemporaires",
      "Effect": "Allow",
      "Action": "redshift-serverless:GetCredentials",
      "Resource": "${ARN_WG}"
    }
  ]
}
FIN_POLITIQUE

# Validation avant soumission : une ARN corrompue produit sinon un
# MalformedPolicyDocument opaque.
if ! python3 -m json.tool "${TMP}/politique.json" > /dev/null; then
  rouge "JSON invalide - interpolation corrompue."
  cat "${TMP}/politique.json"
  exit 1
fi
vert "  JSON valide"

# Controle STRUCTUREL des ARN.
# On ne cherche pas des signatures de corruption connues - on verifie que
# chaque ARN a la forme attendue. Une corruption par modificateur zsh
# supprime un deux-points, donc reduit le nombre de segments : c'est ce que
# ce controle detecte, sans supposer a quoi ressemble l'erreur.
verifier_arn() {
  python3 - "$1" <<'FIN_PY'
import json, re, sys

texte = open(sys.argv[1], encoding="utf-8").read()
arns = sorted(set(re.findall(r'"(arn:[^"]*)"', texte)))
mauvaises = []

for arn in arns:
    segments = arn.split(":")
    # Forme canonique : arn:partition:service:region:compte:ressource
    if len(segments) < 6:
        mauvaises.append((arn, "seulement %d segments au lieu de 6" % len(segments)))
        continue
    compte = segments[4]
    if compte and not re.fullmatch(r"\d{12}", compte):
        mauvaises.append((arn, "identifiant de compte invalide : %r" % compte))
        continue
    if segments[1] != "aws":
        mauvaises.append((arn, "partition inattendue : %r" % segments[1]))

if mauvaises:
    print("  ARN mal formees :")
    for arn, motif in mauvaises:
        print("    %s" % arn)
        print("      -> %s" % motif)
    sys.exit(1)

print("  %d ARN verifiees, toutes bien formees" % len(arns))
FIN_PY
}

if ! verifier_arn "${TMP}/politique.json"; then
  rouge "Interpolation corrompue - politique non soumise."
  exit 1
fi

# --- 5. Relation de confiance -------------------------------------------------
bleu ""
bleu "== Redaction de la relation de confiance =="
cat > "${TMP}/confiance.json" <<FIN_CONFIANCE
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "SeulLUtilisateurSoucheePeutAssumerCeRole",
      "Effect": "Allow",
      "Principal": { "AWS": "${ARN_APPELANT}" },
      "Action": "sts:AssumeRole"
    }
  ]
}
FIN_CONFIANCE
python3 -m json.tool "${TMP}/confiance.json" > /dev/null
vert "  JSON valide - principal : ${ARN_APPELANT}"

# --- 6. Creation ou mise a jour de la politique geree --------------------------
bleu ""
bleu "== Politique geree ${NOM_POLITIQUE} =="
ARN_POLITIQUE="arn:aws:iam::${SL_ACCOUNT_ID}:policy/${NOM_POLITIQUE}"

if aws iam get-policy --policy-arn "${ARN_POLITIQUE}" >/dev/null 2>&1; then
  # Une politique geree conserve au plus 5 versions. On purge les anciennes
  # non par defaut avant d'en ajouter une, sinon la creation echoue.
  while IFS= read -r v; do
    [[ -z "${v}" ]] && continue
    aws iam delete-policy-version --policy-arn "${ARN_POLITIQUE}" --version-id "${v}" || true
    vert "  ancienne version ${v} supprimee"
  done < <(aws iam list-policy-versions --policy-arn "${ARN_POLITIQUE}" \
             --query 'Versions[?IsDefaultVersion==`false`].VersionId' --output text | tr '\t' '\n')

  aws iam create-policy-version --policy-arn "${ARN_POLITIQUE}" \
      --policy-document "file://${TMP}/politique.json" \
      --set-as-default >/dev/null
  vert "  nouvelle version publiee et activee"
else
  aws iam create-policy --policy-name "${NOM_POLITIQUE}" \
      --policy-document "file://${TMP}/politique.json" \
      --description "Droits minimaux de l'orchestrateur Airflow - projet SoundLab" >/dev/null
  vert "  politique creee"
fi

# --- 7. Creation ou mise a jour du role ---------------------------------------
bleu ""
bleu "== Role ${NOM_ROLE} =="
if aws iam get-role --role-name "${NOM_ROLE}" >/dev/null 2>&1; then
  aws iam update-assume-role-policy --role-name "${NOM_ROLE}" \
      --policy-document "file://${TMP}/confiance.json"
  vert "  relation de confiance mise a jour"
else
  aws iam create-role --role-name "${NOM_ROLE}" \
      --assume-role-policy-document "file://${TMP}/confiance.json" \
      --max-session-duration 14400 \
      --description "Orchestrateur Airflow - aucune permission d'administration" >/dev/null
  vert "  role cree (duree de session maximale : 4 h)"
fi

aws iam attach-role-policy --role-name "${NOM_ROLE}" --policy-arn "${ARN_POLITIQUE}"
vert "  politique rattachee"

ARN_ROLE="arn:aws:iam::${SL_ACCOUNT_ID}:role/${NOM_ROLE}"

# --- 7 bis. Attendre que la politique PUBLIEE soit la politique EVALUEE -------
# IAM est a coherence differee. Tester une politique juste apres l'avoir
# modifiee revient a mesurer l'ancienne : c'est exactement ce qui a produit
# un faux positif lors de la mise au point (incident 10.12 du journal).
#
# Plutot qu'un delai arbitraire, on interroge le simulateur jusqu'a ce qu'il
# reponde ce que la nouvelle politique implique. On attend la condition, pas
# un nombre de secondes.
bleu ""
bleu "== Attente de la propagation de la politique =="
PROPAGEE=0
for essai in $(seq 1 12); do
  DECISION="$(aws iam simulate-principal-policy                 --policy-source-arn "${ARN_ROLE}"                 --action-names s3:ListBucket                 --resource-arns "arn:aws:s3:::${SL_B_RAW}"                 --query 'EvaluationResults[0].EvalDecision'                 --output text 2>/dev/null || echo "")"
  if [[ "${DECISION}" == *"Deny"* ]]; then
    vert "  Politique evaluee a jour (decision sur raw : ${DECISION}, tentative ${essai})"
    PROPAGEE=1
    break
  fi
  jaune "  propagation en cours (decision sur raw : ${DECISION:-indisponible}) - ${essai}/12"
  sleep 5
done

if (( PROPAGEE == 0 )); then
  rouge "Apres 60 s, la politique evaluee n'est toujours pas celle publiee."
  rouge "Relance le script dans une minute."
  exit 1
fi

# Controle croise par simulation, avant meme d'assumer le role. Le simulateur
# repond a la question "qu'est-ce que ce role a le droit de faire", sans
# dependre de l'existence des objets ni du comportement de S3.
bleu ""
bleu "== Controle par simulation IAM =="
simuler() {
  local libelle="$1" action="$2" ressource="$3" attendu="$4"
  local decision
  decision="$(aws iam simulate-principal-policy                 --policy-source-arn "${ARN_ROLE}"                 --action-names "${action}"                 --resource-arns "${ressource}"                 --query 'EvaluationResults[0].EvalDecision'                 --output text 2>/dev/null || echo "erreur")"
  if [[ "${attendu}" == "allowed" && "${decision}" == "allowed" ]] \
     || [[ "${attendu}" == "denied" && "${decision}" == *"Deny"* ]]; then
    vert "  ${decision}  ${libelle}"
  else
    rouge "  ${decision}  ${libelle}  (attendu : ${attendu})"
    return 1
  fi
}

ECHECS_SIMU=0
simuler "lister curated"          s3:ListBucket "arn:aws:s3:::${SL_B_CUR}" allowed || ECHECS_SIMU=$((ECHECS_SIMU+1))
simuler "lister raw"              s3:ListBucket "arn:aws:s3:::${SL_B_RAW}" denied  || ECHECS_SIMU=$((ECHECS_SIMU+1))
simuler "lire un objet de raw"    s3:GetObject  "arn:aws:s3:::${SL_B_RAW}/x" denied || ECHECS_SIMU=$((ECHECS_SIMU+1))
simuler "ecrire dans curated"     s3:PutObject  "arn:aws:s3:::${SL_B_CUR}/x" denied || ECHECS_SIMU=$((ECHECS_SIMU+1))
simuler "publier dans scripts"    s3:PutObject  "arn:aws:s3:::${SL_B_SCR}/x" allowed || ECHECS_SIMU=$((ECHECS_SIMU+1))

if (( ECHECS_SIMU > 0 )); then
  rouge "${ECHECS_SIMU} ecart(s) entre la politique voulue et la politique evaluee."
  exit 1
fi

# --- 8. Enregistrement dans la configuration ----------------------------------
if grep -q '^export SL_AIRFLOW_ROLE_ARN=' "${DEPOT}/.soundlab.env" 2>/dev/null; then
  vert "  SL_AIRFLOW_ROLE_ARN deja present dans .soundlab.env"
else
  printf 'export SL_AIRFLOW_ROLE_ARN=%s\n' "${ARN_ROLE}" >> "${DEPOT}/.soundlab.env"
  vert "  SL_AIRFLOW_ROLE_ARN ajoute a .soundlab.env"
fi

# --- 9. VERIFICATION : le role fait-il ce qu'on croit ? -----------------------
bleu ""
bleu "== Verification du role - IAM est a coherence differee, on patiente =="

JETONS=""
for essai in 1 2 3 4 5 6; do
  if JETONS="$(aws sts assume-role \
        --role-arn "${ARN_ROLE}" \
        --role-session-name "verification-politique" \
        --duration-seconds 900 \
        --query 'Credentials.[AccessKeyId,SecretAccessKey,SessionToken]' \
        --output text 2>/dev/null)"; then
    vert "  Role assume (tentative ${essai})"
    break
  fi
  jaune "  propagation en cours, nouvelle tentative dans 5 s (${essai}/6)"
  sleep 5
done

if [[ -z "${JETONS}" ]]; then
  rouge "Impossible d'assumer le role apres 6 tentatives."
  exit 1
fi

read -r CLE SECRET JETON <<< "${JETONS}"

# Les identifiants temporaires deviennent l'identite active le temps des
# tests. AWS_PROFILE doit etre neutralise : sinon la chaine de resolution
# peut retomber sur la cle permanente et les tests mesureraient les droits
# de l'administrateur, pas ceux du role. Un test negatif qui echoue pour la
# mauvaise raison est pire qu'aucun test.
PROFIL_SAUVEGARDE="${AWS_PROFILE:-}"
unset AWS_PROFILE AWS_DEFAULT_PROFILE || true
export AWS_ACCESS_KEY_ID="${CLE}"
export AWS_SECRET_ACCESS_KEY="${SECRET}"
export AWS_SESSION_TOKEN="${JETON}"
export AWS_REGION="${SL_REGION}"

# Verification prealable : on agit bien sous l'identite du role.
IDENTITE_TEST="$(aws sts get-caller-identity --query Arn --output text 2>/dev/null || echo "")"
if [[ "${IDENTITE_TEST}" != *":assumed-role/${NOM_ROLE}/"* ]]; then
  rouge "Les tests ne s'executent pas sous le role attendu : ${IDENTITE_TEST}"
  exit 1
fi
vert "  Identite de test : ${IDENTITE_TEST}"

ECHECS=0

test_autorise() {
  local libelle="$1"; shift
  local sortie
  if sortie="$("$@" 2>&1)"; then
    vert "  AUTORISE  ${libelle}"
  else
    rouge "  REFUSE alors qu'il fallait autoriser : ${libelle}"
    printf '%s\n' "${sortie}" | head -3
    ECHECS=$((ECHECS + 1))
  fi
}

test_refuse() {
  local libelle="$1"; shift
  local sortie
  if sortie="$("$@" 2>&1)"; then
    rouge "  PROBLEME  ${libelle} a REUSSI alors qu'il fallait refuser"
    ECHECS=$((ECHECS + 1))
    return
  fi
  # Un echec ne suffit pas : il doit venir d'un refus de permission et non
  # d'une faute de frappe, d'une ressource absente ou d'un incident reseau.
  if printf '%s' "${sortie}" | grep -qiE 'AccessDenied|not authorized|explicit deny|AuthorizationError'; then
    vert "  REFUSE    ${libelle}  (motif de permission confirme)"
  else
    jaune "  INDECIS   ${libelle} a echoue, mais pas pour un motif de permission :"
    printf '%s\n' "${sortie}" | head -3
    ECHECS=$((ECHECS + 1))
  fi
}

bleu ""
bleu "-- Ce que l'orchestrateur DOIT pouvoir faire"
test_autorise "piloter l'application EMR" \
  aws emr-serverless get-application --application-id "${SL_EMR_APP_ID}"
test_autorise "lister le compartiment curated" \
  aws s3api list-objects-v2 --bucket "${SL_B_CUR}" --max-items 1
test_autorise "acceder aux journaux d'execution EMR" \
  aws s3api list-objects-v2 --bucket "${SL_B_LOG}" --max-items 1

bleu ""
bleu "-- Ce que l'orchestrateur NE DOIT PAS pouvoir faire"
test_refuse "lister les utilisateurs IAM" \
  aws iam list-users --max-items 1
test_refuse "enumerer tous les compartiments du compte" \
  aws s3api list-buckets
test_refuse "enumerer le compartiment des donnees brutes" \
  aws s3api list-objects-v2 --bucket "${SL_B_RAW}" --max-items 1
# Sans s3:ListBucket sur raw, S3 renvoie desormais AccessDenied quelle que
# soit l'existence de la cle : le refus n'est plus ambigu.
test_refuse "lire une donnee source dans raw" \
  aws s3api get-object --bucket "${SL_B_RAW}" --key "sonde-de-controle" /dev/null
test_refuse "ecrire dans curated" \
  aws s3api put-object --bucket "${SL_B_CUR}" --key "sonde-de-controle"

# Retablissement de l'identite souche.
unset AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN
if [[ -n "${PROFIL_SAUVEGARDE}" ]]; then
  export AWS_PROFILE="${PROFIL_SAUVEGARDE}"
fi
unset CLE SECRET JETON JETONS

if (( ECHECS > 0 )); then
  bleu ""
  rouge "${ECHECS} verification(s) en echec - le role ne fait pas ce qui est annonce."
  rouge "Ne pas le brancher sur Airflow en l'etat."
  exit 1
fi

bleu ""
vert "================================================================"
vert " Role verifie dans les deux sens."
vert ""
vert "   ${ARN_ROLE}"
vert ""
vert " Autorise : soumettre des jobs EMR, lire curated/models/scripts,"
vert "            publier le code des jobs, executer du SQL sur ${SL_RS_WORKGROUP}"
vert " Refuse   : toute action IAM, creation de compartiment, suppression,"
vert "            et tout ce qui n'est pas explicitement liste."
vert "================================================================"
