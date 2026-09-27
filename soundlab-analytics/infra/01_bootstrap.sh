#!/usr/bin/env bash
# =============================================================================
#  SoundLab Analytics — Bloc 6 Big Data
#  Étape 1 : provisionnement de l'infrastructure socle
#
#  Crée : 5 buckets S3 chiffrés  |  1 clé KMS  |  1 rôle d'exécution EMR
#         Serverless  |  1 trail CloudTrail  |  1 secret de pseudonymisation
#         |  1 base Glue
#
#  Propriétés :
#   - IDEMPOTENT : relançable sans effet de bord ni doublon.
#   - MONORÉGION : refuse de s'exécuter si le profil AWS pointe ailleurs.
#   - AUCUN SECRET EN DUR : le sel de hachage est généré localement puis
#     stocké dans Secrets Manager, jamais écrit sur disque.
#
#  Usage :  AWS_PROFILE=soundlab ./01_bootstrap.sh
#  Auteur : Loïc Rabetsanta
# =============================================================================

set -euo pipefail

# ----------------------------------------------------------------------------
# Paramètres du projet
# ----------------------------------------------------------------------------
export AWS_PROFILE="${AWS_PROFILE:-soundlab}"
REGION="eu-north-1"                     # Stockholm — UE, tarif le plus bas
PROJECT="soundlab"
GLUE_DB="soundlab_curated"
TRAIL_NAME="soundlab-trail"
EMR_ROLE="SoundLabEMRServerlessExecutionRole"
SECRET_NAME="soundlab/pseudonymisation-salt"
KMS_ALIAS="alias/soundlab"

TAGS_CLI="Key=Project,Value=SoundLab Key=Env,Value=dev Key=Owner,Value=loic"

log()  { printf '\033[1;34m[%s]\033[0m %s\n' "$(date +%H:%M:%S)" "$*"; }
ok()   { printf '\033[1;32m  ✓\033[0m %s\n' "$*"; }
skip() { printf '\033[1;33m  =\033[0m %s (déjà présent)\n' "$*"; }
die()  { printf '\033[1;31m  ✗ %s\033[0m\n' "$*" >&2; exit 1; }

TMP="$(mktemp -d)"; trap 'rm -rf "$TMP"' EXIT

# ----------------------------------------------------------------------------
# 0. Garde-fous — c'est ce bloc qui empêche de rejouer les erreurs du 1er compte
# ----------------------------------------------------------------------------
log "Vérification du contexte AWS"

CFG_REGION="$(aws configure get region 2>/dev/null || true)"
[[ "$CFG_REGION" == "$REGION" ]] \
  || die "Le profil '$AWS_PROFILE' pointe sur '${CFG_REGION:-<vide>}' au lieu de '$REGION'."

if [[ -n "${AWS_REGION:-}${AWS_DEFAULT_REGION:-}" ]]; then
  [[ "${AWS_REGION:-$REGION}" == "$REGION" && "${AWS_DEFAULT_REGION:-$REGION}" == "$REGION" ]] \
    || die "Une variable d'environnement AWS_REGION/AWS_DEFAULT_REGION écrase le profil."
fi

CALLER_ARN="$(aws sts get-caller-identity --query Arn --output text)"
ACCOUNT_ID="$(aws sts get-caller-identity --query Account --output text)"
SFX="${ACCOUNT_ID: -6}"
ok "Identité : $CALLER_ARN"
ok "Région   : $REGION — suffixe de nommage : $SFX"

# Noms de buckets — globalement uniques grâce au suffixe de compte
B_RAW="${PROJECT}-raw-${SFX}"
B_CUR="${PROJECT}-curated-${SFX}"
B_MOD="${PROJECT}-models-${SFX}"
B_SCR="${PROJECT}-scripts-${SFX}"
B_LOG="${PROJECT}-logs-${SFX}"

# ----------------------------------------------------------------------------
# 1. Clé KMS gérée par le client
#    Pourquoi une CMK plutôt que le chiffrement S3 par défaut : elle donne une
#    politique de clé auditable, une rotation annuelle automatique et une trace
#    CloudTrail de chaque déchiffrement — c'est le livrable « sécurisation des
#    données » du brief. Le surcoût est de ~1 $/mois.
# ----------------------------------------------------------------------------
log "Clé KMS"

if KEY_ARN="$(aws kms describe-key --key-id "$KMS_ALIAS" \
              --query 'KeyMetadata.Arn' --output text 2>/dev/null)"; then
  skip "clé KMS $KMS_ALIAS"
else
  KEY_ID="$(aws kms create-key \
    --description "SoundLab Analytics - chiffrement des donnees au repos" \
    --key-usage ENCRYPT_DECRYPT --key-spec SYMMETRIC_DEFAULT \
    --tags TagKey=Project,TagValue=SoundLab TagKey=Env,TagValue=dev \
    --query 'KeyMetadata.KeyId' --output text)"
  aws kms create-alias --alias-name "$KMS_ALIAS" --target-key-id "$KEY_ID"
  aws kms enable-key-rotation --key-id "$KEY_ID"
  KEY_ARN="$(aws kms describe-key --key-id "$KEY_ID" --query 'KeyMetadata.Arn' --output text)"
  ok "clé KMS créée, rotation annuelle activée"
fi
ok "KEY_ARN=$KEY_ARN"

# ----------------------------------------------------------------------------
# 2. Buckets S3
#    raw     : données brutes Kaggle, immuables (versioning)
#    curated : Parquet produit par PySpark
#    models  : artefacts MLflow, modèle sérialisé
#    scripts : jobs PySpark soumis à EMR Serverless
#    logs    : journaux EMR + CloudTrail
# ----------------------------------------------------------------------------
log "Buckets S3"

create_bucket() {
  local b="$1" enc="$2"      # enc = kms | aes

  if aws s3api head-bucket --bucket "$b" 2>/dev/null; then
    skip "bucket s3://$b"
  else
    aws s3api create-bucket --bucket "$b" --region "$REGION" \
      --create-bucket-configuration "LocationConstraint=$REGION" >/dev/null
    ok "bucket s3://$b créé"
  fi

  # Aucun accès public, en aucune circonstance
  aws s3api put-public-access-block --bucket "$b" \
    --public-access-block-configuration \
    "BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true"

  # Versioning : protège d'un écrasement accidentel d'un jeu de données
  aws s3api put-bucket-versioning --bucket "$b" \
    --versioning-configuration Status=Enabled

  # Chiffrement au repos ; BucketKeyEnabled divise les appels KMS par ~100
  if [[ "$enc" == "kms" ]]; then
    cat > "$TMP/enc.json" <<JSON
{"Rules":[{"ApplyServerSideEncryptionByDefault":
  {"SSEAlgorithm":"aws:kms","KMSMasterKeyID":"$KEY_ARN"},"BucketKeyEnabled":true}]}
JSON
  else
    cat > "$TMP/enc.json" <<'JSON'
{"Rules":[{"ApplyServerSideEncryptionByDefault":{"SSEAlgorithm":"AES256"},"BucketKeyEnabled":true}]}
JSON
  fi
  aws s3api put-bucket-encryption --bucket "$b" \
    --server-side-encryption-configuration "file://$TMP/enc.json"

  # Hygiène de coût : purge les uploads multipart abandonnés et les vieilles
  # versions non courantes (un job Spark interrompu en laisse beaucoup)
  cat > "$TMP/lc.json" <<'JSON'
{"Rules":[
 {"ID":"abort-incomplete-mpu","Status":"Enabled","Filter":{"Prefix":""},
  "AbortIncompleteMultipartUpload":{"DaysAfterInitiation":7}},
 {"ID":"expire-noncurrent","Status":"Enabled","Filter":{"Prefix":""},
  "NoncurrentVersionExpiration":{"NoncurrentDays":30}}
]}
JSON
  aws s3api put-bucket-lifecycle-configuration --bucket "$b" \
    --lifecycle-configuration "file://$TMP/lc.json" >/dev/null

  aws s3api put-bucket-tagging --bucket "$b" --tagging \
    'TagSet=[{Key=Project,Value=SoundLab},{Key=Env,Value=dev},{Key=Owner,Value=loic}]'
}

create_bucket "$B_RAW" kms
create_bucket "$B_CUR" kms
create_bucket "$B_MOD" kms
create_bucket "$B_SCR" kms
create_bucket "$B_LOG" aes      # CloudTrail écrit ici : AES256 évite une
                                # politique de clé supplémentaire

# TLS obligatoire sur tous les buckets (refuse tout appel en HTTP clair)
log "Politiques de bucket — TLS obligatoire"
for b in "$B_RAW" "$B_CUR" "$B_MOD" "$B_SCR"; do
  cat > "$TMP/pol.json" <<JSON
{"Version":"2012-10-17","Statement":[
 {"Sid":"DenyInsecureTransport","Effect":"Deny","Principal":"*","Action":"s3:*",
  "Resource":["arn:aws:s3:::$b","arn:aws:s3:::$b/*"],
  "Condition":{"Bool":{"aws:SecureTransport":"false"}}}
]}
JSON
  aws s3api put-bucket-policy --bucket "$b" --policy "file://$TMP/pol.json"
  ok "politique TLS appliquée à $b"
done

# Bucket de logs : TLS + autorisation d'écriture pour CloudTrail
cat > "$TMP/pol_log.json" <<JSON
{"Version":"2012-10-17","Statement":[
 {"Sid":"DenyInsecureTransport","Effect":"Deny","Principal":"*","Action":"s3:*",
  "Resource":["arn:aws:s3:::$B_LOG","arn:aws:s3:::$B_LOG/*"],
  "Condition":{"Bool":{"aws:SecureTransport":"false"}}},
 {"Sid":"AWSCloudTrailAclCheck","Effect":"Allow",
  "Principal":{"Service":"cloudtrail.amazonaws.com"},
  "Action":"s3:GetBucketAcl","Resource":"arn:aws:s3:::$B_LOG",
  "Condition":{"StringEquals":{"aws:SourceArn":"arn:aws:cloudtrail:$REGION:$ACCOUNT_ID:trail/$TRAIL_NAME"}}},
 {"Sid":"AWSCloudTrailWrite","Effect":"Allow",
  "Principal":{"Service":"cloudtrail.amazonaws.com"},
  "Action":"s3:PutObject","Resource":"arn:aws:s3:::$B_LOG/AWSLogs/$ACCOUNT_ID/*",
  "Condition":{"StringEquals":{
    "s3:x-amz-acl":"bucket-owner-full-control",
    "aws:SourceArn":"arn:aws:cloudtrail:$REGION:$ACCOUNT_ID:trail/$TRAIL_NAME"}}}
]}
JSON
aws s3api put-bucket-policy --bucket "$B_LOG" --policy "file://$TMP/pol_log.json"
ok "politique CloudTrail + TLS appliquée à $B_LOG"

# ----------------------------------------------------------------------------
# 3. Secret de pseudonymisation (tâche 8)
#    Le sel ne doit jamais transiter par un script ni par Git : sans lui, les
#    user_id hachés ne sont pas ré-identifiables par force brute — c'est
#    l'argument central du DPIA.
# ----------------------------------------------------------------------------
log "Secret de pseudonymisation"

if aws secretsmanager describe-secret --secret-id "$SECRET_NAME" >/dev/null 2>&1; then
  skip "secret $SECRET_NAME"
else
  SALT="$(openssl rand -hex 32)"
  aws secretsmanager create-secret \
    --name "$SECRET_NAME" \
    --description "Sel de hachage SHA-256 des user_id du Taste Profile" \
    --secret-string "{\"salt\":\"$SALT\"}" \
    --kms-key-id "$KEY_ARN" \
    --tags Key=Project,Value=SoundLab Key=Env,Value=dev >/dev/null
  unset SALT
  ok "secret créé et chiffré avec la CMK du projet"
fi
SECRET_ARN="$(aws secretsmanager describe-secret --secret-id "$SECRET_NAME" \
              --query ARN --output text)"

# ----------------------------------------------------------------------------
# 4. Rôle d'exécution EMR Serverless
#    Un seul rôle, contre trois pour un cluster EMR sur EC2 — c'est ce qui
#    bloquait sur le compte précédent.
# ----------------------------------------------------------------------------
log "Rôle IAM EMR Serverless"

cat > "$TMP/trust.json" <<'JSON'
{"Version":"2012-10-17","Statement":[
 {"Effect":"Allow","Principal":{"Service":"emr-serverless.amazonaws.com"},
  "Action":"sts:AssumeRole"}]}
JSON

if aws iam get-role --role-name "$EMR_ROLE" >/dev/null 2>&1; then
  skip "rôle $EMR_ROLE"
  aws iam update-assume-role-policy --role-name "$EMR_ROLE" \
    --policy-document "file://$TMP/trust.json"
else
  aws iam create-role --role-name "$EMR_ROLE" \
    --description "Execution role des jobs PySpark SoundLab sur EMR Serverless" \
    --assume-role-policy-document "file://$TMP/trust.json" \
    --tags Key=Project,Value=SoundLab Key=Env,Value=dev >/dev/null
  ok "rôle $EMR_ROLE créé"
fi

# Politique au moindre privilège : bornée aux 5 buckets du projet,
# à la CMK du projet et au seul secret de pseudonymisation.
cat > "$TMP/emr_policy.json" <<JSON
{"Version":"2012-10-17","Statement":[
 {"Sid":"LectureSources","Effect":"Allow",
  "Action":["s3:GetObject","s3:ListBucket","s3:GetBucketLocation"],
  "Resource":["arn:aws:s3:::$B_RAW","arn:aws:s3:::$B_RAW/*",
              "arn:aws:s3:::$B_SCR","arn:aws:s3:::$B_SCR/*",
              "arn:aws:s3:::$B_CUR","arn:aws:s3:::$B_CUR/*"]},
 {"Sid":"EcritureResultats","Effect":"Allow",
  "Action":["s3:PutObject","s3:DeleteObject","s3:AbortMultipartUpload",
            "s3:ListBucketMultipartUploads","s3:ListMultipartUploadParts"],
  "Resource":["arn:aws:s3:::$B_CUR/*","arn:aws:s3:::$B_MOD/*","arn:aws:s3:::$B_LOG/*"]},
 {"Sid":"ListeCibles","Effect":"Allow",
  "Action":["s3:ListBucket","s3:GetBucketLocation"],
  "Resource":["arn:aws:s3:::$B_MOD","arn:aws:s3:::$B_LOG"]},
 {"Sid":"Chiffrement","Effect":"Allow",
  "Action":["kms:Encrypt","kms:Decrypt","kms:ReEncrypt*",
            "kms:GenerateDataKey*","kms:DescribeKey"],
  "Resource":"$KEY_ARN"},
 {"Sid":"SelDePseudonymisation","Effect":"Allow",
  "Action":["secretsmanager:GetSecretValue"],
  "Resource":"$SECRET_ARN"},
 {"Sid":"CatalogueGlue","Effect":"Allow",
  "Action":["glue:GetDatabase*","glue:GetTable*","glue:CreateTable","glue:UpdateTable",
            "glue:GetPartition*","glue:BatchCreatePartition","glue:CreateDatabase"],
  "Resource":["arn:aws:glue:$REGION:$ACCOUNT_ID:catalog",
              "arn:aws:glue:$REGION:$ACCOUNT_ID:database/$GLUE_DB",
              "arn:aws:glue:$REGION:$ACCOUNT_ID:table/$GLUE_DB/*"]},
 {"Sid":"Journalisation","Effect":"Allow",
  "Action":["logs:CreateLogGroup","logs:CreateLogStream","logs:PutLogEvents",
            "logs:DescribeLogStreams","logs:DescribeLogGroups"],
  "Resource":"arn:aws:logs:$REGION:$ACCOUNT_ID:log-group:/aws/emr-serverless/*"}
]}
JSON

aws iam put-role-policy --role-name "$EMR_ROLE" \
  --policy-name "SoundLabEMRServerlessAccess" \
  --policy-document "file://$TMP/emr_policy.json"
ok "politique en ligne appliquée (moindre privilège)"

EMR_ROLE_ARN="$(aws iam get-role --role-name "$EMR_ROLE" --query 'Role.Arn' --output text)"

# ----------------------------------------------------------------------------
# 5. Base Glue — catalogue des tables Parquet, interrogeable via Athena
# ----------------------------------------------------------------------------
log "Base Glue"
if aws glue get-database --name "$GLUE_DB" >/dev/null 2>&1; then
  skip "base Glue $GLUE_DB"
else
  aws glue create-database --database-input \
    "{\"Name\":\"$GLUE_DB\",\"Description\":\"Tables curated SoundLab Analytics\",\
\"LocationUri\":\"s3://$B_CUR/\"}"
  ok "base Glue $GLUE_DB créée"
fi

# ----------------------------------------------------------------------------
# 6. CloudTrail — traçabilité de tous les appels d'API (livrable « surveillance
#    et audit des accès » du brief). Les événements de gestion du premier trail
#    d'un compte sont gratuits ; seul le stockage S3 est facturé (~0,02 $/mois).
# ----------------------------------------------------------------------------
log "CloudTrail"
if aws cloudtrail get-trail --name "$TRAIL_NAME" >/dev/null 2>&1; then
  skip "trail $TRAIL_NAME"
else
  aws cloudtrail create-trail --name "$TRAIL_NAME" \
    --s3-bucket-name "$B_LOG" --s3-key-prefix "" \
    --is-multi-region-trail --enable-log-file-validation \
    --tags-list Key=Project,Value=SoundLab >/dev/null
  ok "trail $TRAIL_NAME créé (multirégion, validation d'intégrité activée)"
fi
aws cloudtrail start-logging --name "$TRAIL_NAME"
ok "journalisation active"

# ----------------------------------------------------------------------------
# 7. Fichier d'environnement — source unique de vérité pour les étapes suivantes
#    (déjà couvert par .gitignore : il contient des ARN, pas des secrets)
# ----------------------------------------------------------------------------
ENV_FILE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/.soundlab.env"
cat > "$ENV_FILE" <<ENVF
# Généré par 01_bootstrap.sh le $(date -u +%Y-%m-%dT%H:%M:%SZ)
export AWS_PROFILE=soundlab
export SL_REGION=$REGION
export SL_ACCOUNT_ID=$ACCOUNT_ID
export SL_B_RAW=$B_RAW
export SL_B_CUR=$B_CUR
export SL_B_MOD=$B_MOD
export SL_B_SCR=$B_SCR
export SL_B_LOG=$B_LOG
export SL_KMS_KEY_ARN=$KEY_ARN
export SL_EMR_ROLE_ARN=$EMR_ROLE_ARN
export SL_SECRET_ARN=$SECRET_ARN
export SL_GLUE_DB=$GLUE_DB
ENVF

echo
log "Étape 1 terminée"
cat <<REC

  Récapitulatif
  ─────────────
  Région          $REGION
  Buckets         s3://$B_RAW
                  s3://$B_CUR
                  s3://$B_MOD
                  s3://$B_SCR
                  s3://$B_LOG
  Clé KMS         $KMS_ALIAS
  Rôle EMR        $EMR_ROLE
  Secret          $SECRET_NAME
  Base Glue       $GLUE_DB
  CloudTrail      $TRAIL_NAME (multirégion)

  Variables écrites dans : $ENV_FILE
  Charge-les avant chaque session :  source .soundlab.env

REC
