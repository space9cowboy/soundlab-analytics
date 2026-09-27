#!/usr/bin/env zsh
# 3v_12 (tache 3.3, E4) : role du Lambda de pilotage soundlab-3v-pilotage, puis simulation chemin par chemin.
# Droits : lire le registre et les manifestes incrementaux, ecrire le registre seul, lire la sortie du pilote
# Spark des jobs de l'application EMR, lister (noms seulement) curated et logs pour distinguer "absent" de
# "interdit", KMS alias/soundlab via S3 uniquement, lister les executions de la seule machine a etats 3.3.
# Aucun acces au sel, au bucket brut, ni aux Lambdas. Idempotent : role cree s'il manque, politique reecrite.
# v2 (tache 5.1) : cloudwatch:PutMetricData limite a l'espace de noms SoundLab/3V (metriques de fraicheur).
set -euo pipefail
COMPTE=589276558852; REGION=eu-north-1
ROLE=SoundLab3VLambdaPilotage; POL=SoundLab3VLambdaPilotageAcces
CLE=arn:aws:kms:${REGION}:${COMPTE}:key/716b5b3a-5b92-4540-a0de-355485eac79d
MACHINE=arn:aws:states:${REGION}:${COMPTE}:stateMachine:soundlab-3v-chargement-incremental
CUR=arn:aws:s3:::soundlab-curated-558852; LOG=arn:aws:s3:::soundlab-logs-558852; RAW=arn:aws:s3:::soundlab-raw-558852
T=$(mktemp -d)

cat > ${T}/confiance.json <<EOF
{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}
EOF
cat > ${T}/acces.json <<EOF
{"Version":"2012-10-17","Statement":[
 {"Sid":"LireRegistreEtManifestes","Effect":"Allow","Action":"s3:GetObject","Resource":[
   "${CUR}/trois_v/_etat/chargements_incr.json",
   "${CUR}/trois_v/_manifestes/listenbrainz/manifeste_listenbrainz_*_incremental.json"]},
 {"Sid":"EcrireRegistre","Effect":"Allow","Action":"s3:PutObject","Resource":"${CUR}/trois_v/_etat/chargements_incr.json"},
 {"Sid":"LireSortiePilote","Effect":"Allow","Action":"s3:GetObject",
  "Resource":"${LOG}/emr-serverless/applications/00g8l9brbs9e3f1d/jobs/*/SPARK_DRIVER/stdout.gz"},
 {"Sid":"ListerPourAbsence","Effect":"Allow","Action":"s3:ListBucket","Resource":["${CUR}","${LOG}"]},
 {"Sid":"KmsViaS3","Effect":"Allow","Action":["kms:Decrypt","kms:GenerateDataKey"],"Resource":"${CLE}",
  "Condition":{"StringEquals":{"kms:ViaService":"s3.${REGION}.amazonaws.com"}}},
 {"Sid":"GardeConcurrence","Effect":"Allow","Action":"states:ListExecutions","Resource":"${MACHINE}"},
 {"Sid":"MetriquesFraicheur","Effect":"Allow","Action":"cloudwatch:PutMetricData","Resource":"*",
  "Condition":{"StringEquals":{"cloudwatch:namespace":"SoundLab/3V"}}}
]}
EOF

if aws iam get-role --role-name ${ROLE} >/dev/null 2>&1; then
  echo "ROLE_EXISTANT ${ROLE}"
else
  aws iam create-role --role-name ${ROLE} --assume-role-policy-document file://${T}/confiance.json \
    --description "SoundLab 3V tache 3.3 : Lambda de pilotage (planifier, continuite, inscrire)" \
    --query 'Role.Arn' --output text
  aws iam attach-role-policy --role-name ${ROLE} \
    --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole
fi
aws iam put-role-policy --role-name ${ROLE} --policy-name ${POL} --policy-document file://${T}/acces.json
echo "POLITIQUE_ECRITE ${POL}"
ARN=$(aws iam get-role --role-name ${ROLE} --query 'Role.Arn' --output text)

# Simulation : action | ressource | cle de contexte (ou -) | valeur | attendu
ok=0; n=0
while IFS='|' read -r action ressource ckey cval attendu; do
  n=$((n+1))
  if [[ ${ckey} == - ]]; then
    r=$(aws iam simulate-principal-policy --policy-source-arn ${ARN} --action-names ${action} \
        --resource-arns ${ressource} --query 'EvaluationResults[0].EvalDecision' --output text)
  else
    r=$(aws iam simulate-principal-policy --policy-source-arn ${ARN} --action-names ${action} \
        --resource-arns ${ressource} \
        --context-entries ContextKeyName=${ckey},ContextKeyValues=${cval},ContextKeyType=string \
        --query 'EvaluationResults[0].EvalDecision' --output text)
  fi
  if [[ ${r} == ${attendu} ]]; then ok=$((ok+1)); s=OK; else s=ECHEC; fi
  printf '%-5s %2d %-26s %-14s %s\n' ${s} ${n} ${action} ${r} ${ressource#arn:aws:}
done <<EOF
s3:GetObject|${CUR}/trois_v/_etat/chargements_incr.json|-|-|allowed
s3:PutObject|${CUR}/trois_v/_etat/chargements_incr.json|-|-|allowed
s3:GetObject|${CUR}/trois_v/_manifestes/listenbrainz/manifeste_listenbrainz_2676_incremental.json|-|-|allowed
s3:PutObject|${CUR}/trois_v/_manifestes/listenbrainz/manifeste_listenbrainz_2676_incremental.json|-|-|implicitDeny
s3:GetObject|${LOG}/emr-serverless/applications/00g8l9brbs9e3f1d/jobs/00g92i6hfdmqjg1f/SPARK_DRIVER/stdout.gz|-|-|allowed
s3:GetObject|${LOG}/emr-serverless/applications/00g8l9brbs9e3f1d/jobs/00g92i6hfdmqjg1f/SPARK_DRIVER/stderr.gz|-|-|implicitDeny
s3:GetObject|${RAW}/listenbrainz/incrementaux/dump=2675/part-2675.json.zst|-|-|implicitDeny
s3:ListBucket|${CUR}|-|-|allowed
s3:ListBucket|${LOG}|-|-|allowed
s3:ListBucket|${RAW}|-|-|implicitDeny
kms:Decrypt|${CLE}|kms:ViaService|s3.${REGION}.amazonaws.com|allowed
kms:Decrypt|${CLE}|kms:ViaService|secretsmanager.${REGION}.amazonaws.com|implicitDeny
kms:GenerateDataKey|${CLE}|kms:ViaService|s3.${REGION}.amazonaws.com|allowed
states:ListExecutions|${MACHINE}|-|-|allowed
states:ListExecutions|arn:aws:states:${REGION}:${COMPTE}:stateMachine:autre|-|-|implicitDeny
secretsmanager:GetSecretValue|arn:aws:secretsmanager:${REGION}:${COMPTE}:secret:soundlab/pseudonymisation-salt-listenbrainz-Xt64Yk|-|-|implicitDeny
s3:DeleteObject|${CUR}/trois_v/_etat/chargements_incr.json|-|-|implicitDeny
cloudwatch:PutMetricData|*|cloudwatch:namespace|SoundLab/3V|allowed
cloudwatch:PutMetricData|*|cloudwatch:namespace|AWS/EMRServerless|implicitDeny
EOF
rm -rf ${T}
[[ ${ok} == ${n} ]] && echo "ROLE_PILOTAGE_OK ${ok}/${n} ${ARN}" || { echo "ROLE_PILOTAGE_ECHEC ${ok}/${n}"; exit 1; }
