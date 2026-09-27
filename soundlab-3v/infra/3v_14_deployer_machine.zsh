#!/usr/bin/env zsh
# 3v_14 (tache 3.3, E3/E4) : deploie la machine a etats soundlab-3v-chargement-incremental.
# 1. Lambda de pilotage v2 (inscrire lit l'identifiant du job dans la sortie de startJobRun.sync).
# 2. Role SoundLab3VStepFunctionsChargement : invoquer les deux Lambdas, lancer/suivre/annuler un job de
#    l'application EMR, transmettre le seul role d'execution EMR a EMR Serverless, regle EventBridge geree
#    par Step Functions pour le mode .sync. Aucun acces S3, KMS ni secret.
# 3. Validation de la definition par AWS, creation (ou mise a jour) de la machine. AUCUNE execution lancee.
# 4. Simulation du role chemin par chemin.
set -euo pipefail
COMPTE=589276558852; REGION=eu-north-1
ROLE=SoundLab3VStepFunctionsChargement; POL=SoundLab3VStepFunctionsChargementAcces
NOM=soundlab-3v-chargement-incremental
MACHINE=arn:aws:states:${REGION}:${COMPTE}:stateMachine:${NOM}
FN=arn:aws:lambda:${REGION}:${COMPTE}:function
APP=arn:aws:emr-serverless:${REGION}:${COMPTE}:/applications/00g8l9brbs9e3f1d
ROLE_EMR=arn:aws:iam::${COMPTE}:role/SoundLabEMRServerlessExecutionRole
REGLE=arn:aws:events:${REGION}:${COMPTE}:rule/StepFunctionsGetEventsForEMRServerlessJobRule
DEF=infra/3v_13_machine_chargement.json
T=$(mktemp -d)

echo "--- 1. Lambda de pilotage v2"
aws lambda update-function-code --function-name soundlab-3v-pilotage \
  --zip-file fileb://lambda/3v_pilotage/paquet.zip --query 'CodeSha256' --output text
aws lambda wait function-updated-v2 --function-name soundlab-3v-pilotage && echo "PILOTAGE_A_JOUR"

echo "--- 2. Role Step Functions"
cat > ${T}/confiance.json <<EOF
{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"states.amazonaws.com"},
 "Action":"sts:AssumeRole","Condition":{"StringEquals":{"aws:SourceAccount":"${COMPTE}"}}}]}
EOF
cat > ${T}/acces.json <<EOF
{"Version":"2012-10-17","Statement":[
 {"Sid":"InvoquerLambdas3V","Effect":"Allow","Action":"lambda:InvokeFunction","Resource":[
   "${FN}:soundlab-3v-pilotage","${FN}:soundlab-3v-pilotage:*",
   "${FN}:soundlab-3v-ingestion","${FN}:soundlab-3v-ingestion:*"]},
 {"Sid":"JobsEmr","Effect":"Allow","Action":["emr-serverless:StartJobRun","emr-serverless:GetJobRun","emr-serverless:CancelJobRun"],
  "Resource":["${APP}","${APP}/jobruns/*"]},
 {"Sid":"PasserRoleEmr","Effect":"Allow","Action":"iam:PassRole","Resource":"${ROLE_EMR}",
  "Condition":{"StringEquals":{"iam:PassedToService":"emr-serverless.amazonaws.com"}}},
 {"Sid":"RegleSync","Effect":"Allow","Action":["events:PutTargets","events:PutRule","events:DescribeRule"],"Resource":"${REGLE}"}
]}
EOF
if aws iam get-role --role-name ${ROLE} >/dev/null 2>&1; then
  echo "ROLE_EXISTANT ${ROLE}"
else
  aws iam create-role --role-name ${ROLE} --assume-role-policy-document file://${T}/confiance.json \
    --description "SoundLab 3V tache 3.3 : machine a etats de chargement incremental" --query 'Role.Arn' --output text
fi
aws iam put-role-policy --role-name ${ROLE} --policy-name ${POL} --policy-document file://${T}/acces.json
echo "POLITIQUE_ECRITE ${POL}"
ARN_ROLE=$(aws iam get-role --role-name ${ROLE} --query 'Role.Arn' --output text)

echo "--- 3. Validation et creation de la machine (aucune execution)"
aws stepfunctions validate-state-machine-definition --type STANDARD --definition file://${DEF} \
  --query '{resultat:result,diagnostics:diagnostics}' --output json
if aws stepfunctions describe-state-machine --state-machine-arn ${MACHINE} >/dev/null 2>&1; then
  aws stepfunctions update-state-machine --state-machine-arn ${MACHINE} --definition file://${DEF} \
    --role-arn ${ARN_ROLE} --query 'updateDate' --output text
  echo "MACHINE_MISE_A_JOUR"
else
  for i in 1 2 3 4 5 6; do   # le role vient d'etre cree : IAM peut mettre quelques secondes a le propager
    if aws stepfunctions create-state-machine --name ${NOM} --type STANDARD --definition file://${DEF} \
         --role-arn ${ARN_ROLE} --query 'stateMachineArn' --output text 2>${T}/err; then
      echo "MACHINE_CREEE"; break
    fi
    grep -qE "authorized to assume|AccessDeniedException|InvalidRoleArn" ${T}/err || { cat ${T}/err; exit 1; }
    echo "  role pas encore propage, nouvel essai dans 10 s"; sleep 10
    [[ ${i} == 6 ]] && { cat ${T}/err; exit 1; }
  done
fi
aws stepfunctions list-executions --state-machine-arn ${MACHINE} --query 'length(executions)' --output text \
  | sed 's/^/EXECUTIONS_EXISTANTES /'

echo "--- 4. Simulation du role"
ok=0; n=0
while IFS='|' read -r action ressource ckey cval attendu; do
  n=$((n+1))
  if [[ ${ckey} == - ]]; then
    r=$(aws iam simulate-principal-policy --policy-source-arn ${ARN_ROLE} --action-names ${action} \
        --resource-arns ${ressource} --query 'EvaluationResults[0].EvalDecision' --output text)
  else
    r=$(aws iam simulate-principal-policy --policy-source-arn ${ARN_ROLE} --action-names ${action} \
        --resource-arns ${ressource} \
        --context-entries ContextKeyName=${ckey},ContextKeyValues=${cval},ContextKeyType=string \
        --query 'EvaluationResults[0].EvalDecision' --output text)
  fi
  if [[ ${r} == ${attendu} ]]; then ok=$((ok+1)); s=OK; else s=ECHEC; fi
  printf '%-5s %2d %-30s %-13s %s\n' ${s} ${n} ${action} ${r} ${ressource#arn:aws:}
done <<EOF
lambda:InvokeFunction|${FN}:soundlab-3v-pilotage|-|-|allowed
lambda:InvokeFunction|${FN}:soundlab-3v-ingestion|-|-|allowed
lambda:InvokeFunction|${FN}:soundlab-3v-sonde-sortie|-|-|implicitDeny
emr-serverless:StartJobRun|${APP}|-|-|allowed
emr-serverless:GetJobRun|${APP}/jobruns/00g92i6hfdmqjg1f|-|-|allowed
emr-serverless:CancelJobRun|${APP}/jobruns/00g92i6hfdmqjg1f|-|-|allowed
emr-serverless:StartJobRun|arn:aws:emr-serverless:${REGION}:${COMPTE}:/applications/autre|-|-|implicitDeny
iam:PassRole|${ROLE_EMR}|iam:PassedToService|emr-serverless.amazonaws.com|allowed
iam:PassRole|${ROLE_EMR}|iam:PassedToService|lambda.amazonaws.com|implicitDeny
iam:PassRole|arn:aws:iam::${COMPTE}:role/SoundLabAirflowRole|iam:PassedToService|emr-serverless.amazonaws.com|implicitDeny
events:PutRule|${REGLE}|-|-|allowed
events:PutRule|arn:aws:events:${REGION}:${COMPTE}:rule/autre|-|-|implicitDeny
s3:GetObject|arn:aws:s3:::soundlab-curated-558852/trois_v/_etat/chargements_incr.json|-|-|implicitDeny
secretsmanager:GetSecretValue|arn:aws:secretsmanager:${REGION}:${COMPTE}:secret:soundlab/pseudonymisation-salt-listenbrainz-Xt64Yk|-|-|implicitDeny
EOF
rm -rf ${T}
[[ ${ok} == ${n} ]] && echo "MACHINE_DEPLOYEE ${ok}/${n} ${MACHINE}" || { echo "ROLE_SFN_ECHEC ${ok}/${n}"; exit 1; }
