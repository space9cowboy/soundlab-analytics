#!/usr/bin/env zsh
# 3v_15 (tache 3.3, E1/E6) : declenchement par EventBridge Scheduler de la machine soundlab-3v-chargement-incremental.
# 1. Role SoundLab3VSchedulerChargement : states:StartExecution sur cette seule machine.
# 2. Planification quotidienne soundlab-3v-chargement-quotidien : 02:00 UTC chaque jour (E1).
# 3. Planification unique soundlab-3v-chargement-essai : dans 15 minutes, supprimee par AWS apres usage (E6 :
#    premier chargement sans intervention, observable aujourd'hui).
# 4. Simulation du role.
set -euo pipefail
COMPTE=589276558852; REGION=eu-north-1
ROLE=SoundLab3VSchedulerChargement; POL=SoundLab3VSchedulerChargementAcces
MACHINE=arn:aws:states:${REGION}:${COMPTE}:stateMachine:soundlab-3v-chargement-incremental
T=$(mktemp -d)

echo "--- 1. Role Scheduler"
cat > ${T}/confiance.json <<EOF
{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"scheduler.amazonaws.com"},
 "Action":"sts:AssumeRole","Condition":{"StringEquals":{"aws:SourceAccount":"${COMPTE}"}}}]}
EOF
cat > ${T}/acces.json <<EOF
{"Version":"2012-10-17","Statement":[
 {"Sid":"DemarrerChargement","Effect":"Allow","Action":"states:StartExecution","Resource":"${MACHINE}"}]}
EOF
if aws iam get-role --role-name ${ROLE} >/dev/null 2>&1; then
  echo "ROLE_EXISTANT ${ROLE}"
else
  aws iam create-role --role-name ${ROLE} --assume-role-policy-document file://${T}/confiance.json \
    --description "SoundLab 3V tache 3.3 : declenchement du chargement incremental" --query 'Role.Arn' --output text
fi
aws iam put-role-policy --role-name ${ROLE} --policy-name ${POL} --policy-document file://${T}/acces.json
echo "POLITIQUE_ECRITE ${POL}"
ARN_ROLE=$(aws iam get-role --role-name ${ROLE} --query 'Role.Arn' --output text)
CIBLE="{\"Arn\":\"${MACHINE}\",\"RoleArn\":\"${ARN_ROLE}\",\"Input\":\"{}\",\"RetryPolicy\":{\"MaximumRetryAttempts\":2,\"MaximumEventAgeInSeconds\":3600}}"

creer() {   # nom expression [options...] ; le role vient d'etre cree : IAM peut mettre quelques secondes a le propager
  local nom=$1 expr=$2; shift 2
  for i in 1 2 3 4 5 6; do
    if aws scheduler create-schedule --name ${nom} --schedule-expression "${expr}" \
         --schedule-expression-timezone UTC --flexible-time-window Mode=OFF --target "${CIBLE}" \
         --state ENABLED "$@" --query 'ScheduleArn' --output text 2>${T}/err; then
      return 0
    fi
    grep -qE "ConflictException" ${T}/err && { echo "PLANIFICATION_EXISTANTE ${nom}"; return 1; }
    grep -qE "ValidationException.*(role|Role)|AccessDenied" ${T}/err || { cat ${T}/err; return 1; }
    echo "  role pas encore propage, nouvel essai dans 10 s"; sleep 10
  done
  cat ${T}/err; return 1
}

echo "--- 2. Planification quotidienne (02:00 UTC)"
creer soundlab-3v-chargement-quotidien "cron(0 2 * * ? *)" \
  --description "SoundLab 3V : chargement quotidien des dumps incrementaux ListenBrainz"

echo "--- 3. Planification unique d'essai (dans 15 min)"
QUAND=$(python3 -c 'import datetime as d; print((d.datetime.now(d.timezone.utc)+d.timedelta(minutes=15)).strftime("%Y-%m-%dT%H:%M:00"))')
echo "ESSAI_PREVU ${QUAND} UTC"
creer soundlab-3v-chargement-essai "at(${QUAND})" --action-after-completion DELETE \
  --description "SoundLab 3V : premier chargement planifie (preuve E6)"

aws scheduler list-schedules --name-prefix soundlab-3v-chargement \
  --query 'Schedules[].{nom:Name,etat:State}' --output table

echo "--- 4. Simulation du role"
ok=0; n=0
while IFS='|' read -r action ressource attendu; do
  n=$((n+1))
  r=$(aws iam simulate-principal-policy --policy-source-arn ${ARN_ROLE} --action-names ${action} \
      --resource-arns ${ressource} --query 'EvaluationResults[0].EvalDecision' --output text)
  if [[ ${r} == ${attendu} ]]; then ok=$((ok+1)); s=OK; else s=ECHEC; fi
  printf '%-5s %2d %-24s %-13s %s\n' ${s} ${n} ${action} ${r} ${ressource#arn:aws:}
done <<EOF
states:StartExecution|${MACHINE}|allowed
states:StartExecution|arn:aws:states:${REGION}:${COMPTE}:stateMachine:autre|implicitDeny
states:StopExecution|arn:aws:states:${REGION}:${COMPTE}:execution:soundlab-3v-chargement-incremental:x|implicitDeny
lambda:InvokeFunction|arn:aws:lambda:${REGION}:${COMPTE}:function:soundlab-3v-ingestion|implicitDeny
EOF
rm -rf ${T}
[[ ${ok} == ${n} ]] && echo "SCHEDULER_OK ${ok}/${n} essai ${QUAND} UTC" || { echo "SCHEDULER_ECHEC ${ok}/${n}"; exit 1; }
