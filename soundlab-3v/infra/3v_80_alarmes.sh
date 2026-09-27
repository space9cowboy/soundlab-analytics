#!/usr/bin/env bash
# =============================================================================
#  SoundLab Analytics — chantier trois V, tâche 5.2
#  Trois alarmes sur la chaîne quotidienne, reliées au sujet SNS soundlab-alertes.
#
#    bash infra/3v_80_alarmes.sh            crée ou met à jour les alarmes
#    bash infra/3v_80_alarmes.sh test       les déclenche volontairement et
#                                           vérifie que l'action SNS a été exécutée
#
#  soundlab-3v-chargement-en-echec   exécutions de la machine à états en échec,
#                                    expirées ou interrompues (>= 1 sur 5 min).
#                                    Métrique validée par les deux échecs réels
#                                    (26/09 ~15:00 et 27/09 ~04:00, heure de Paris).
#  soundlab-3v-chargement-absent     aucune exécution réussie en 24 h ; donnée
#                                    manquante = panne (R14, défaillance silencieuse).
#                                    Fondée sur les exécutions réussies et non sur
#                                    EcoutesChargees : un dump vide (2683) reste un
#                                    chargement normal.
#  soundlab-3v-fraicheur-degradee    FraicheurDisponibiliteSecondes > 36 h. En régime
#                                    quotidien, un dump est chargé au cycle de 02:00
#                                    UTC suivant sa clôture ; 36 h = un cycle manqué
#                                    plus une marge. Valeur de régime à confirmer :
#                                    un seul chargement planifié mesuré au 27/09.
#
#  Le test par set-alarm-state prouve le trajet jusqu'à la notification, pas la
#  logique de la métrique ; la vérification lit l'historique d'action de chaque
#  alarme et échoue si SNS n'a pas été appelé.
# =============================================================================
set -euo pipefail
P=(--region eu-north-1 --profile soundlab)
COMPTE=589276558852
SM="arn:aws:states:eu-north-1:${COMPTE}:stateMachine:soundlab-3v-chargement-incremental"
SNS="arn:aws:sns:eu-north-1:${COMPTE}:soundlab-alertes"
A1=soundlab-3v-chargement-en-echec
A2=soundlab-3v-chargement-absent
A3=soundlab-3v-fraicheur-degradee

if [[ "${1:-}" == "test" ]]; then
  for a in "${A1}" "${A2}" "${A3}"; do
    aws cloudwatch set-alarm-state --alarm-name "${a}" --state-value ALARM \
        --state-reason "Declenchement volontaire, tache 5.2, $(date -u +%Y-%m-%dT%H:%MZ)" "${P[@]}"
    echo "DECLENCHEE ${a}"
  done
  sleep 30
  ok=0
  for a in "${A1}" "${A2}" "${A3}"; do
    h=$(aws cloudwatch describe-alarm-history --alarm-name "${a}" --history-item-type Action \
          --max-records 1 "${P[@]}" --query 'AlarmHistoryItems[0].HistorySummary' --output text)
    echo "ACTION ${a} : ${h}"
    [[ "${h}" == Successfully* ]] && ok=$((ok + 1))
  done
  echo "ACTIONS_SNS_REUSSIES ${ok}/3"
  if [[ "${ok}" -eq 3 ]]; then echo "TEST_ALARMES_OK"; else echo "TEST_ALARMES_ECHEC"; exit 1; fi
  exit 0
fi

M1=$(cat <<JSON
[{"Id":"f","ReturnData":false,"MetricStat":{"Metric":{"Namespace":"AWS/States","MetricName":"ExecutionsFailed","Dimensions":[{"Name":"StateMachineArn","Value":"${SM}"}]},"Period":300,"Stat":"Sum"}},
 {"Id":"t","ReturnData":false,"MetricStat":{"Metric":{"Namespace":"AWS/States","MetricName":"ExecutionsTimedOut","Dimensions":[{"Name":"StateMachineArn","Value":"${SM}"}]},"Period":300,"Stat":"Sum"}},
 {"Id":"a","ReturnData":false,"MetricStat":{"Metric":{"Namespace":"AWS/States","MetricName":"ExecutionsAborted","Dimensions":[{"Name":"StateMachineArn","Value":"${SM}"}]},"Period":300,"Stat":"Sum"}},
 {"Id":"e","ReturnData":true,"Label":"Echecs de la chaine 3V","Expression":"FILL(f,0)+FILL(t,0)+FILL(a,0)"}]
JSON
)
aws cloudwatch put-metric-alarm --alarm-name "${A1}" \
  --alarm-description "3V 5.2 : execution de soundlab-3v-chargement-incremental en echec, expiree ou interrompue" \
  --metrics "${M1}" --comparison-operator GreaterThanOrEqualToThreshold --threshold 1 \
  --evaluation-periods 1 --datapoints-to-alarm 1 --treat-missing-data notBreaching \
  --alarm-actions "${SNS}" "${P[@]}"
echo "CREEE ${A1}"

aws cloudwatch put-metric-alarm --alarm-name "${A2}" \
  --alarm-description "3V 5.2 (R14) : aucune execution reussie de la chaine quotidienne en 24 h" \
  --namespace AWS/States --metric-name ExecutionsSucceeded \
  --dimensions "Name=StateMachineArn,Value=${SM}" --statistic Sum --period 86400 \
  --comparison-operator LessThanThreshold --threshold 1 \
  --evaluation-periods 1 --datapoints-to-alarm 1 --treat-missing-data breaching \
  --alarm-actions "${SNS}" "${P[@]}"
echo "CREEE ${A2}"

aws cloudwatch put-metric-alarm --alarm-name "${A3}" \
  --alarm-description "3V 5.2 : fraicheur de disponibilite ListenBrainz superieure a 36 h" \
  --namespace SoundLab/3V --metric-name FraicheurDisponibiliteSecondes \
  --dimensions "Name=Source,Value=ListenBrainz" --statistic Maximum --period 3600 \
  --comparison-operator GreaterThanThreshold --threshold 129600 \
  --evaluation-periods 1 --datapoints-to-alarm 1 --treat-missing-data notBreaching \
  --alarm-actions "${SNS}" "${P[@]}"
echo "CREEE ${A3}"

aws cloudwatch describe-alarms --alarm-names "${A1}" "${A2}" "${A3}" "${P[@]}" \
  --query 'MetricAlarms[].[AlarmName,StateValue,Threshold,length(AlarmActions)]' --output text
