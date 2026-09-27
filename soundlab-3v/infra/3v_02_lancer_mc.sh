#!/usr/bin/env bash
set -u
cd "$(dirname "$0")/.."
source .soundlab.env
P="$1"
DEBUT="$2"
FIN="${3:-2016-12-31}"
APP=00g8l9brbs9e3f1d
LOG="data/mc_reel_${P}.log"
echo "=== $P $DEBUT -> $FIN debut $(date '+%H:%M:%S')"
bash infra/submit_job.sh jobs/3v_21_agregation_titre_jour.py "s3://$SL_B_RAW/listenbrainz/ecoutes" "$DEBUT" "$FIN" "s3://$SL_B_CUR/trois_v/mc_reel/$P" "$P" > "$LOG" 2>&1
ID=$(grep -o 'jobRunId = [a-z0-9]*' "$LOG" | awk '{print $3}')
echo "JOBRUNID=$ID"
grep -E 'Paramètres|"etat"|PALIER|LIGNES|JOURS|GROUPES|COLONNES|PHASES|MC_|No space|Exception' "$LOG" | head -20
for i in $(seq 1 30); do
  V=$(aws emr-serverless get-job-run --application-id $APP --job-run-id "$ID" --query 'jobRun.billedResourceUtilization.vCPUHour' --output text)
  [ -n "$V" ] && [ "$V" != "None" ] && break
  sleep 10
done
aws emr-serverless get-job-run --application-id $APP --job-run-id "$ID" --query 'jobRun.{etat:state,duree:totalExecutionDurationSeconds,vcpu_facture:billedResourceUtilization.vCPUHour,mem_facture:billedResourceUtilization.memoryGBHour,disque_facture:billedResourceUtilization.storageGBHour}' --output json
