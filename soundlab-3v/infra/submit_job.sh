#!/usr/bin/env bash
# =============================================================================
#  SoundLab Analytics — Soumission d'un job PySpark à EMR Serverless
#
#  Téléverse le script dans le bucket `scripts`, lance le job, puis suit son
#  état jusqu'à la fin et affiche les logs en cas d'échec.
#
#  Usage :
#    ./infra/submit_job.sh jobs/07_ingest_music_info.py --source s3://... ...
#
#  Portable macOS / CloudShell.
# =============================================================================

set -euo pipefail

log()  { printf '\033[1;34m[%s]\033[0m %s\n' "$(date +%H:%M:%S)" "$*"; }
ok()   { printf '\033[1;32m  ✓\033[0m %s\n' "$*"; }
die()  { printf '\033[1;31m  ✗ %s\033[0m\n' "$*" >&2; exit 1; }

[[ $# -ge 1 ]] || die "Usage : $0 <script.py> [arguments du job...]"
SCRIPT_LOCAL="$1"; shift
[[ -f "$SCRIPT_LOCAL" ]] || die "Script introuvable : $SCRIPT_LOCAL"

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=/dev/null
[[ -f "$ROOT/.soundlab.env" ]] && source "$ROOT/.soundlab.env"

: "${SL_EMR_APP_ID:?SL_EMR_APP_ID manquant — source .soundlab.env}"
: "${SL_EMR_ROLE_ARN:?SL_EMR_ROLE_ARN manquant}"
: "${SL_B_SCR:?SL_B_SCR manquant}"
: "${SL_B_LOG:?SL_B_LOG manquant}"

NOM_SCRIPT="$(basename "$SCRIPT_LOCAL")"
S3_SCRIPT="s3://$SL_B_SCR/jobs/$NOM_SCRIPT"
NOM_JOB="${NOM_SCRIPT%.py}-$(date +%Y%m%d-%H%M%S)"

# ----------------------------------------------------------------------------
# 1. Publication du script
# ----------------------------------------------------------------------------
log "Publication de $NOM_SCRIPT"
aws s3 cp "$SCRIPT_LOCAL" "$S3_SCRIPT" --only-show-errors
ok "$S3_SCRIPT"

# ----------------------------------------------------------------------------
# 2. Arguments du job -> tableau JSON
# ----------------------------------------------------------------------------
ARGS_JSON="[]"
if [[ $# -gt 0 ]]; then
  ARGS_JSON="$(printf '%s\n' "$@" | python3 -c \
    'import json,sys; print(json.dumps([l.rstrip("\n") for l in sys.stdin]))')"
fi

# ----------------------------------------------------------------------------
# 3. Paramètres Spark
#
#  Dimensionnement par défaut : petit job (tâche 7). Total 10 vCPU au plus,
#  sous le quota de 16 vCPU concurrents du compte.
#
#  Pour un job plus lourd, surcharger sans toucher au script :
#      export SL_SPARK_PARAMS="--conf spark.executor.cores=4 ..."
#
#  Rappels :
#   - EMR Serverless impose que initialExecutors tienne dans
#     [minExecutors, maxExecutors] ; il vaut 3 par défaut, donc on le déclare
#     explicitement plutôt que de subir une valeur invisible.
#   - Le plafond réel n'est pas maximumCapacity de l'application mais le quota
#     « Max concurrent vCPUs per account » (L-D05C8A75).
# ----------------------------------------------------------------------------
SPARK_PARAMS="${SL_SPARK_PARAMS:---conf spark.executor.cores=2 \
--conf spark.executor.memory=8g \
--conf spark.driver.cores=2 \
--conf spark.driver.memory=4g \
--conf spark.executor.instances=2 \
--conf spark.dynamicAllocation.enabled=true \
--conf spark.dynamicAllocation.minExecutors=1 \
--conf spark.dynamicAllocation.initialExecutors=2 \
--conf spark.dynamicAllocation.maxExecutors=4 \
--conf spark.sql.sources.partitionOverwriteMode=dynamic}"

echo "  Paramètres Spark : ${SL_SPARK_PARAMS:+(surchargés via SL_SPARK_PARAMS)}"

DRIVER_JSON="$(python3 - "$S3_SCRIPT" "$ARGS_JSON" "$SPARK_PARAMS" <<'PY'
import json, sys
entry, args, params = sys.argv[1], json.loads(sys.argv[2]), sys.argv[3]
print(json.dumps({"sparkSubmit": {
    "entryPoint": entry,
    "entryPointArguments": args,
    "sparkSubmitParameters": params,
}}))
PY
)"

OVERRIDES_JSON="$(python3 - "$SL_B_LOG" <<'PY'
import json, sys
print(json.dumps({"monitoringConfiguration": {
    "s3MonitoringConfiguration": {"logUri": f"s3://{sys.argv[1]}/emr-serverless/"}
}}))
PY
)"

# ----------------------------------------------------------------------------
# 4. Lancement
# ----------------------------------------------------------------------------
log "Lancement du job $NOM_JOB"
JOB_ID="$(aws emr-serverless start-job-run \
  --application-id "$SL_EMR_APP_ID" \
  --execution-role-arn "$SL_EMR_ROLE_ARN" \
  --name "$NOM_JOB" \
  --job-driver "$DRIVER_JSON" \
  --configuration-overrides "$OVERRIDES_JSON" \
  --query 'jobRunId' --output text)"
ok "jobRunId = $JOB_ID"

# ----------------------------------------------------------------------------
# 5. Suivi
#    Le premier job démarre à froid : compter 60 à 120 s avant l'état RUNNING.
# ----------------------------------------------------------------------------
log "Suivi de l'exécution (Ctrl-C n'annule pas le job)"
ETAT=""; DEBUT="$(date +%s)"
while true; do
  ETAT="$(aws emr-serverless get-job-run \
          --application-id "$SL_EMR_APP_ID" --job-run-id "$JOB_ID" \
          --query 'jobRun.state' --output text)"
  printf '\r  %-12s  %3ds écoulées ' "$ETAT" "$(( $(date +%s) - DEBUT ))"
  case "$ETAT" in
    SUCCESS|FAILED|CANCELLED) echo; break ;;
  esac
  sleep 10
done

DETAIL="$(aws emr-serverless get-job-run \
          --application-id "$SL_EMR_APP_ID" --job-run-id "$JOB_ID" \
          --query 'jobRun.{etat:state,detail:stateDetails,duree:totalExecutionDurationSeconds,vCPUh:billedResourceUtilization.vCPUHour,memGBh:billedResourceUtilization.memoryGBHour}')"
echo "$DETAIL"

LOG_BASE="s3://$SL_B_LOG/emr-serverless/applications/$SL_EMR_APP_ID/jobs/$JOB_ID"

if [[ "$ETAT" == "SUCCESS" ]]; then
  ok "Job terminé avec succès"
  echo "  Logs : $LOG_BASE/SPARK_DRIVER/stdout.gz"
  echo
  echo "  Sortie du pilote :"
  aws s3 cp "$LOG_BASE/SPARK_DRIVER/stdout.gz" - 2>/dev/null | gunzip 2>/dev/null | sed 's/^/    /' || \
    echo "    (logs pas encore disponibles, réessaie dans 30 s)"
else
  printf '\033[1;31m  ✗ Job en échec\033[0m\n' >&2
  echo "  Trace d'erreur :" >&2
  aws s3 cp "$LOG_BASE/SPARK_DRIVER/stderr.gz" - 2>/dev/null | gunzip 2>/dev/null | tail -60 | sed 's/^/    /' >&2 || \
    echo "    Logs indisponibles. Console : EMR Studio > Serverless > $SL_EMR_APP_ID" >&2
  exit 1
fi
