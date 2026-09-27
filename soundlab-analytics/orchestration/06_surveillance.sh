#!/usr/bin/env bash
# =============================================================================
#  SoundLab Analytics - Tache 21, surveillance CloudWatch
#  Couvre la competence C2.7 : "mettre en place des outils de surveillance pour
#  suivre les performances de l'infrastructure de donnees, identifier les
#  problemes potentiels et optimiser les systemes, en vue d'une gestion
#  proactive."
#
#  PRINCIPE DIRECTEUR : aucune dimension n'est supposee.
#  CloudWatch exige un jeu de dimensions exact et complet. Une alarme posee
#  sur un jeu incomplet reste eternellement en INSUFFICIENT_DATA : elle
#  ressemble a de la surveillance sans en etre une. Le script interroge donc
#  list-metrics pour chaque metrique, retient le jeu reellement publie qui
#  designe NOS ressources, et verifie qu'il porte des donnees avant de creer
#  quoi que ce soit.
#
#  Usage :  bash 06_surveillance.sh adresse@courriel.fr [--tester]
#           --tester declenche reellement une alarme pour verifier que la
#           chaine de notification fonctionne de bout en bout.
#  Idempotent : relancable sans effet de bord.
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

COURRIEL="${1:-}"
if [[ -z "${COURRIEL}" ]]; then
  rouge "Usage : bash 06_surveillance.sh adresse@courriel.fr"
  rouge "L'adresse recoit les alertes. Elle n'est stockee que dans ton compte AWS."
  exit 1
fi

# shellcheck disable=SC1091
source "${DEPOT}/.soundlab.env"
for v in SL_REGION SL_ACCOUNT_ID SL_EMR_APP_ID SL_RS_WORKGROUP; do
  [[ -n "${!v:-}" ]] || { rouge "Variable ${v} absente de .soundlab.env"; exit 1; }
done

EFF_REGION="${AWS_REGION:-${AWS_DEFAULT_REGION:-$(aws configure get region 2>/dev/null || true)}}"
[[ "${EFF_REGION}" == "${SL_REGION}" ]] || { rouge "Region effective '${EFF_REGION}' != '${SL_REGION}'"; exit 1; }

NOM_SUJET="soundlab-alertes"
NOM_TABLEAU="soundlab-bigdata"

# =============================================================================
#  Outil : resoudre le jeu de dimensions reellement publie
# =============================================================================
# Renvoie un JSON [{"Name":...,"Value":...}, ...] : le jeu de dimensions publie
# le PLUS COURT qui contienne la valeur recherchee. Le plus court, parce qu'une
# alarme doit agreger et non cibler un job particulier.
resoudre_dimensions() {
  local espace="$1" metrique="$2" valeur="$3"
  aws cloudwatch list-metrics --namespace "${espace}" --metric-name "${metrique}" \
      --output json 2>/dev/null \
    | python3 -c "
import sys, json
valeur = sys.argv[1]
jeux = [m['Dimensions'] for m in json.load(sys.stdin).get('Metrics', [])]
# On ne garde que les jeux qui designent notre ressource.
retenus = [d for d in jeux if any(x['Value'] == valeur for x in d)]
if not retenus:
    sys.exit(1)
retenus.sort(key=len)
print(json.dumps(retenus[0]))
" "${valeur}"
}

# Verifie qu'un couple metrique/dimensions porte effectivement des donnees.
porte_des_donnees() {
  local espace="$1" metrique="$2" dims_json="$3" jours="${4:-7}"
  local dims n
  dims="$(printf '%s' "${dims_json}" | python3 -c "
import sys, json
print(' '.join('Name=%s,Value=%s' % (d['Name'], d['Value']) for d in json.load(sys.stdin)))
")"
  # shellcheck disable=SC2086
  n="$(aws cloudwatch get-metric-statistics --namespace "${espace}" \
        --metric-name "${metrique}" --dimensions ${dims} \
        --start-time "$(date -u -v-${jours}d +%Y-%m-%dT%H:%M:%SZ 2>/dev/null \
                        || date -u -d "-${jours} days" +%Y-%m-%dT%H:%M:%SZ)" \
        --end-time   "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
        --period 86400 --statistics Sum \
        --query 'length(Datapoints)' --output text 2>/dev/null || echo 0)"
  [[ "${n}" =~ ^[0-9]+$ ]] && (( n > 0 ))
}

dims_pour_cli() {
  printf '%s' "$1" | python3 -c "
import sys, json
print(' '.join('Name=%s,Value=%s' % (d['Name'], d['Value']) for d in json.load(sys.stdin)))
"
}

dims_lisibles() {
  printf '%s' "$1" | python3 -c "
import sys, json
print(' + '.join('%s=%s' % (d['Name'], d['Value']) for d in json.load(sys.stdin)))
"
}

# =============================================================================
#  1. Sujet de notification
# =============================================================================
bleu "== Sujet de notification =="
ARN_SUJET="$(aws sns create-topic --name "${NOM_SUJET}" --query TopicArn --output text)"
vert "  ${ARN_SUJET}"

DEJA="$(aws sns list-subscriptions-by-topic --topic-arn "${ARN_SUJET}" \
          --query "Subscriptions[?Endpoint=='${COURRIEL}'].SubscriptionArn" --output text 2>/dev/null || echo "")"
if [[ -z "${DEJA}" ]]; then
  aws sns subscribe --topic-arn "${ARN_SUJET}" --protocol email --notification-endpoint "${COURRIEL}" >/dev/null
  jaune "  Abonnement cree - CONFIRME LE COURRIEL AWS, sinon aucune alerte n'arrivera."
else
  vert "  Abonnement deja present (${DEJA})"
fi

# =============================================================================
#  2. Resolution des dimensions
# =============================================================================
bleu ""
bleu "== Resolution des dimensions reellement publiees =="

D_EMR="$(resoudre_dimensions AWS/EMRServerless SuccessJobs "${SL_EMR_APP_ID}")" || {
  rouge "Aucune metrique EMR trouvee pour l'application ${SL_EMR_APP_ID}."
  rouge "Lance au moins un job avant de poser des alarmes."
  exit 1
}
vert "  EMR (jobs)      : $(dims_lisibles "${D_EMR}")"

# CPUAllocated existe a deux granularites ; la plus courte agrege l'application.
D_CPU="$(resoudre_dimensions AWS/EMRServerless CPUAllocated "${SL_EMR_APP_ID}")" || D_CPU=""
[[ -n "${D_CPU}" ]] && vert "  EMR (capacite)  : $(dims_lisibles "${D_CPU}")"

D_LIMITE="$(resoudre_dimensions AWS/Redshift-Serverless UsageLimitAvailable "${SL_RS_WORKGROUP}")" || D_LIMITE=""
[[ -n "${D_LIMITE}" ]] && vert "  Redshift (quota): $(dims_lisibles "${D_LIMITE}")"

# Metrique de duree des requetes : verifiable, donc alarmable.
# On ne tente PLUS d'emprunter un jeu de dimensions a une metrique voisine :
# UserQueriesFailed n'est publiee qu'avec QueryType, sans Workgroup, tandis que
# QueriesSucceeded l'est avec Workgroup + QueryType. Emprunter le second au
# premier fabrique une combinaison que personne n'emet - une alarme morte.
# Les jeux de dimensions publies refletent ce qui a ete EMIS, pas un schema.
D_DUREE="$(resoudre_dimensions AWS/Redshift-Serverless UserQueryDuration "${SL_RS_WORKGROUP}")" || D_DUREE=""
[[ -n "${D_DUREE}" ]] && vert "  Redshift (duree): $(dims_lisibles "${D_DUREE}")"

# Validation croisee : FailedJobs n'a legitimement aucune donnee tant qu'aucun
# job n'a echoue. On valide donc SON jeu de dimensions au moyen de SuccessJobs,
# qui partage le meme jeu et doit, lui, porter des points.
bleu ""
bleu "== Validation du jeu de dimensions EMR =="
if porte_des_donnees AWS/EMRServerless SuccessJobs "${D_EMR}"; then
  vert "  SuccessJobs porte des donnees - le jeu est le bon"
else
  rouge "  SuccessJobs ne porte aucune donnee sur ce jeu de dimensions."
  rouge "  L'alarme FailedJobs resterait en INSUFFICIENT_DATA. Arret."
  exit 1
fi

# =============================================================================
#  3. Alarmes
# =============================================================================
bleu ""
bleu "== Alarmes =="

poser_alarme() {
  local nom="$1" espace="$2" metrique="$3" dims_json="$4" stat="$5" \
        periode="$6" seuil="$7" operateur="$8" manquant="$9" description="${10}"
  # shellcheck disable=SC2086
  aws cloudwatch put-metric-alarm \
    --alarm-name "${nom}" \
    --alarm-description "${description}" \
    --namespace "${espace}" --metric-name "${metrique}" \
    --dimensions $(dims_pour_cli "${dims_json}") \
    --statistic "${stat}" --period "${periode}" --evaluation-periods 1 \
    --threshold "${seuil}" --comparison-operator "${operateur}" \
    --treat-missing-data "${manquant}" \
    --alarm-actions "${ARN_SUJET}" --ok-actions "${ARN_SUJET}"
  vert "  posee : ${nom}"
}

poser_alarme "soundlab-emr-jobs-en-echec" \
  AWS/EMRServerless FailedJobs "${D_EMR}" Sum 300 1 GreaterThanOrEqualToThreshold notBreaching \
  "Au moins un job EMR Serverless a echoue. Consulter le journal Airflow de la tache correspondante."

if [[ -n "${D_CPU}" ]]; then
  # Le quota du compte est de 16 vCPU concurrents (incident 10.3). Alerter a 15
  # previent AVANT le ServiceQuotaExceededException, au lieu de le constater.
  poser_alarme "soundlab-emr-quota-vcpu-proche" \
    AWS/EMRServerless CPUAllocated "${D_CPU}" Maximum 300 15 GreaterThanOrEqualToThreshold notBreaching \
    "La capacite allouee approche le quota de 16 vCPU du compte. Un job supplementaire echouerait."
fi

if [[ -n "${D_LIMITE}" ]]; then
  # Le plafond dur desactive le groupe de travail. Alerter a 20 % restants
  # transforme une coupure subie en decision anticipee.
  poser_alarme "soundlab-redshift-quota-bientot-atteint" \
    AWS/Redshift-Serverless UsageLimitAvailable "${D_LIMITE}" Minimum 3600 12 LessThanOrEqualToThreshold missing \
    "Moins de 12 RPU-heures restantes sur le plafond mensuel de 60. Au-dela, le groupe de travail est desactive automatiquement."
fi

# Alarme de duree des requetes : creee UNIQUEMENT si la metrique porte
# reellement des donnees sur ce jeu de dimensions. Une alarme invalidable
# n'est pas posee - mieux vaut trois alarmes verifiees que quatre dont une
# ment.
if [[ -n "${D_DUREE}" ]] && porte_des_donnees AWS/Redshift-Serverless UserQueryDuration "${D_DUREE}"; then
  poser_alarme "soundlab-redshift-requetes-lentes" \
    AWS/Redshift-Serverless UserQueryDuration "${D_DUREE}" Average 900 60000000 GreaterThanThreshold missing \
    "Duree moyenne des requetes superieure a 60 s sur 15 minutes. Degradation des performances de l'entrepot."
else
  jaune "  omise : alarme de duree des requetes (metrique non validee)"
fi

# L'alarme sur les requetes en echec avait ete posee sur un jeu de dimensions
# emprunte, que UserQueriesFailed n'emet pas. On la supprime plutot que de la
# laisser afficher OK sans jamais rien mesurer.
if aws cloudwatch describe-alarms --alarm-names soundlab-redshift-requetes-en-echec \
     --query 'length(MetricAlarms)' --output text 2>/dev/null | grep -q '^1$'; then
  aws cloudwatch delete-alarms --alarm-names soundlab-redshift-requetes-en-echec
  jaune "  supprimee : soundlab-redshift-requetes-en-echec (jeu de dimensions invalide)"
fi

# =============================================================================
#  4. Tableau de bord
# =============================================================================
bleu ""
bleu "== Tableau de bord ${NOM_TABLEAU} =="

python3 - "${TMP}/tableau.json" "${SL_REGION}" "${SL_EMR_APP_ID}" "${SL_RS_WORKGROUP}" \
         "${D_EMR}" "${D_CPU:-[]}" "${D_LIMITE:-[]}" <<'FIN_PY'
import json, sys

sortie, region, app, wg, d_emr, d_cpu, d_limite = sys.argv[1:8]
d_emr, d_cpu, d_limite = (json.loads(x) for x in (d_emr, d_cpu, d_limite))

def plat(dims):
    """[{'Name':..,'Value':..}] -> ['Name','Value', ...] pour les widgets."""
    r = []
    for d in dims:
        r += [d["Name"], d["Value"]]
    return r

def widget(x, y, w, h, titre, metriques, periode=300, extra=None):
    props = {
        "metrics": metriques, "view": "timeSeries", "stacked": False,
        "region": region, "title": titre, "period": periode,
    }
    if extra:
        props.update(extra)
    return {"type": "metric", "x": x, "y": y, "width": w, "height": h, "properties": props}

widgets = []

# --- Sante du pipeline ------------------------------------------------------
widgets.append(widget(0, 0, 12, 6, "Jobs EMR - soumis, reussis, en echec", [
    ["AWS/EMRServerless", "SubmittedJobs"] + plat(d_emr) + [{"label": "soumis", "stat": "Sum"}],
    ["...", "SuccessJobs"] + plat(d_emr) + [{"label": "reussis", "stat": "Sum"}],
    ["...", "FailedJobs"] + plat(d_emr) + [{"label": "en echec", "stat": "Sum", "color": "#d62728"}],
], extra={"stacked": False, "yAxis": {"left": {"min": 0}}}))

# --- Saturation du quota ----------------------------------------------------
if d_cpu:
    widgets.append(widget(12, 0, 12, 6, "Capacite EMR allouee et plafond du compte", [
        ["AWS/EMRServerless", "CPUAllocated"] + plat(d_cpu) + [{"label": "vCPU alloues", "stat": "Maximum"}],
        ["...", "MaxCPUAllowed"] + plat(d_cpu) + [{"label": "plafond declare", "stat": "Maximum"}],
    ], extra={"annotations": {"horizontal": [
        {"label": "quota du compte : 16 vCPU", "value": 16, "color": "#d62728"}
    ]}}))

# --- Alloue contre utilise : le widget d'optimisation -----------------------
# Expression de recherche : agrege tous les JobId, qu'on ne peut pas enumerer
# a l'avance puisqu'un nouveau job cree une nouvelle dimension.
recherche = (
    '{AWS/EMRServerless,ApplicationId,ApplicationName,CapacityAllocationType,'
    'JobId,JobName,WorkerType} ApplicationId="%s"' % app
)
widgets.append(widget(0, 6, 12, 6, "Ressources reservees contre reellement utilisees", [
    [{"expression": 'SUM(SEARCH(\'%s MetricName="WorkerCpuAllocated"\', \'Average\', 300))' % recherche,
      "label": "vCPU reserves", "id": "e1"}],
    [{"expression": 'SUM(SEARCH(\'%s MetricName="WorkerCpuUsed"\', \'Average\', 300))' % recherche,
      "label": "vCPU utilises", "id": "e2"}],
], extra={"yAxis": {"left": {"min": 0}}}))

# --- Entrepot ---------------------------------------------------------------
widgets.append(widget(12, 6, 12, 6, "Redshift - capacite et duree des requetes", [
    ["AWS/Redshift-Serverless", "ComputeCapacity", "Workgroup", wg,
     {"label": "RPU", "stat": "Average"}],
    ["...", "UserQueryDuration", "Workgroup", wg,
     {"label": "duree moyenne des requetes (us)", "stat": "Average", "yAxis": "right"}],
]))

if d_limite:
    widgets.append(widget(0, 12, 12, 6, "Plafond mensuel Redshift - consomme et restant", [
        ["AWS/Redshift-Serverless", "UsageLimitConsumed"] + plat(d_limite) + [{"label": "consomme", "stat": "Maximum"}],
        ["...", "UsageLimitAvailable"] + plat(d_limite) + [{"label": "restant", "stat": "Minimum"}],
    ], periode=3600, extra={"annotations": {"horizontal": [
        {"label": "seuil d'alerte : 12 RPU-h restantes", "value": 12, "color": "#ff7f0e"}
    ]}}))

widgets.append(widget(12, 12, 12, 6, "Redshift - volumetrie et requetes en echec", [
    ["AWS/Redshift-Serverless", "DataStorage", "Workgroup", wg,
     {"label": "stockage (Mo)", "stat": "Maximum"}],
    ["...", "UserQueriesFailed", "Workgroup", wg,
     {"label": "requetes en echec", "stat": "Sum", "yAxis": "right", "color": "#d62728"}],
], periode=3600))

json.dump({"widgets": widgets}, open(sortie, "w"), ensure_ascii=True)
print("  %d widgets composes" % len(widgets))
FIN_PY

python3 -m json.tool "${TMP}/tableau.json" >/dev/null || { rouge "JSON de tableau invalide"; exit 1; }

aws cloudwatch put-dashboard --dashboard-name "${NOM_TABLEAU}" \
    --dashboard-body "file://${TMP}/tableau.json" \
    --query 'DashboardValidationMessages' --output json

vert "  Tableau de bord publie"

# =============================================================================
#  5. Etat final
# =============================================================================
# =============================================================================
#  4 bis. Verification du dispositif lui-meme
#
#  Une alarme qu'on n'a jamais vue se declencher n'est pas verifiee. Le mode
#  --tester force une alarme en etat ALARM, ce qui declenche reellement la
#  chaine alarme -> SNS -> courriel, puis la remet en OK. AWS reevalue ensuite
#  l'etat sur les vraies donnees a la periode suivante.
# =============================================================================
if [[ "${2:-}" == "--tester" ]]; then
  bleu ""
  bleu "== Test du dispositif d'alerte (declenchement force) =="
  jaune "  Une alarme va reellement partir. Verifie ta boite aux lettres."
  aws cloudwatch set-alarm-state \
    --alarm-name "soundlab-emr-jobs-en-echec" \
    --state-value ALARM \
    --state-reason "Test manuel du dispositif de notification - aucun incident reel"
  vert "  Etat force a ALARM"
  sleep 10
  aws cloudwatch set-alarm-state \
    --alarm-name "soundlab-emr-jobs-en-echec" \
    --state-value OK \
    --state-reason "Fin du test manuel"
  vert "  Etat remis a OK - deux courriels attendus (declenchement puis retour a la normale)"
fi

bleu ""
bleu "== Audit des alarmes =="
# Trente secondes : CloudWatch a besoin d'un cycle d'evaluation pour sortir de
# INSUFFICIENT_DATA. Lire l'etat immediatement apres creation ne mesure rien.
sleep 30

# Deux questions distinctes, longtemps confondues :
#   1. le jeu de dimensions est-il valide ?  -> sur 7 jours, ou l'on SAIT
#      qu'il y a eu de l'activite ;
#   2. y a-t-il de l'activite maintenant ?   -> sur la derniere periode.
#
# Une metrique EVENEMENTIELLE (FailedJobs, CPUAllocated sur du serverless)
# n'est emise que pendant une activite : son silence au repos est normal.
# Une metrique CONTINUE (UsageLimitAvailable) est emise en permanence : son
# silence signale une erreur de configuration.
#
# Confondre les deux fait passer une alarme saine pour cassee, ou l'inverse.
aws cloudwatch describe-alarms --alarm-name-prefix soundlab \
    --query 'MetricAlarms[].[AlarmName,StateValue,StateReason,Namespace,MetricName,Dimensions]' \
    --output json > "${TMP}/alarmes.json"

python3 - "${TMP}/alarmes.json" <<'FIN_AUDIT'
import json, subprocess, sys
from datetime import datetime, timedelta, timezone

alarmes = json.load(open(sys.argv[1], encoding="utf-8"))
maintenant = datetime.now(timezone.utc)

def points(espace, metrique, dims, jours):
    args = ["aws", "cloudwatch", "get-metric-statistics",
            "--namespace", espace, "--metric-name", metrique,
            "--start-time", (maintenant - timedelta(days=jours)).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "--end-time", maintenant.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "--period", "86400", "--statistics", "Sum",
            "--query", "length(Datapoints)", "--output", "text"]
    if dims:
        args.append("--dimensions")
        args += ["Name=%s,Value=%s" % (d["Name"], d["Value"]) for d in dims]
    r = subprocess.run(args, capture_output=True, text=True)
    try:
        return int(r.stdout.strip())
    except ValueError:
        return 0

suspectes = []
for nom, etat, motif, espace, metrique, dims in alarmes:
    aveugle = "no datapoints were received" in (motif or "")
    if not aveugle:
        verdict = "%s (mesure active)" % etat
    elif points(espace, metrique, dims, 7) > 0:
        verdict = "%s (au repos - jeu de dimensions valide)" % etat
    else:
        verdict = "MUETTE - aucune donnee sur 7 jours"
        suspectes.append(nom)
    print("  %-44s %s" % (nom, verdict))

print()
if suspectes:
    print("  %d alarme(s) n'ont jamais rien mesure - configuration a revoir :" % len(suspectes))
    for n in suspectes:
        print("    - " + n)
    sys.exit(1)
print("  Toutes les alarmes portent sur un jeu de dimensions verifie.")
FIN_AUDIT

bleu ""
vert "================================================================"
vert " Tableau de bord : https://${SL_REGION}.console.aws.amazon.com/cloudwatch/home?region=${SL_REGION}#dashboards:name=${NOM_TABLEAU}"
vert " Sujet d'alertes : ${ARN_SUJET}"
vert ""
vert " N'OUBLIE PAS de confirmer l'abonnement recu par courriel."
vert " Cout : 1 tableau de bord et 4 alarmes restent dans l'offre gratuite"
vert " (3 tableaux et 10 alarmes standard inclus par compte)."
vert "================================================================"
