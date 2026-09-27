#!/usr/bin/env bash
# =============================================================================
#  SoundLab Analytics - Tache 14, etape 3b (prealable)
#  Accorde a l'orchestrateur les droits Redshift necessaires au chargement.
#
#  Le probleme : l'API de donnees mappe chaque identite IAM vers un
#  utilisateur de base distinct. Les tables appartiennent a IAM:loic-admin ;
#  le DAG se presentera sous une autre identite, qui n'a aucun droit.
#
#  La methode : on ne devine pas le nom de cet utilisateur, on le demande.
#  Une premiere connexion sous le role le cree, un SELECT current_user donne
#  son nom exact, et les GRANT portent sur ce nom-la.
#
#  Usage :  bash 05_droits_redshift.sh
#  Idempotent : les GRANT sont sans effet s'ils sont deja poses.
# =============================================================================
set -euo pipefail

RACINE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEPOT="$(cd "${RACINE}/.." && pwd)"

bleu()  { printf '\033[0;36m%s\033[0m\n' "$*"; }
vert()  { printf '\033[0;32m%s\033[0m\n' "$*"; }
jaune() { printf '\033[0;33m%s\033[0m\n' "$*"; }
rouge() { printf '\033[0;31m%s\033[0m\n' "$*"; }

# shellcheck disable=SC1091
source "${DEPOT}/.soundlab.env"

for v in SL_RS_WORKGROUP SL_RS_DB SL_AIRFLOW_ROLE_ARN; do
  [[ -n "${!v:-}" ]] || { rouge "Variable ${v} absente de .soundlab.env"; exit 1; }
done

# --- Execution d'une instruction, avec attente et diagnostic ------------------
# Renvoie l'identifiant de l'instruction sur la sortie standard ; l'etat et le
# message d'erreur eventuel vont sur la sortie d'erreur.
executer() {
  local sql="$1" libelle="${2:-instruction}"
  local id etat motif
  id="$(aws redshift-data execute-statement \
          --workgroup-name "${SL_RS_WORKGROUP}" \
          --database "${SL_RS_DB}" \
          --sql "${sql}" --query Id --output text)"
  etat="SUBMITTED"
  for _ in $(seq 1 40); do
    etat="$(aws redshift-data describe-statement --id "${id}" --query Status --output text)"
    case "${etat}" in FINISHED|FAILED|ABORTED) break ;; esac
    sleep 1
  done
  if [[ "${etat}" == "FINISHED" ]]; then
    vert "  OK      ${libelle}" >&2
    printf '%s' "${id}"
    return 0
  fi
  motif="$(aws redshift-data describe-statement --id "${id}" --query Error --output text 2>/dev/null || echo "")"
  rouge "  ECHEC   ${libelle} (${etat})" >&2
  [[ -n "${motif}" && "${motif}" != "None" ]] && rouge "          ${motif}" >&2
  printf '%s' ""
  return 1
}

premiere_valeur() {
  aws redshift-data get-statement-result --id "$1" \
    --query 'Records[0][0].stringValue' --output text 2>/dev/null || echo ""
}

# =============================================================================
#  1. Identite de base de donnees du role Airflow
# =============================================================================
bleu "== Identite Redshift du role d'orchestration =="

JETONS="$(aws sts assume-role \
            --role-arn "${SL_AIRFLOW_ROLE_ARN}" \
            --role-session-name "droits-redshift" \
            --duration-seconds 900 \
            --query 'Credentials.[AccessKeyId,SecretAccessKey,SessionToken]' \
            --output text)"
read -r C_CLE C_SEC C_JET <<< "${JETONS}"

PROFIL_SAUVE="${AWS_PROFILE:-}"
unset AWS_PROFILE AWS_DEFAULT_PROFILE || true
export AWS_ACCESS_KEY_ID="${C_CLE}" AWS_SECRET_ACCESS_KEY="${C_SEC}" AWS_SESSION_TOKEN="${C_JET}"

# Cette premiere connexion cree l'utilisateur de base s'il n'existe pas.
ID="$(executer "SELECT current_user;" "premiere connexion sous le role")" || {
  rouge "Le role ne parvient pas a joindre Redshift."
  rouge "Verifie redshift-data:ExecuteStatement et redshift-serverless:GetCredentials."
  exit 1
}
UTILISATEUR_DB="$(premiere_valeur "${ID}")"

unset AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN
[[ -n "${PROFIL_SAUVE}" ]] && export AWS_PROFILE="${PROFIL_SAUVE}"
unset C_CLE C_SEC C_JET JETONS

if [[ -z "${UTILISATEUR_DB}" || "${UTILISATEUR_DB}" == "None" ]]; then
  rouge "Nom d'utilisateur illisible."
  exit 1
fi
vert "  Utilisateur de base : ${UTILISATEUR_DB}"

# =============================================================================
#  2. Attribution des droits, sous l'identite administratrice
# =============================================================================
bleu ""
bleu "== Attribution des droits (identite administratrice) =="

TABLES=("soundlab.dim_track" "soundlab.fact_listening")
ECHECS=0

executer "GRANT USAGE ON SCHEMA soundlab TO \"${UTILISATEUR_DB}\";" \
         "USAGE sur le schema soundlab" >/dev/null || ECHECS=$((ECHECS + 1))

for t in "${TABLES[@]}"; do
  executer "GRANT SELECT, INSERT, UPDATE, DELETE ON ${t} TO \"${UTILISATEUR_DB}\";" \
           "SELECT/INSERT/UPDATE/DELETE sur ${t}" >/dev/null || ECHECS=$((ECHECS + 1))
done

# TRUNCATE est un privilege distinct, apparu tardivement dans Redshift.
# S'il n'existe pas sur cette version, le chargement utilisera DELETE dans
# une transaction - ce qui est d'ailleurs atomique, contrairement a TRUNCATE.
TRUNCATE_DISPONIBLE=1
for t in "${TABLES[@]}"; do
  executer "GRANT TRUNCATE ON ${t} TO \"${UTILISATEUR_DB}\";" \
           "TRUNCATE sur ${t}" >/dev/null || TRUNCATE_DISPONIBLE=0
done

# Les tables temoins _naif ne sont volontairement PAS accordees :
# elles ne servent qu'a l'etude de benchmark, jamais au pipeline.
jaune "  Les tables _naif restent hors de portee de l'orchestrateur (voulu)"

# =============================================================================
#  3. Verification sous l'identite du role
# =============================================================================
bleu ""
bleu "== Verification sous l'identite du role =="

JETONS="$(aws sts assume-role \
            --role-arn "${SL_AIRFLOW_ROLE_ARN}" \
            --role-session-name "verif-droits-redshift" \
            --duration-seconds 900 \
            --query 'Credentials.[AccessKeyId,SecretAccessKey,SessionToken]' \
            --output text)"
read -r C_CLE C_SEC C_JET <<< "${JETONS}"
unset AWS_PROFILE AWS_DEFAULT_PROFILE || true
export AWS_ACCESS_KEY_ID="${C_CLE}" AWS_SECRET_ACCESS_KEY="${C_SEC}" AWS_SESSION_TOKEN="${C_JET}"

ID="$(executer "SELECT count(*) FROM soundlab.dim_track;" "lecture de dim_track")" \
  && vert "  dim_track contient $(aws redshift-data get-statement-result --id "${ID}" \
             --query 'Records[0][0].longValue' --output text) lignes" \
  || ECHECS=$((ECHECS + 1))

ID="$(executer "SELECT count(*) FROM soundlab.fact_listening;" "lecture de fact_listening")" \
  && vert "  fact_listening contient $(aws redshift-data get-statement-result --id "${ID}" \
             --query 'Records[0][0].longValue' --output text) lignes" \
  || ECHECS=$((ECHECS + 1))

# Controle negatif : les tables temoins doivent rester inaccessibles.
if executer "SELECT count(*) FROM soundlab.dim_track_naif;" \
            "lecture de dim_track_naif (doit ECHOUER)" >/dev/null 2>&1; then
  jaune "  ATTENTION : l'orchestrateur accede aux tables temoins."
  jaune "  Ce n'est pas dangereux, mais ce n'est pas le cloisonnement voulu."
else
  vert "  REFUSE  acces aux tables temoins - conforme"
fi

unset AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN
[[ -n "${PROFIL_SAUVE}" ]] && export AWS_PROFILE="${PROFIL_SAUVE}"
unset C_CLE C_SEC C_JET JETONS

# =============================================================================
bleu ""
if (( ECHECS > 0 )); then
  rouge "${ECHECS} operation(s) en echec - le chargement ne fonctionnera pas."
  exit 1
fi

vert "================================================================"
vert " Droits Redshift en place pour ${UTILISATEUR_DB}"
if (( TRUNCATE_DISPONIBLE == 1 )); then
  vert " TRUNCATE disponible -> chargement par TRUNCATE + COPY"
else
  vert " TRUNCATE indisponible -> chargement par DELETE en transaction"
fi
vert "================================================================"

# Enregistrement pour la suite.
if ! grep -q '^export SL_RS_USER_AIRFLOW=' "${DEPOT}/.soundlab.env" 2>/dev/null; then
  printf 'export SL_RS_USER_AIRFLOW=%s\n' "${UTILISATEUR_DB}" >> "${DEPOT}/.soundlab.env"
  vert " SL_RS_USER_AIRFLOW ajoute a .soundlab.env"
fi
