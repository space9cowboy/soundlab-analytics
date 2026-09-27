#!/usr/bin/env bash
# =============================================================================
#  SoundLab Analytics - Tache 14, etape 2b
#  Delivre au conteneur Airflow des identifiants temporaires issus du role
#  restreint, et jamais la cle permanente.
#
#  A relancer quand les jetons expirent. La duree par defaut est d'une heure ;
#  le role autorise jusqu'a quatre heures.
#
#      bash 02_jetons.sh          # 1 h
#      bash 02_jetons.sh 14400    # 4 h, maximum autorise par le role
#
#  Ce script n'affiche JAMAIS la valeur d'un identifiant.
# =============================================================================
set -euo pipefail

RACINE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEPOT="$(cd "${RACINE}/.." && pwd)"

bleu()  { printf '\033[0;36m%s\033[0m\n' "$*"; }
vert()  { printf '\033[0;32m%s\033[0m\n' "$*"; }
jaune() { printf '\033[0;33m%s\033[0m\n' "$*"; }
rouge() { printf '\033[0;31m%s\033[0m\n' "$*"; }

DUREE="${1:-3600}"

# --- 0. Configuration ---------------------------------------------------------
bleu "== Chargement de la configuration =="
# shellcheck disable=SC1091
source "${DEPOT}/.soundlab.env"

if [[ -z "${SL_AIRFLOW_ROLE_ARN:-}" ]]; then
  rouge "SL_AIRFLOW_ROLE_ARN absent. Lance d'abord 01_role_airflow.sh."
  exit 1
fi
vert "  Role cible : ${SL_AIRFLOW_ROLE_ARN}"

# Des jetons perimes encore exportes dans le shell empecheraient d'assumer le
# role. On repasse explicitement par l'identite souche du profil.
unset AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN || true

# --- 1. Obtention des jetons --------------------------------------------------
bleu ""
bleu "== Obtention d'identifiants temporaires (${DUREE} s) =="
REPONSE="$(aws sts assume-role \
             --role-arn "${SL_AIRFLOW_ROLE_ARN}" \
             --role-session-name "airflow-local-$(date +%Y%m%d-%H%M%S)" \
             --duration-seconds "${DUREE}" \
             --query 'Credentials.[AccessKeyId,SecretAccessKey,SessionToken,Expiration]' \
             --output text)"

read -r JETON_CLE JETON_SECRET JETON_SESSION JETON_EXPIRE <<< "${REPONSE}"

if [[ -z "${JETON_SESSION}" ]]; then
  rouge "Aucun jeton de session recu."
  exit 1
fi
vert "  Jetons obtenus, expiration : ${JETON_EXPIRE}"
vert "  Empreinte de la cle temporaire : ...${JETON_CLE: -4}  (valeur jamais affichee)"

# --- 2. Transmission des variables par docker-compose -------------------------
# Idempotent : n'ajoute le bloc que s'il est absent.
bleu ""
bleu "== Transmission des variables au conteneur =="
python3 - "${RACINE}/docker-compose.yaml" <<'FIN_PY'
import sys

chemin = sys.argv[1]
contenu = open(chemin, encoding="utf-8").read()

if "AWS_SESSION_TOKEN" in contenu:
    print("  docker-compose.yaml transmet deja les variables AWS")
    sys.exit(0)

ancre = "    AIRFLOW__SCHEDULER__DAG_DIR_LIST_INTERVAL: '15'\n"
if ancre not in contenu:
    print("  ANCRE INTROUVABLE - ajoute le bloc a la main sous 'environment:'")
    sys.exit(1)

bloc = ancre + """    # Identifiants temporaires issus de SoundLabAirflowRole.
    # Alimentes par le fichier .env, renouveles par 02_jetons.sh.
    # Aucune cle permanente ne franchit cette frontiere.
    AWS_ACCESS_KEY_ID: ${AWS_ACCESS_KEY_ID:-}
    AWS_SECRET_ACCESS_KEY: ${AWS_SECRET_ACCESS_KEY:-}
    AWS_SESSION_TOKEN: ${AWS_SESSION_TOKEN:-}
    AWS_REGION: ${AWS_REGION:-eu-north-1}
    AWS_DEFAULT_REGION: ${AWS_REGION:-eu-north-1}
"""

open(chemin, "w", encoding="utf-8").write(contenu.replace(ancre, bloc, 1))
print("  bloc de variables AWS ajoute a docker-compose.yaml")
FIN_PY

# --- 3. Ecriture du fichier .env ----------------------------------------------
bleu ""
bleu "== Ecriture de ${RACINE}/.env =="
UID_AIRFLOW="$(grep -E '^AIRFLOW_UID=' "${RACINE}/.env" 2>/dev/null | head -1 | cut -d= -f2 || true)"
UID_AIRFLOW="${UID_AIRFLOW:-50000}"

umask 077
cat > "${RACINE}/.env" <<FIN_ENV
# ==========================================================================
#  NE JAMAIS VERSIONNER CE FICHIER.
#  Identifiants temporaires du role SoundLabAirflowRole.
#  Expiration : ${JETON_EXPIRE}
#  Renouvellement : bash 02_jetons.sh
# ==========================================================================
AIRFLOW_UID=${UID_AIRFLOW}
AWS_REGION=${SL_REGION}
AWS_ACCESS_KEY_ID=${JETON_CLE}
AWS_SECRET_ACCESS_KEY=${JETON_SECRET}
AWS_SESSION_TOKEN=${JETON_SESSION}
FIN_ENV
chmod 600 "${RACINE}/.env"
vert "  Ecrit, permissions 600 (lisible par toi seul)"

unset JETON_CLE JETON_SECRET JETON_SESSION REPONSE

# Garde-fou : le fichier ne doit pas etre suivi par git.
if git -C "${DEPOT}" ls-files --error-unmatch "orchestration/.env" >/dev/null 2>&1; then
  rouge "ALERTE : orchestration/.env est suivi par git. Retire-le immediatement :"
  rouge "  git rm --cached orchestration/.env"
  exit 1
fi
vert "  Non suivi par git - verifie"

# --- 4. Redemarrage et verification -------------------------------------------
bleu ""
bleu "== Application aux conteneurs =="
cd "${RACINE}"
docker compose up -d --no-deps airflow-scheduler airflow-webserver >/dev/null 2>&1
sleep 8

PRESENT="$(docker compose exec -T airflow-scheduler \
             sh -c 'test -n "$AWS_SESSION_TOKEN" && echo oui || echo non' 2>/dev/null || echo non)"
if [[ "${PRESENT}" != "oui" ]]; then
  rouge "  Le conteneur ne voit pas AWS_SESSION_TOKEN."
  rouge "  Verifie le bloc 'environment' de docker-compose.yaml."
  exit 1
fi
vert "  Le conteneur recoit bien un jeton de session"

# Verification de l'identite effective vue depuis le conteneur, si boto3 y est.
IDENTITE="$(docker compose exec -T airflow-scheduler python -c \
  'import boto3; print(boto3.client("sts").get_caller_identity()["Arn"])' 2>/dev/null || echo "")"

if [[ -n "${IDENTITE}" ]]; then
  if [[ "${IDENTITE}" == *"assumed-role/SoundLabAirflowRole/"* ]]; then
    vert "  Identite vue par Airflow : ${IDENTITE}"
  else
    rouge "  Identite inattendue dans le conteneur : ${IDENTITE}"
    exit 1
  fi
else
  jaune "  boto3 indisponible dans l'image - verification d'identite reportee"
fi

bleu ""
vert "================================================================"
vert " Le conteneur Airflow opere sous un role restreint."
vert " Expiration des jetons : ${JETON_EXPIRE}"
vert ""
vert " Pour renouveler : bash orchestration/02_jetons.sh"
vert "================================================================"
