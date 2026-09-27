#!/usr/bin/env bash
# =============================================================================
#  SoundLab Analytics - Tache 14, etape 1
#  Installe un Airflow local en conteneur, SANS aucun acces AWS.
#
#  Pourquoi sans AWS : si quelque chose casse a cette etape, la cause est
#  Docker ou Airflow. Aucune ambiguite avec une question de permissions.
#  Les identifiants viendront a l'etape 2, par un role dedie a duree limitee.
#
#  Usage :  bash 00_init_airflow.sh
#  Idempotent : relancable sans effet de bord. Le fichier .env n'est jamais
#  ecrase s'il existe deja.
# =============================================================================
set -euo pipefail

RACINE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$RACINE"

bleu()  { printf '\033[0;36m%s\033[0m\n' "$*"; }
vert()  { printf '\033[0;32m%s\033[0m\n' "$*"; }
rouge() { printf '\033[0;31m%s\033[0m\n' "$*"; }

# --- 0. Garde-fous -----------------------------------------------------------
bleu "== Verification de l'environnement =="

if ! command -v docker >/dev/null 2>&1; then
  rouge "docker introuvable. Installe Docker Desktop."
  exit 1
fi

if ! docker info >/dev/null 2>&1; then
  rouge "Le demon Docker ne repond pas."
  rouge "Lance Docker Desktop, attends qu'il affiche 'Running', puis relance ce script."
  exit 1
fi
vert "  Docker operationnel : $(docker --version)"

if ! docker compose version >/dev/null 2>&1; then
  rouge "docker compose (plugin v2) introuvable."
  exit 1
fi
vert "  Compose operationnel : $(docker compose version --short)"

# Garde-fou de securite : ce script ne doit jamais voir d'identifiants AWS.
if [[ -n "${AWS_ACCESS_KEY_ID:-}" || -n "${AWS_SECRET_ACCESS_KEY:-}" ]]; then
  rouge "ATTENTION : des identifiants AWS sont presents dans l'environnement du shell."
  rouge "Cette etape ne doit pas y toucher. Ouvre un terminal propre, ou fais :"
  rouge "  unset AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN"
  exit 1
fi
vert "  Aucun identifiant AWS dans l'environnement - conforme a l'etape 1"

# --- 1. Arborescence ---------------------------------------------------------
bleu ""
bleu "== Creation de l'arborescence =="
mkdir -p dags logs plugins config
vert "  dags/ logs/ plugins/ config/"

# --- 2. Fichier .env ---------------------------------------------------------
# AIRFLOW_UID : sur macOS, Docker Desktop gere deja la correspondance des
# proprietaires. La valeur 50000 est celle de l'utilisateur airflow dans
# l'image officielle ; elle evite un avertissement au demarrage.
if [[ -f .env ]]; then
  vert "  .env deja present, conserve en l'etat"
else
  cat > .env <<'FIN_ENV'
# Identifiant de l'utilisateur dans le conteneur.
# macOS : laisser 50000. Linux : mettre le resultat de `id -u`.
AIRFLOW_UID=50000

# ATTENTION - ce fichier recevra a l'etape 2 des identifiants AWS temporaires.
# Il est deja exclu du versionnement. Ne jamais l'ajouter a git, meme vide.
FIN_ENV
  vert "  .env cree"
fi

# --- 3. docker-compose.yaml --------------------------------------------------
# Choix : LocalExecutor plutot que CeleryExecutor.
# Le compose officiel d'Airflow demarre aussi Redis, un worker Celery, un
# triggerer et Flower - six conteneurs pour executer quatre taches. Le
# LocalExecutor execute les taches en sous-processus du planificateur : deux
# services au lieu de six, memes DAG, meme semantique d'echec et de reprise.
bleu ""
bleu "== Ecriture de docker-compose.yaml =="
cat > docker-compose.yaml <<'FIN_COMPOSE'
# SoundLab Analytics - Airflow local (LocalExecutor)
# Aucun acces AWS a ce stade : voir 00_init_airflow.sh

x-airflow-commun: &airflow-commun
  image: apache/airflow:2.10.5-python3.12
  environment: &airflow-env
    AIRFLOW__CORE__EXECUTOR: LocalExecutor
    AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres/airflow
    AIRFLOW__CORE__LOAD_EXAMPLES: 'false'
    AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION: 'true'
    AIRFLOW__WEBSERVER__EXPOSE_CONFIG: 'false'
    AIRFLOW__SCHEDULER__DAG_DIR_LIST_INTERVAL: '15'
  volumes:
    - ./dags:/opt/airflow/dags
    - ./logs:/opt/airflow/logs
    - ./plugins:/opt/airflow/plugins
    - ./config:/opt/airflow/config
  user: "${AIRFLOW_UID:-50000}:0"

services:

  postgres:
    image: postgres:16
    environment:
      POSTGRES_USER: airflow
      POSTGRES_PASSWORD: airflow
      POSTGRES_DB: airflow
    volumes:
      - postgres-airflow:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD", "pg_isready", "-U", "airflow"]
      interval: 10s
      timeout: 5s
      retries: 5
    restart: unless-stopped

  # Initialisation de la base de metadonnees et creation du compte.
  # Ce service s'execute une fois puis se termine ; les deux suivants
  # attendent sa terminaison reussie.
  airflow-init:
    <<: *airflow-commun
    entrypoint: /bin/bash
    command:
      - -c
      - |
        airflow db migrate
        airflow users create \
          --role Admin --username admin --password admin \
          --firstname Loic --lastname Rabetsanta \
          --email loic@soundlab.local 2>/dev/null || true
        echo "Initialisation terminee."
    depends_on:
      postgres:
        condition: service_healthy
    restart: on-failure

  airflow-scheduler:
    <<: *airflow-commun
    command: scheduler
    depends_on:
      airflow-init:
        condition: service_completed_successfully
    restart: unless-stopped

  airflow-webserver:
    <<: *airflow-commun
    command: webserver
    # Liaison sur la boucle locale uniquement : l'interface n'est pas
    # exposee aux autres machines du reseau.
    ports:
      - "127.0.0.1:8080:8080"
    depends_on:
      airflow-init:
        condition: service_completed_successfully
    healthcheck:
      test: ["CMD", "curl", "--fail", "http://localhost:8080/health"]
      interval: 30s
      timeout: 10s
      retries: 5
      start_period: 40s
    restart: unless-stopped

volumes:
  postgres-airflow:
FIN_COMPOSE
vert "  docker-compose.yaml ecrit"

# --- 4. DAG de verification --------------------------------------------------
bleu ""
bleu "== Ecriture du DAG de verification =="
cat > dags/00_verification_installation.py <<'FIN_DAG'
"""
DAG de verification de l'installation - SoundLab Analytics.

Il ne touche a rien : quatre commandes shell qui affichent du texte.
Son seul role est de prouver que le planificateur lit les fichiers, que les
taches s'executent et que la forme du graphe est bien celle qu'on a declaree.

Il reproduit volontairement la forme du DAG reel, qui est un losange et non
une chaine :

    source_a ---+
                +--> porte_qualite --> traitement_aval
    source_b ---+

source_a et source_b ne dependent pas l'une de l'autre. C'est porte_qualite
qui a besoin des deux.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

ARGUMENTS_PAR_DEFAUT = {
    # Une seule nouvelle tentative : sur un echec de logique metier, reessayer
    # ne sert a rien. Le reessai est utile contre un alea reseau, pas contre
    # une erreur de code.
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
    "depends_on_past": False,
}

with DAG(
    dag_id="00_verification_installation",
    description="Verification de l'installation - aucune dependance AWS",
    default_args=ARGUMENTS_PAR_DEFAUT,
    start_date=datetime(2026, 9, 1),
    # None : le DAG ne se declenche que manuellement. Les donnees du projet
    # n'ont aucun horodatage, donc aucune periodicite n'aurait de sens ici.
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["soundlab", "verification"],
) as dag:

    source_a = BashOperator(
        task_id="source_a",
        bash_command="echo 'Lecture de la source A (music_info)' && sleep 3",
    )

    source_b = BashOperator(
        task_id="source_b",
        bash_command="echo 'Lecture de la source B (listening_history)' && sleep 3",
    )

    porte_qualite = BashOperator(
        task_id="porte_qualite",
        bash_command=(
            "echo 'Controles de qualite : 13/13 passes' && "
            "echo 'Code de sortie 0 -> le graphe continue' && exit 0"
        ),
    )

    traitement_aval = BashOperator(
        task_id="traitement_aval",
        bash_command="echo 'Construction des variables, puis chargement'",
    )

    # La declaration des dependances. La liste a gauche signifie que les deux
    # taches doivent etre terminees avant que porte_qualite ne demarre.
    [source_a, source_b] >> porte_qualite >> traitement_aval
FIN_DAG
vert "  dags/00_verification_installation.py ecrit"

# --- 5. Exclusions de versionnement -----------------------------------------
bleu ""
bleu "== Mise a jour du .gitignore du depot =="
GITIGNORE="$RACINE/../.gitignore"
if [[ ! -f "$GITIGNORE" ]]; then
  touch "$GITIGNORE"
fi
ajoute_ignore() {
  local motif="$1"
  if ! grep -qxF "$motif" "$GITIGNORE" 2>/dev/null; then
    printf '%s\n' "$motif" >> "$GITIGNORE"
    vert "  ajoute : $motif"
  else
    vert "  deja present : $motif"
  fi
}
ajoute_ignore "orchestration/logs/"
ajoute_ignore "orchestration/plugins/"
ajoute_ignore ".env"
ajoute_ignore "**/.env"

# --- 6. Validation de la configuration ---------------------------------------
bleu ""
bleu "== Validation de docker-compose.yaml =="
if docker compose config -q; then
  vert "  Configuration valide"
else
  rouge "  Configuration invalide - ne pas demarrer"
  exit 1
fi

# --- 7. Recuperation des images ----------------------------------------------
bleu ""
bleu "== Telechargement des images (quelques minutes la premiere fois) =="
docker compose pull --quiet
vert "  Images disponibles localement"

bleu ""
vert "================================================================"
vert " Etape 1 prete. Rien n'a encore demarre."
vert ""
vert " Pour demarrer :   cd orchestration && docker compose up -d"
vert " Interface :       http://127.0.0.1:8080   (admin / admin)"
vert " Pour arreter :    docker compose down"
vert "================================================================"
