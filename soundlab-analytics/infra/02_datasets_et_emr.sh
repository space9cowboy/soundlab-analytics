#!/usr/bin/env bash
# =============================================================================
#  SoundLab Analytics — Bloc 6 Big Data
#  Étape 2 : chargement des données brutes + moteur de calcul
#
#  1. Récupère les deux CSV du Million Song Dataset (Kaggle) si absents
#  2. Calcule un manifeste d'empreintes SHA-256 (traçabilité / lignage)
#  3. Les téléverse dans s3://<raw>/msd/ avec contrôle d'intégrité
#  4. Crée l'application EMR Serverless `soundlab-spark`
#
#  Usage :  ./infra/02_datasets_et_emr.sh [chemin/vers/dossier/data]
#  Auteur : Loïc Rabetsanta
# =============================================================================

set -euo pipefail

# ----------------------------------------------------------------------------
# Contexte
# ----------------------------------------------------------------------------
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=/dev/null
source "$ROOT/.soundlab.env"

DATA_DIR="${1:-$ROOT/data}"
KAGGLE_SLUG="undefinenull/million-song-dataset-spotify-lastfm"
APP_NAME="soundlab-spark"

F_MUSIC="Music Info.csv"
F_HIST="User Listening History.csv"
K_MUSIC="msd/music_info.csv"                 # clés S3 : sans espace ni majuscule
K_HIST="msd/user_listening_history.csv"

log()  { printf '\033[1;34m[%s]\033[0m %s\n' "$(date +%H:%M:%S)" "$*"; }
ok()   { printf '\033[1;32m  ✓\033[0m %s\n' "$*"; }
skip() { printf '\033[1;33m  =\033[0m %s (déjà présent)\n' "$*"; }
die()  { printf '\033[1;31m  ✗ %s\033[0m\n' "$*" >&2; exit 1; }

[[ "$(aws configure get region)" == "$SL_REGION" ]] \
  || die "Région du profil incohérente avec .soundlab.env"
ok "Contexte : compte $SL_ACCOUNT_ID / région $SL_REGION"

mkdir -p "$DATA_DIR"

# ----------------------------------------------------------------------------
# 1. Récupération des CSV
# ----------------------------------------------------------------------------
log "Jeux de données locaux ($DATA_DIR)"

if [[ -f "$DATA_DIR/$F_MUSIC" && -f "$DATA_DIR/$F_HIST" ]]; then
  skip "les deux CSV"
else
  command -v kaggle >/dev/null \
    || die "CLI Kaggle absente. Installe-la : pip install kaggle
       puis dépose ton jeton API dans ~/.kaggle/kaggle.json (chmod 600).
       Alternative : copie manuellement les deux CSV dans $DATA_DIR"
  [[ -f "$HOME/.kaggle/kaggle.json" ]] \
    || die "Jeton Kaggle manquant : ~/.kaggle/kaggle.json"

  log "Téléchargement depuis Kaggle (plusieurs minutes)"
  kaggle datasets download -d "$KAGGLE_SLUG" -p "$DATA_DIR" --unzip
  ok "archive décompressée"
fi

for f in "$F_MUSIC" "$F_HIST"; do
  [[ -f "$DATA_DIR/$f" ]] || die "Fichier introuvable : $DATA_DIR/$f"
done

# ----------------------------------------------------------------------------
# 2. Manifeste d'intégrité
#    Versionné dans Git : c'est la preuve, pour le jury, que les données
#    analysées sont exactement celles téléversées. Lignage de données.
# ----------------------------------------------------------------------------
log "Manifeste SHA-256"

MANIFEST="$DATA_DIR/MANIFEST.sha256"
: > "$MANIFEST"
declare -A LOCAL_SIZE
for f in "$F_MUSIC" "$F_HIST"; do
  p="$DATA_DIR/$f"
  h="$(shasum -a 256 "$p" | awk '{print $1}')"
  s="$(wc -c < "$p" | tr -d ' ')"
  n="$(( $(wc -l < "$p") - 1 ))"                  # -1 : ligne d'en-tête
  LOCAL_SIZE["$f"]="$s"
  printf '%s  %s  %s octets  %s lignes\n' "$h" "$f" "$s" "$n" >> "$MANIFEST"
  ok "$f — $n lignes, $(( s / 1024 / 1024 )) Mio"
done
ok "manifeste écrit : $MANIFEST"

# ----------------------------------------------------------------------------
# 3. Téléversement vers S3
#    --checksum-algorithm SHA256 fait vérifier chaque part par S3 à la
#    réception : une corruption réseau est rejetée côté serveur, pas
#    découverte trois jours plus tard dans Spark.
# ----------------------------------------------------------------------------
log "Téléversement vers s3://$SL_B_RAW/msd/"

# Parallélisme : accélère nettement le gros fichier sur une connexion domestique
aws configure set s3.max_concurrent_requests 20
aws configure set s3.multipart_chunksize 64MB

upload() {
  local src="$1" key="$2"
  if aws s3api head-object --bucket "$SL_B_RAW" --key "$key" >/dev/null 2>&1; then
    skip "s3://$SL_B_RAW/$key"
  else
    aws s3 cp "$src" "s3://$SL_B_RAW/$key" \
      --checksum-algorithm SHA256 \
      --metadata "source=kaggle:$KAGGLE_SLUG" \
      --only-show-errors
    ok "téléversé : $key"
  fi
}

upload "$DATA_DIR/$F_MUSIC" "$K_MUSIC"
upload "$DATA_DIR/$F_HIST"  "$K_HIST"

# Vérification : la taille distante doit correspondre à la taille locale
log "Contrôle d'intégrité"
verify() {
  local f="$1" key="$2"
  local remote
  remote="$(aws s3api head-object --bucket "$SL_B_RAW" --key "$key" \
            --query ContentLength --output text)"
  if [[ "$remote" == "${LOCAL_SIZE[$f]}" ]]; then
    ok "$key — $remote octets, conforme"
  else
    die "$key — taille distante $remote ≠ locale ${LOCAL_SIZE[$f]}"
  fi
}
verify "$F_MUSIC" "$K_MUSIC"
verify "$F_HIST"  "$K_HIST"

# Confirme que le chiffrement par défaut s'est bien appliqué
ENC="$(aws s3api head-object --bucket "$SL_B_RAW" --key "$K_MUSIC" \
       --query 'ServerSideEncryption' --output text)"
ok "chiffrement au repos : $ENC"

# ----------------------------------------------------------------------------
# 4. Application EMR Serverless
#    - ARM64 (Graviton) : ~20 % moins cher que x86 à performance égale
#    - PAS de capacité pré-initialisée : on ne paie que pendant les jobs.
#      Contrepartie assumée : ~60 à 120 s de démarrage à froid par job.
#    - Arrêt automatique après 5 min d'inactivité : filet de sécurité budget.
#    - Plafond à 32 vCPU : borne haute infranchissable, même en cas de bug.
# ----------------------------------------------------------------------------
log "Application EMR Serverless"

EXISTING="$(aws emr-serverless list-applications \
            --query "applications[?name=='$APP_NAME'].id" --output text)"

if [[ -n "$EXISTING" && "$EXISTING" != "None" ]]; then
  skip "application $APP_NAME ($EXISTING)"
  APP_ID="$EXISTING"
else
  # Les versions supportées par EMR Serverless évoluent ; on essaie de la plus
  # récente à la plus ancienne et on garde la première acceptée.
  DETECTED="$(aws emr list-release-labels --query 'ReleaseLabels' --output text 2>/dev/null \
              | tr '\t' '\n' | grep -E '^emr-7\.[0-9]+\.0$' | sort -V | tail -1 || true)"
  APP_ID=""
  for RL in $DETECTED emr-7.9.0 emr-7.5.0 emr-7.2.0 emr-6.15.0; do
    [[ -n "$RL" ]] || continue
    log "  essai avec $RL"
    if APP_ID="$(aws emr-serverless create-application \
          --name "$APP_NAME" \
          --release-label "$RL" \
          --type SPARK \
          --architecture ARM64 \
          --maximum-capacity '{"cpu":"32vCPU","memory":"128GB","disk":"400GB"}' \
          --auto-start-configuration '{"enabled":true}' \
          --auto-stop-configuration '{"enabled":true,"idleTimeoutMinutes":5}' \
          --tags Project=SoundLab,Env=dev,Owner=loic \
          --query 'applicationId' --output text 2>/dev/null)"; then
      ok "application créée avec $RL — id $APP_ID"
      echo "export SL_EMR_RELEASE=$RL" >> "$ROOT/.soundlab.env"
      break
    fi
    APP_ID=""
  done
  [[ -n "$APP_ID" ]] || die "Aucune version EMR acceptée. Lance la commande sans
       '2>/dev/null' pour lire l'erreur exacte."
fi

# ----------------------------------------------------------------------------
# 5. Mise à jour du fichier d'environnement
# ----------------------------------------------------------------------------
grep -q '^export SL_EMR_APP_ID=' "$ROOT/.soundlab.env" \
  && sed -i '' "s|^export SL_EMR_APP_ID=.*|export SL_EMR_APP_ID=$APP_ID|" "$ROOT/.soundlab.env" \
  || echo "export SL_EMR_APP_ID=$APP_ID" >> "$ROOT/.soundlab.env"

echo
log "Étape 2 terminée"
cat <<REC

  Récapitulatif
  ─────────────
  s3://$SL_B_RAW/$K_MUSIC
  s3://$SL_B_RAW/$K_HIST
  Manifeste       $MANIFEST
  EMR Serverless  $APP_NAME — $APP_ID
                  ARM64, arrêt auto 5 min, plafond 32 vCPU

  Recharge l'environnement :  source .soundlab.env

REC
