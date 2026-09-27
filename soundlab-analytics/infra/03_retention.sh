#!/usr/bin/env bash
# =============================================================================
#  SoundLab Analytics — Action A1 de l'AIPD
#  Application technique des durées de conservation définies au § 1.5
#
#  Idempotent : relançable sans effet de bord.
# =============================================================================
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "${ROOT}/.soundlab.env"

ok() { printf '\033[1;32m  ✓\033[0m %s\n' "$*"; }
TMP="$(mktemp -d)"; trap 'rm -rf "${TMP}"' EXIT

# -----------------------------------------------------------------------------
#  Subtilité déterminante sur un bucket versionné : `Expiration` ne SUPPRIME
#  pas l'objet, elle pose un marqueur de suppression et bascule la version
#  courante en version non courante. La donnée n'est réellement effacée qu'à
#  l'expiration de cette version non courante.
#
#  Un cycle de vie sans `NoncurrentVersionExpiration` donne donc l'illusion
#  d'une purge alors que les données restent intégralement récupérables — et
#  toujours soumises au RGPD.
#
#  Effacement effectif = durée annoncée + 30 jours de purge technique.
# -----------------------------------------------------------------------------
appliquer() {
  local bucket="$1" jours="$2" libelle="$3"

  if [[ "${jours}" == "0" ]]; then
    cat > "${TMP}/lc.json" <<'JSON'
{"Rules":[
 {"ID":"purge-technique","Status":"Enabled","Filter":{"Prefix":""},
  "NoncurrentVersionExpiration":{"NoncurrentDays":30},
  "AbortIncompleteMultipartUpload":{"DaysAfterInitiation":7},
  "Expiration":{"ExpiredObjectDeleteMarker":true}}
]}
JSON
  else
    cat > "${TMP}/lc.json" <<JSON
{"Rules":[
 {"ID":"retention-rgpd","Status":"Enabled","Filter":{"Prefix":""},
  "Expiration":{"Days":${jours}},
  "NoncurrentVersionExpiration":{"NoncurrentDays":30},
  "AbortIncompleteMultipartUpload":{"DaysAfterInitiation":7}}
]}
JSON
  fi

  aws s3api put-bucket-lifecycle-configuration \
    --bucket "${bucket}" --lifecycle-configuration "file://${TMP}/lc.json" >/dev/null
  ok "${bucket} — ${libelle}"
}

echo "Application des durées de conservation (AIPD § 1.5)"
appliquer "${SL_B_RAW}" 365 "12 mois (donnees brutes)"
appliquer "${SL_B_CUR}" 730 "24 mois (donnees preparees)"
appliquer "${SL_B_LOG}" 365 "12 mois (journaux)"
appliquer "${SL_B_MOD}"   0 "sans expiration (aucune donnee personnelle)"
appliquer "${SL_B_SCR}"   0 "sans expiration (code source)"

echo
echo "Verification"
for b in "${SL_B_RAW}" "${SL_B_CUR}" "${SL_B_LOG}" "${SL_B_MOD}" "${SL_B_SCR}"; do
  printf '  %-32s ' "${b}"
  aws s3api get-bucket-lifecycle-configuration --bucket "${b}" \
    --query 'Rules[0].{regle:ID,expiration:Expiration.Days,purge:NoncurrentVersionExpiration.NoncurrentDays}' \
    --output text
done
