#!/usr/bin/env bash
# =============================================================================
#  SoundLab Analytics — Pilote SQL pour Redshift Serverless
#
#  Exécute un fichier .sql, ou une requête isolée, via la Redshift Data API :
#  aucune connexion réseau directe, aucun mot de passe, authentification par
#  l'identité IAM de l'appelant.
#
#  Usage :
#    ./infra/rs.sh sql/01_schema_etoile.sql
#    ./infra/rs.sh -c "SELECT COUNT(*) FROM soundlab.fact_listening;"
#
#  Conventions du fichier SQL :
#    · une instruction se termine par un point-virgule en fin de ligne
#    · une ligne « -- @ Libellé » nomme l'instruction suivante dans la sortie
#    · {{ROLE_ARN}} et {{BUCKET_CUR}} sont substitués à l'exécution
#
#  La durée affichée provient de la Data API elle-même (champ Duration), pas
#  d'un chronomètre local : elle exclut la latence réseau et l'attente de
#  scrutation, ce qui la rend comparable d'une exécution à l'autre.
# =============================================================================

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "${ROOT}/.soundlab.env"

: "${SL_RS_WORKGROUP:?SL_RS_WORKGROUP manquant — source .soundlab.env}"
: "${SL_RS_DB:?SL_RS_DB manquant}"
: "${SL_RS_ROLE_ARN:?SL_RS_ROLE_ARN manquant}"

TMP="$(mktemp -d)"; trap 'rm -rf "${TMP}"' EXIT

vert()  { printf '\033[1;32m%s\033[0m' "$*"; }
rouge() { printf '\033[1;31m%s\033[0m' "$*"; }
gris()  { printf '\033[2m%s\033[0m' "$*"; }

TOTAL_MS=0
NB_OK=0
NB_KO=0

# ----------------------------------------------------------------------------
#  Session persistante
#
#  Par défaut, la Data API ouvre une session distincte pour chaque instruction.
#  Tout réglage posé par un SET serait donc perdu dès l'instruction suivante —
#  ce qui rendrait impossible, entre autres, la désactivation du cache de
#  résultats indispensable à un benchmark honnête.
#
#  La première instruction ouvre donc une session maintenue ouverte, dont
#  l'identifiant est réutilisé par toutes les suivantes.
# ----------------------------------------------------------------------------
SESSION_ID=""
SESSION_TTL=1800

# ----------------------------------------------------------------------------
#  Exécute une instruction et affiche son résultat
# ----------------------------------------------------------------------------
executer() {
  local sql="$1" libelle="$2"

  printf '  %-52s ' "${libelle}"

  local id
  if [[ -z "${SESSION_ID}" ]]; then
    # Première instruction : ouverture d'une session maintenue
    if ! aws redshift-data execute-statement \
            --workgroup-name "${SL_RS_WORKGROUP}" \
            --database "${SL_RS_DB}" \
            --session-keep-alive-seconds "${SESSION_TTL}" \
            --sql "${sql}" --output json > "${TMP}/exec.json" 2>"${TMP}/err"; then
      rouge "REFUSÉE"; echo
      sed 's/^/      /' "${TMP}/err" >&2
      NB_KO=$(( NB_KO + 1 ))
      return 1
    fi
    id="$(python3 -c 'import json,sys;print(json.load(open(sys.argv[1]))["Id"])' "${TMP}/exec.json")"
    SESSION_ID="$(python3 -c 'import json,sys;print(json.load(open(sys.argv[1])).get("SessionId",""))' "${TMP}/exec.json")"
  else
    # Instructions suivantes : réutilisation de la même session
    if ! id="$(aws redshift-data execute-statement \
                --session-id "${SESSION_ID}" \
                --sql "${sql}" \
                --query Id --output text 2>"${TMP}/err")"; then
      rouge "REFUSÉE"; echo
      sed 's/^/      /' "${TMP}/err" >&2
      NB_KO=$(( NB_KO + 1 ))
      return 1
    fi
  fi

  local etat
  while :; do
    etat="$(aws redshift-data describe-statement --id "${id}" --query Status --output text)"
    case "${etat}" in FINISHED|FAILED|ABORTED) break ;; esac
    sleep 2
  done

  aws redshift-data describe-statement --id "${id}" > "${TMP}/desc.json"

  local duree_ms
  duree_ms="$(python3 -c '
import json,sys
d=json.load(open(sys.argv[1]))
print(round(d.get("Duration",0)/1_000_000))' "${TMP}/desc.json")"

  if [[ "${etat}" != "FINISHED" ]]; then
    rouge "ÉCHEC"; printf '  %s ms\n' "${duree_ms}"
    python3 -c '
import json,sys
d=json.load(open(sys.argv[1]))
print("      " + (d.get("Error") or "erreur non détaillée"))' "${TMP}/desc.json" >&2
    NB_KO=$(( NB_KO + 1 ))
    return 1
  fi

  vert "OK"; printf '  %8s ms\n' "${duree_ms}"
  TOTAL_MS=$(( TOTAL_MS + duree_ms ))
  NB_OK=$(( NB_OK + 1 ))

  # Résultats, s'il y en a
  local a_resultat
  a_resultat="$(python3 -c '
import json,sys
d=json.load(open(sys.argv[1]))
print("1" if d.get("HasResultSet") else "0")' "${TMP}/desc.json")"

  if [[ "${a_resultat}" == "1" ]]; then
    aws redshift-data get-statement-result --id "${id}" > "${TMP}/res.json"
    python3 - "${TMP}/res.json" <<'PY'
import json, sys

d = json.load(open(sys.argv[1]))
cols = [c["name"] for c in d.get("ColumnMetadata", [])]
lignes = []
for row in d.get("Records", []):
    vals = []
    for cell in row:
        if cell.get("isNull"):
            vals.append("NULL")
        else:
            vals.append(str(next(iter(cell.values()))))
    lignes.append(vals)

if cols:
    larg = [len(c) for c in cols]
    for l in lignes:
        for i, v in enumerate(l):
            if i < len(larg):
                larg[i] = max(larg[i], len(v))
    larg = [min(w, 40) for w in larg]

    def fmt(vals):
        return "  ".join(str(v)[:larg[i]].ljust(larg[i]) for i, v in enumerate(vals))

    print("      " + fmt(cols))
    print("      " + "  ".join("-" * w for w in larg))
    for l in lignes[:25]:
        print("      " + fmt(l))
    if len(lignes) > 25:
        print(f"      … {len(lignes) - 25} ligne(s) supplémentaire(s)")
PY
  fi
  return 0
}

# ----------------------------------------------------------------------------
#  Entrée
# ----------------------------------------------------------------------------
substituer() {
  sed -e "s|{{ROLE_ARN}}|${SL_RS_ROLE_ARN}|g" \
      -e "s|{{BUCKET_CUR}}|${SL_B_CUR}|g" \
      -e "s|{{BUCKET_LOG}}|${SL_B_LOG}|g"
}

if [[ "${1:-}" == "-c" ]]; then
  [[ -n "${2:-}" ]] || { echo "Usage : $0 -c \"SQL\"" >&2; exit 1; }
  REQ="$(printf '%s' "$2" | substituer)"
  echo "Redshift ${SL_RS_WORKGROUP} / ${SL_RS_DB}"
  executer "${REQ}" "requête directe"
  exit $?
fi

FICHIER="${1:-}"
[[ -f "${FICHIER}" ]] || { echo "Usage : $0 <fichier.sql> | -c \"SQL\"" >&2; exit 1; }

echo "Redshift ${SL_RS_WORKGROUP} / ${SL_RS_DB}"
echo "Fichier  ${FICHIER}"
echo

substituer < "${FICHIER}" > "${TMP}/sql"

BUF=""
LIBELLE=""
while IFS= read -r ligne || [[ -n "${ligne}" ]]; do

  # Ligne de libellé : « -- @ Titre »
  if [[ "${ligne}" =~ ^[[:space:]]*--[[:space:]]*@[[:space:]]* ]]; then
    LIBELLE="$(printf '%s' "${ligne}" | sed -E 's/^[[:space:]]*--[[:space:]]*@[[:space:]]*//')"
    continue
  fi

  # Commentaire ordinaire ou ligne vide hors instruction en cours
  if [[ -z "${BUF}" && ( -z "${ligne// /}" || "${ligne}" =~ ^[[:space:]]*-- ) ]]; then
    continue
  fi

  BUF="${BUF}${ligne}"$'\n'

  # Fin d'instruction : point-virgule en fin de ligne
  if [[ "${ligne}" =~ \;[[:space:]]*$ ]]; then
    executer "${BUF}" "${LIBELLE:-instruction}" || true
    BUF=""
    LIBELLE=""
  fi
done < "${TMP}/sql"

echo
printf 'Bilan : '
vert "${NB_OK} réussie(s)"
if (( NB_KO > 0 )); then printf ' · '; rouge "${NB_KO} en échec"; fi
gris " · cumul $(( TOTAL_MS / 1000 )),$(printf '%03d' $(( TOTAL_MS % 1000 ))) s de calcul Redshift"
echo

exit $(( NB_KO > 0 ? 1 : 0 ))
