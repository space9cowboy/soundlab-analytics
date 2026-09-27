#!/usr/bin/env bash
# =============================================================================
#  SoundLab Analytics - prealable a l'UNLOAD
#  Autorise le role Redshift a ecrire dans un prefixe d'export de la couche
#  curated, et attend que la politique publiee soit la politique evaluee.
#
#  POURQUOI PAS LE PREFIXE DEJA AUTORISE ?
#  Le role dispose deja de s3:PutObject sur soundlab-logs/redshift-unload/*.
#  Trois raisons de ne pas l'utiliser pour un export metier :
#    - le compartiment de journaux porte une expiration a 365 jours ;
#    - il est chiffre en AES256, hors du perimetre de la cle du projet
#      decrit dans l'AIPD ;
#    - une donnee metier rangee dans un bucket de logs est un classement
#      fautif, et se voit.
#
#  Usage :  bash 07_droits_unload.sh
#  Idempotent : n'ajoute la declaration que si elle est absente.
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

# shellcheck disable=SC1091
source "${DEPOT}/.soundlab.env"
for v in SL_REGION SL_ACCOUNT_ID SL_B_CUR; do
  [[ -n "${!v:-}" ]] || { rouge "Variable ${v} absente de .soundlab.env"; exit 1; }
done

ROLE="SoundLabRedshiftS3Role"
POLITIQUE="SoundLabRedshiftAccess"
PREFIXE="exports_entrepot"
SID="EcritureExportsEntrepot"

bleu "== Politique actuelle du role ${ROLE} =="
aws iam get-role-policy --role-name "${ROLE}" --policy-name "${POLITIQUE}" \
    --query 'PolicyDocument' --output json > "${TMP}/actuelle.json"
vert "  $(python3 -c "import json;print(len(json.load(open('${TMP}/actuelle.json'))['Statement']))") declaration(s)"

bleu ""
bleu "== Ajout de la declaration d'ecriture =="
python3 - "${TMP}/actuelle.json" "${TMP}/nouvelle.json" "${SID}" \
         "arn:aws:s3:::${SL_B_CUR}/${PREFIXE}/*" <<'FIN_PY'
import json, sys

chemin_in, chemin_out, sid, ressource = sys.argv[1:5]
doc = json.load(open(chemin_in, encoding="utf-8"))

if any(d.get("Sid") == sid for d in doc["Statement"]):
    print("  declaration deja presente")
else:
    doc["Statement"].append({
        "Sid": sid,
        "Effect": "Allow",
        # DeleteObject est requis par l'option CLEANPATH d'UNLOAD, qui vide
        # la destination avant d'ecrire. Sans elle, un second export
        # echouerait sur "path is not empty".
        "Action": ["s3:PutObject", "s3:DeleteObject"],
        "Resource": ressource,
    })
    print("  declaration ajoutee : %s" % ressource)

json.dump(doc, open(chemin_out, "w", encoding="utf-8"), indent=2)
FIN_PY

python3 -m json.tool "${TMP}/nouvelle.json" > /dev/null || { rouge "JSON invalide"; exit 1; }

aws iam put-role-policy --role-name "${ROLE}" --policy-name "${POLITIQUE}" \
    --policy-document "file://${TMP}/nouvelle.json"
vert "  Politique publiee"

# --- Attente de la propagation ------------------------------------------------
# IAM est a coherence differee (incident 10.14). On attend que la decision
# evaluee soit celle qu'on vient de publier, plutot qu'un delai arbitraire.
bleu ""
bleu "== Attente de la propagation =="
ARN_ROLE="arn:aws:iam::${SL_ACCOUNT_ID}:role/${ROLE}"
PROPAGEE=0
for essai in $(seq 1 12); do
  DECISION="$(aws iam simulate-principal-policy \
                --policy-source-arn "${ARN_ROLE}" \
                --action-names s3:PutObject \
                --resource-arns "arn:aws:s3:::${SL_B_CUR}/${PREFIXE}/sonde" \
                --query 'EvaluationResults[0].EvalDecision' --output text 2>/dev/null || echo "")"
  if [[ "${DECISION}" == "allowed" ]]; then
    vert "  Autorisation evaluee : allowed (tentative ${essai})"
    PROPAGEE=1
    break
  fi
  jaune "  propagation en cours (${DECISION:-indisponible}) - ${essai}/12"
  sleep 5
done
(( PROPAGEE == 1 )) || { rouge "Non propagee apres 60 s. Relance dans une minute."; exit 1; }

# --- Controle negatif ---------------------------------------------------------
# L'elargissement doit rester borne au prefixe d'export.
bleu ""
bleu "== Controle : l'elargissement reste borne =="
D="$(aws iam simulate-principal-policy --policy-source-arn "${ARN_ROLE}" \
      --action-names s3:PutObject \
      --resource-arns "arn:aws:s3:::${SL_B_CUR}/music_info/sonde" \
      --query 'EvaluationResults[0].EvalDecision' --output text)"
if [[ "${D}" == *"Deny"* ]]; then
  vert "  ${D}  ecriture dans music_info - correctement refusee"
else
  rouge "  ${D}  ecriture dans music_info AUTORISEE - la politique est trop large."
  exit 1
fi

bleu ""
vert "================================================================"
vert " Destination d'export autorisee :"
vert "   s3://${SL_B_CUR}/${PREFIXE}/"
vert " Chiffree par la cle du projet, sans expiration prematuree."
vert "================================================================"
