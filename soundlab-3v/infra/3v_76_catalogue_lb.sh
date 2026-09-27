#!/usr/bin/env bash
# =============================================================================
#  SoundLab Analytics — chantier trois V, tâche 4.3
#  Déclare la table externe lb_faits_jour dans le catalogue Glue soundlab_curated
#  (sortie du job 3v_61, partitionnée origine=/mois=), avec ses 431 partitions.
#
#    bash infra/3v_76_catalogue_lb.sh        (depuis la racine de soundlab-3v)
#
#  Exécuté avec le profil administrateur : SoundLabRedshiftS3Role n'a sur Glue
#  que des droits de lecture bornés à soundlab_curated, et on ne les étend pas.
#  Rejouable : la table existante est conservée ; une partition déjà déclarée
#  est rapportée par Glue (AlreadyExistsException) sans être dupliquée.
#
#  Contrôles qui peuvent échouer :
#    · avant écriture : S3 doit contenir exactement 171 mois complet et 260 incr ;
#    · après écriture : Glue doit compter exactement 431 partitions.
# =============================================================================
set -euo pipefail
cd "$(dirname "$0")/.."
P=(--region eu-north-1 --profile soundlab)
D=soundlab_curated
T=lb_faits_jour
B=s3://soundlab-curated-558852/trois_v/entrepot/faits_jour
C=infra/3v_76_catalogue

nc=$(aws s3 ls "${B}/origine=complet/" "${P[@]}" | grep -c 'PRE mois=' || true)
ni=$(aws s3 ls "${B}/origine=incr/" "${P[@]}" | grep -c 'PRE mois=' || true)
echo "PREFIXES_S3 complet ${nc} incr ${ni}"
if [[ "${nc}" -ne 171 || "${ni}" -ne 260 ]]; then
  echo "CATALOGUE_ECHEC prefixes S3 inattendus, aucune ecriture"
  exit 1
fi

if aws glue get-table --database-name "${D}" --name "${T}" "${P[@]}" >/dev/null 2>&1; then
  echo "TABLE_DEJA_PRESENTE ${D}.${T}"
else
  aws glue create-table --database-name "${D}" --table-input "file://${C}/table.json" "${P[@]}"
  echo "TABLE_CREEE ${D}.${T}"
fi

for f in "${C}"/partitions_*.json; do
  err=$(aws glue batch-create-partition --database-name "${D}" --table-name "${T}" \
          --partition-input-list "file://${f}" "${P[@]}" \
          --query 'Errors[].ErrorDetail.ErrorCode' --output text)
  echo "LOT $(basename "${f}") erreurs: ${err:-aucune}"
done

n=$(aws glue get-partitions --database-name "${D}" --table-name "${T}" "${P[@]}" \
      --query 'Partitions[].Values[0]' --output text | tr '\t' '\n' | grep -c . || true)
echo "PARTITIONS_GLUE ${n}"
if [[ "${n}" -eq 431 ]]; then echo "CATALOGUE_OK"; else echo "CATALOGUE_ECHEC"; exit 1; fi
