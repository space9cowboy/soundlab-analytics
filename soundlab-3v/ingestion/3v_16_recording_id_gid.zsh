#!/bin/zsh
# 3v_16 : extrait la correspondance id interne -> MBID (gid) de la table recording
# du dump MusicBrainz principal (CC0), en flux, sans stocker l'archive.
# Usage : zsh ingestion/3v_16_recording_id_gid.zsh <repertoire_sortie>
# Sorties : recording_id_gid.tsv, recording_nf.txt, mbdump_sha_flux.txt
set -u
SORTIE=${1:?repertoire de sortie requis}
D=https://data.metabrainz.org/pub/musicbrainz/data/fullexport/20260923-002121
SHA_ATTENDU=46fb50474948b42880f373ff463ce48b4d30c5bde3802c495eae07e35f73a05b
mkdir -p "$SORTIE" && cd "$SORTIE" || exit 1
date "+DEBUT %Y-%m-%d %H:%M:%S"
curl -sSf "$D/mbdump.tar.bz2" \
  | tee >(shasum -a 256 | cut -d' ' -f1 > mbdump_sha_flux.txt) \
  | tar -xOjf - mbdump/recording \
  | awk -F'\t' '{c[NF]++; print $1"\t"$2} END{for(k in c) print "NF", k, c[k] > "recording_nf.txt"}' \
  > recording_id_gid.tsv
echo "STATUTS $pipestatus"
sleep 10
SHA_LU=$(cat mbdump_sha_flux.txt 2>/dev/null)
if [[ "$SHA_LU" == "$SHA_ATTENDU" ]]; then echo "SHA_OK"; else echo "SHA_KO lu=$SHA_LU"; fi
cat recording_nf.txt 2>/dev/null
echo "LIGNES $(wc -l < recording_id_gid.tsv)"
date "+FIN %Y-%m-%d %H:%M:%S"
