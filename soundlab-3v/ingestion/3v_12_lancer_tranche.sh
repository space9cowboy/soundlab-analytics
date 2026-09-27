#!/usr/bin/env bash
set -u
cd "$(dirname "$0")/.."
source .soundlab.env
SEUIL="$1"
MAN="$2"
REPRISE="${3:-0000-00}"
U=https://data.metabrainz.org/pub/musicbrainz/listenbrainz/fullexport/listenbrainz-dump-2663-20260915-000002-full
F=listenbrainz-listens-dump-2663-20260915-000002-full.tar.zst
echo "DEBUT $(date '+%Y-%m-%dT%H:%M:%S') REPRISE_APRES=$REPRISE"
curl -sS "$U/$F" | tee >(shasum -a 256 > "$MAN.sha") >(wc -c > "$MAN.n") | zstd -dc 2>/dev/null | python3 ingestion/3v_10_flux_listenbrainz.py --dest "s3://$SL_B_RAW/listenbrainz/ecoutes" --dump-id 2663 --seuil-octets-json "$SEUIL" --manifeste "$MAN" --reprendre-apres "$REPRISE"
ST=("${PIPESTATUS[@]}")
sleep 5
echo "FIN $(date '+%Y-%m-%dT%H:%M:%S')"
echo "CODE_CURL=${ST[0]}"
echo "CODE_INGESTION=${ST[3]}"
echo "OCTETS_LUS=$(tr -d ' ' < "$MAN.n")"
echo "SHA256=$(cut -d' ' -f1 "$MAN.sha")"
