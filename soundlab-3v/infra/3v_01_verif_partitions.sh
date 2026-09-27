#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/../.soundlab.env"
aws s3api list-objects-v2 --bucket "$SL_B_RAW" --prefix listenbrainz/ --query 'Contents[].Key' --output json | python3 "$(dirname "$0")/3v_01_verif_partitions.py"
