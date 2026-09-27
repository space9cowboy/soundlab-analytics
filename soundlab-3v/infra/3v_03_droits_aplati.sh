#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/../.soundlab.env"
ROLE=SoundLabEMRServerlessExecutionRole
POL=SoundLab3VEcritureDerivesBrut
DOC=$(cat <<JSON
{"Version":"2012-10-17","Statement":[{"Sid":"EcritureDerivesListenBrainz","Effect":"Allow","Action":["s3:PutObject","s3:DeleteObject","s3:AbortMultipartUpload","s3:ListMultipartUploadParts"],"Resource":["arn:aws:s3:::${SL_B_RAW}/listenbrainz/aplati/*","arn:aws:s3:::${SL_B_RAW}/listenbrainz/rebut/*"]}]}
JSON
)
aws iam put-role-policy --role-name "$ROLE" --policy-name "$POL" --policy-document "$DOC"
echo "POLITIQUE_POSEE $POL"
