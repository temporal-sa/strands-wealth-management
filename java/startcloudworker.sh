#!/bin/bash
# Starts the Java Open Account worker against Temporal Cloud, using the connection settings
# in ../setcloudenv.sh (see src/temporal_supervisor/setcloudenv.example) and, if it exists,
# the claim check settings in ../setclaimcheck.sh.
# The argument is the Open Account task queue; pass the same value to
# src/temporal_supervisor/startcloudworker.sh so the Python supervisor starts the workflow there.
# Can be run from any directory.
if [ -z "$1" ]
  then
    echo "**** Missing required parameter: Task Queue Name for the Open Account Workflow"
    echo "(it sets TEMPORAL_TASK_QUEUE_OPEN_ACCOUNT; use the same name when starting the Python worker:"
    echo "src/temporal_supervisor/startcloudworker.sh <name>)"
    exit 1
fi
cd "$(dirname "$0")" || exit 1
export TEMPORAL_TASK_QUEUE_OPEN_ACCOUNT="$1"

if [ ! -f ../setcloudenv.sh ]; then
  echo "**** ../setcloudenv.sh not found. Copy src/temporal_supervisor/setcloudenv.example to the"
  echo "repository root as setcloudenv.sh and fill in your Temporal Cloud details."
  exit 1
fi
[ -f ../setclaimcheck.sh ] && source ../setclaimcheck.sh
source ../setcloudenv.sh

# Map the settings in setcloudenv.sh to the names the Java worker reads (application-tc.yaml).
export TEMPORAL_CERT_PATH="${TEMPORAL_CERT_PATH:-$TEMPORAL_TLS_CLIENT_CERT_PATH}"
KEY_PATH="${TEMPORAL_KEY_PKCS8_PATH:-${TEMPORAL_KEY_PATH:-$TEMPORAL_TLS_CLIENT_KEY_PATH}}"

for var in TEMPORAL_ADDRESS TEMPORAL_NAMESPACE TEMPORAL_CERT_PATH; do
  if [ -z "${!var}" ]; then
    echo "**** $var is not set. Check ../setcloudenv.sh."
    exit 1
  fi
done
if [ -z "$KEY_PATH" ]; then
  echo "**** No client key set. Set TEMPORAL_TLS_CLIENT_KEY_PATH (or TEMPORAL_KEY_PKCS8_PATH) in ../setcloudenv.sh."
  exit 1
fi
for file in "$TEMPORAL_CERT_PATH" "$KEY_PATH"; do
  if [ ! -r "$file" ]; then
    echo "**** Cannot read $file. Check the paths in ../setcloudenv.sh."
    exit 1
  fi
done

# Java's TLS needs the client key in unencrypted PKCS8 format (-----BEGIN PRIVATE KEY-----).
# Keys from tcld or openssl are often PKCS1 (-----BEGIN RSA PRIVATE KEY-----) or SEC1 (EC).
if ! grep -q -- "-----BEGIN PRIVATE KEY-----" "$KEY_PATH"; then
  PKCS8_PATH="${KEY_PATH%.*}.pkcs8.key"
  echo "**** The client key $KEY_PATH is not an unencrypted PKCS8 key, which the Java worker needs."
  echo "Convert it with:"
  echo ""
  echo "  openssl pkcs8 -topk8 -nocrypt -in \"$KEY_PATH\" -out \"$PKCS8_PATH\""
  echo ""
  echo "then add this line to ../setcloudenv.sh (the Python worker keeps using TEMPORAL_TLS_CLIENT_KEY_PATH):"
  echo ""
  echo "  export TEMPORAL_KEY_PKCS8_PATH=\"$PKCS8_PATH\""
  exit 1
fi
export TEMPORAL_KEY_PKCS8_PATH="$KEY_PATH"

# Use the Temporal Cloud Spring profile (application-tc.yaml) for the worker's client.
export SPRING_PROFILES_ACTIVE=tc

echo "*** Connecting the Open Account worker to $TEMPORAL_NAMESPACE at $TEMPORAL_ADDRESS"
echo "*** Task queue: $TEMPORAL_TASK_QUEUE_OPEN_ACCOUNT"
./gradlew runWorker
