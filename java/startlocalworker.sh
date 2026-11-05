#!/bin/bash
# Starts the Java Open Account worker against a local Temporal server (127.0.0.1:7233 by
# default; TEMPORAL_ADDRESS / TEMPORAL_NAMESPACE override it, see src/main/resources/application.yaml).
# The argument is the Open Account task queue; pass the same value to
# src/temporal_supervisor/startlocalworker.sh so the Python supervisor starts the workflow there.
# Can be run from any directory.
if [ -z "$1" ]
  then
    echo "**** Missing required parameter: Task Queue Name for the Open Account Workflow"
    echo "(it sets TEMPORAL_TASK_QUEUE_OPEN_ACCOUNT; use the same name when starting the Python worker:"
    echo "src/temporal_supervisor/startlocalworker.sh <name>)"
    exit 1
fi
cd "$(dirname "$0")" || exit 1
export TEMPORAL_TASK_QUEUE_OPEN_ACCOUNT="$1"
[ -f ../setclaimcheck.sh ] && source ../setclaimcheck.sh
echo "*** Task queue: $TEMPORAL_TASK_QUEUE_OPEN_ACCOUNT"
./gradlew runWorker
