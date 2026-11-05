#!/bin/bash
# Start the Temporal + Strands worker against Temporal Cloud (settings from
# setcloudenv.sh in the project root; copy src/temporal_supervisor/setcloudenv.example).
# Usage: startcloudworker.sh [open-account-task-queue]
# With a task queue name, the open-account child workflow runs on that queue (set as
# TEMPORAL_TASK_QUEUE_OPEN_ACCOUNT) and this worker doesn't register the Python
# version of it: start the Java worker with the same name (java/startcloudworker.sh).
cd "$(dirname "$0")/../.." || exit 1
if [ ! -f setcloudenv.sh ]; then
   echo "setcloudenv.sh not found in the project root (copy src/temporal_supervisor/setcloudenv.example)"
   exit 1
fi
source ./setcloudenv.sh
[ -f setgeminikey.sh ] && source ./setgeminikey.sh
[ -f setclaimcheck.sh ] && source ./setclaimcheck.sh
if [ -n "$1" ]; then
  export TEMPORAL_TASK_QUEUE_OPEN_ACCOUNT="$1"
  echo "*** Using $1 as the task queue for the open-account workflow. Remember to also start the Java worker with it. ***"
fi
uv run python src/temporal_supervisor/run_worker.py
