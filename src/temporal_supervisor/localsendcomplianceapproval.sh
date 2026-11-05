#!/bin/bash
# Send the compliance-approved signal to an open-account child workflow on a
# local Temporal server.
# Usage: ./localsendcomplianceapproval.sh <child-workflow-id>
if [ -z "$1" ]; then
   echo "You must specify a workflow ID"
   exit 1
fi
cd "$(dirname "$0")/../.." || exit 1
[ -f setclaimcheck.sh ] && source ./setclaimcheck.sh
uv run python src/temporal_supervisor/run_send_compliance_approval.py --workflow-id "$1"
