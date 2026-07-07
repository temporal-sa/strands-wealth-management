#!/bin/bash
# Send the compliance-approved signal to an open-account child workflow.
# Usage: ./sendcomplianceapproval.sh <child-workflow-id>
cd "$(dirname "$0")/../.." || exit 1
[ -f setgeminikey.sh ] && source ./setgeminikey.sh
uv run python src/temporal_supervisor/run_send_compliance_approval.py --workflow-id "$1"
