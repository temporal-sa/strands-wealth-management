#!/bin/bash
# Start the Temporal + Strands worker locally.
cd "$(dirname "$0")/../.." || exit 1
[ -f setgeminikey.sh ] && source ./setgeminikey.sh
uv run python src/temporal_supervisor/run_worker.py
