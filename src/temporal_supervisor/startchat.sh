#!/bin/bash
# Start the interactive CLI chat client.
cd "$(dirname "$0")/../.." || exit 1
[ -f setgeminikey.sh ] && source ./setgeminikey.sh
uv run python src/temporal_supervisor/run_chat.py
