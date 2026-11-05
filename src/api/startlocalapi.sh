#!/bin/bash
# Start the FastAPI backend locally on port 8000.
cd "$(dirname "$0")/../.." || exit 1
[ -f setgeminikey.sh ] && source ./setgeminikey.sh
[ -f setclaimcheck.sh ] && source ./setclaimcheck.sh
uv run uvicorn api.main:app --reload --app-dir src --host 127.0.0.1 --port 8000
