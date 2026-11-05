#!/bin/bash
# Start the claim-check codec server on http://127.0.0.1:8081, so the Temporal UI
# can show the payloads stored in Redis. Reads REDIS_HOST / REDIS_PORT (from
# setclaimcheck.sh in the project root, if it exists).
cd "$(dirname "$0")/../.." || exit 1
echo "****************************************"
echo "* Remember to start your Redis Server! *"
echo "****************************************"
[ -f setclaimcheck.sh ] && source ./setclaimcheck.sh
uv run python src/temporal_supervisor/codec_server/codec_server.py
