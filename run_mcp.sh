#!/usr/bin/env bash
# Starts the investigation MCP server for Claude Desktop or Claude Code.
# Reads BF_HOST, BF_USER and BF_PASS from .env. Under stdio, stdout carries
# the MCP protocol, so everything else here writes to stderr.
set -euo pipefail
cd "$(dirname "$0")"

if [ ! -f .env ]; then
  echo "Missing .env. Copy .env.example to .env and fill in BF_HOST, BF_USER and BF_PASS." >&2
  exit 1
fi
set -a; source .env; set +a

if [ ! -d .venv ]; then
  python3 -m venv .venv --prompt spoofing >&2
fi
if ! .venv/bin/python -c "import fastmcp, fastapi, requests" 2>/dev/null; then
  .venv/bin/pip install -r requirements.txt >&2
fi

exec .venv/bin/python mcp_server.py "$@"
