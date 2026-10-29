#!/usr/bin/env bash
# Starts the spoofing investigation demo. Reads BF_HOST, BF_USER and BF_PASS from .env.
set -euo pipefail
cd "$(dirname "$0")"

if [ ! -f .env ]; then
  echo "Missing .env. Copy .env.example to .env and fill in BF_HOST, BF_USER and BF_PASS." >&2
  exit 1
fi
set -a; source .env; set +a

if [ ! -d .venv ]; then
  python3 -m venv .venv --prompt spoofing
  .venv/bin/pip install -r requirements.txt
fi

exec .venv/bin/uvicorn server:app --host 0.0.0.0 --port "${PORT:-8000}"
