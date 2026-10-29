"""
Serves the dashboard from the CSV files in ingest/ instead of Black Forest.

For checking the page layout without Black Forest credentials. Query times
shown on the page are local CSV reads, not Black Forest timings, so never
use this for the demo itself.

Run from the demo folder:  python tests/serve_from_csv.py
"""

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import uvicorn  # noqa: E402

import server  # noqa: E402
from test_detection import fake_sql  # noqa: E402

server.run_sql = fake_sql
server.SAVED_RUN = None  # never mix CSV runs with saved Black Forest runs

if __name__ == "__main__":
    uvicorn.run(server.app, host="127.0.0.1", port=int(os.environ.get("PORT", "8010")))
