"""
Calls every MCP tool the way Claude would, against the CSV files in ingest/
instead of Black Forest, and prints a short summary of each result.

Run from the demo folder:  python tests/test_mcp_tools.py
"""

import asyncio
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from fastmcp import Client  # noqa: E402

import mcp_server  # noqa: E402
from test_detection import fake_sql  # noqa: E402

mcp_server.bf.run_sql = fake_sql

CALLS = [
    ("get_system_status", {}),
    ("list_cases", {}),
    ("list_alerts", {}),
    ("get_case", {"case_id": "CCC-2026-0142"}),
    ("get_episode_timeline", {"case_id": "CCC-2026-0142", "episode": "E4"}),
    ("get_calls", {"case_id": "CCC-2026-0142"}),
    ("get_network_as_of", {"case_id": "CCC-2026-0142", "date": "2026-09-15"}),
    ("find_orders", {"start": "2026-10-07T11:02:28", "end": "2026-10-07T11:02:35", "account_id": "ACC-2004"}),
    ("find_calls", {"party_id": "T104"}),
    ("get_transcript", {"comm_id": "CM-0004"}),
    ("links_for", {"entity_id": "KPL", "date": "2026-09-15"}),
    ("describe_datasets", {}),
    ("query_black_forest", {"sql": "SELECT comm_id, from_party FROM demoCatalog.demoNamespace.commsRefDataTable WHERE from_party = 'T104'"}),
    ("query_black_forest", {"sql": "DELETE FROM demoCatalog.demoNamespace.accountsDataTable"}),
]


async def main():
    failures = 0
    async with Client(mcp_server.mcp) as client:
        tools = await client.list_tools()
        print(f"{len(tools)} tools: {', '.join(t.name for t in tools)}\n")
        for name, args in CALLS:
            try:
                result = await client.call_tool(name, args)
                data = result.data if result.data is not None else json.loads(result.content[0].text)
                text = json.dumps(data, default=str)
                print(f"OK   {name} {args or ''}\n     {text[:260]}\n")
            except Exception as error:
                failures += 1
                print(f"FAIL {name} {args}: {error}\n")
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    asyncio.run(main())
