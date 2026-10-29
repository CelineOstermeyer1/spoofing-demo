"""
Black Forest spoofing investigation MCP server.

Exposes the investigation dashboard's Black Forest backed logic to Claude as
MCP tools, so an analyst can investigate the cases by asking Claude. It
imports server.py directly, so Claude gets exactly the same detection, cases
and Black Forest queries as the dashboard.

Uses the same BF_HOST / BF_USER / BF_PASS environment variables as server.py.

  Local (Claude Desktop / Claude Code, stdio):   python mcp_server.py
  Network (streamable HTTP on :8002/mcp):         python mcp_server.py --http
"""

import argparse
import re
import sys
import threading
from datetime import datetime, timedelta, timezone

from fastmcp import FastMCP

# Under the stdio transport stdout carries the MCP protocol, so anything
# server.py prints, at import or later, has to go to stderr instead.
_stdout, sys.stdout = sys.stdout, sys.stderr
import server as bf  # noqa: E402
sys.stdout = _stdout
bf.print = lambda *args, **kwargs: print(*args, **{**kwargs, "file": sys.stderr})

mcp = FastMCP(
    "Black Forest Spoofing Investigations",
    instructions=(
        "Tools for investigating cross-product, cross-actor spoofing, backed by Craxel's "
        "Black Forest platform. All people, firms, instruments, prices and calls are "
        "synthetic. The detection checks every large stock fill for layered, cancelled "
        "futures orders in a linked future, placed by a linked account, in the seconds "
        "before. Start with list_cases or list_alerts, then get_case for the full "
        "picture. Relationships are point in time: use get_network_as_of to see links "
        "as they stood on a given date. Every tool reports the Black Forest queries it "
        "ran and how long they took. Present findings as decision support: the analyst "
        "makes the decision."
    ),
)

RUN_LOCK = threading.Lock()


def _run(refresh=False):
    """The detection run, shared by every tool. Runs once, on first use."""
    with RUN_LOCK:
        if refresh or bf.STATE["run"] is None:
            bf.STATE.update(status="running", error=None)
            bf.STATE.update(status="ready", run=bf.Investigation().run())
        return bf.STATE["run"]


def _case(case_id):
    cases = _run().cases
    if case_id not in cases:
        raise ValueError(f"No case {case_id}. Known cases: {', '.join(cases)}")
    return cases[case_id]


def _plain(value):
    """Datetimes become ISO strings so every tool result is plain JSON."""
    if isinstance(value, datetime):
        return bf.iso(value)
    if isinstance(value, dict):
        return {k: _plain(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_plain(v) for v in value]
    return value


def _queries(log):
    return {"queries": len(log.entries), "black_forest_ms": log.ms()}


def _parse_time(value, name):
    try:
        return bf.parse_ts(value)
    except ValueError:
        raise ValueError(f"{name} must be a UTC time like 2026-10-07T11:02:30")


# ------------------------------------------------------------
# Overview
# ------------------------------------------------------------


@mcp.tool
def get_system_status() -> dict:
    """Black Forest connection check: every demo table with its row count."""
    return bf.status()


@mcp.tool
def list_alerts() -> dict:
    """
    Every large stock execution the detection examined, with its outcome:
    alert (and which case and episode it belongs to) or cleared, with the reason.
    Also says whether a conventional same-account spoofing check would flag it.
    """
    run = _run()
    return {
        "detection_rules": bf.investigation()["rules"],
        "executions": bf.examined_executions(run),
        "run": {"queries": run.log.count(), "black_forest_ms": run.log.ms(), "generated_at": bf.iso(run.started_at)},
    }


@mcp.tool
def list_cases() -> dict:
    """The cases the detection raised: id, title, priority, episodes, dates and instrument pairs."""
    _run()
    return {"cases": bf.investigation()["cases"]}


@mcp.tool
def get_case(case_id: str) -> dict:
    """
    The full case: summary, headline numbers, key findings, every episode
    (pressure orders, sale, timing, price lift, estimated advantage), the
    people and ownership path, what conventional checks would see, and the
    steps the detection took. Case ids look like CCC-2026-0142.
    """
    case = _case(case_id)
    episodes = [{k: v for k, v in e.items() if k != "chart"} for e in case["episodes"]]
    return {
        **{k: case[k] for k in ("id", "title", "kind", "priority", "status", "date_range", "pairs",
                                "connected_party", "trader", "counterparty", "pressure_account",
                                "sale_account", "summary", "stats", "findings", "baseline", "trace")},
        "episodes": episodes,
        "graph_edges": case["graph"]["edges"],
        "calls": [
            {k: c[k] for k in ("id", "evidence", "ts", "channel", "duration_s", "episode", "before_s", "note")}
            for c in case["calls"]
        ],
        "logins": case["logins"],
    }


@mcp.tool
def get_episode_timeline(case_id: str, episode: str = "E1") -> dict:
    """
    Second-by-second view of one episode (E1, E2 ...): every order event and
    best-bid snapshot from 2 s before the first pressure order to 5 s after,
    queried live from Black Forest.
    """
    case = _case(case_id)
    match = next((e for e in case["episodes"] if e["id"].lower() == episode.lower()), None)
    if match is None:
        raise ValueError(f"{case_id} has episodes {', '.join(e['id'] for e in case['episodes'])}")
    start = bf.parse_ts(match["start"])
    log = bf.QueryLog()
    instruments = {match["pressure"]["future"], match["sale"]["stock"]}
    orders = [
        o for o in bf.orders_between(log, "ask", start - bf.CHART_BEFORE, start + bf.CHART_AFTER)
        if o["instrument"] in instruments
    ]
    quotes = [
        q for q in bf.quotes_between(log, start - bf.CHART_BEFORE, start + bf.CHART_AFTER)
        if q["instrument"] in instruments
    ]
    return _plain({
        "episode": {k: v for k, v in match.items() if k != "chart"},
        "orders": sorted(orders, key=lambda o: o["ts"]),
        "best_bids": sorted(quotes, key=lambda q: q["ts"]),
        **_queries(log),
    })


@mcp.tool
def get_calls(case_id: str) -> dict:
    """Recorded calls between the case's parties before each episode, with full transcripts."""
    case = _case(case_id)
    return {
        "calls": [
            {**{k: c[k] for k in ("id", "evidence", "ts", "channel", "duration_s", "episode", "before_s", "note")},
             "transcript": [{"speaker": l["speaker"], "text": l["text"], "confidence": l["confidence"]} for l in c["lines"]]}
            for c in case["calls"]
        ],
        "note": "Speech to text: verify against the audio before relying on a quote.",
    }


@mcp.tool
def get_network_as_of(case_id: str, date: str) -> dict:
    """
    The case's people, accounts and instruments and every link between them,
    as it stood on a date (YYYY-MM-DD). Black Forest is queried live; links
    not yet valid that day are marked. Use it to show point-in-time effects,
    e.g. KPL only joining the Albion 100 index on 21 Sep 2026.
    """
    try:
        day = datetime.fromisoformat(date).date()
    except ValueError:
        raise ValueError("date must look like 2026-10-14")
    graph = bf.graph_as_of(_run(), _case(case_id), day)
    return {
        "as_of": graph["as_of"],
        "nodes": graph["nodes"],
        "links": graph["links"],
        "queries": len(graph["queries"]),
        "black_forest_ms": graph["ms"],
    }


# ------------------------------------------------------------
# Open-ended lookups
# ------------------------------------------------------------


@mcp.tool
def find_orders(start: str, end: str, account_id: str = "", instrument_id: str = "", event_type: str = "") -> dict:
    """
    Order events (NEW, FILL, PARTIAL_FILL, CANCEL) between two UTC times,
    optionally for one account, instrument or event type. The window may be
    at most 2 days. Times look like 2026-10-07T11:02:28.
    """
    first, last = _parse_time(start, "start"), _parse_time(end, "end")
    if last < first or last - first > timedelta(days=2):
        raise ValueError("end must be after start and at most 2 days later")
    log = bf.QueryLog()
    orders = [
        o for o in bf.orders_between(log, "ask", first, last)
        if (not account_id or o["account"] == account_id)
        and (not instrument_id or o["instrument"] == instrument_id)
        and (not event_type or o["type"] == event_type.upper())
    ]
    return _plain({"count": len(orders), "orders": sorted(orders, key=lambda o: o["ts"])[:500], **_queries(log)})


@mcp.tool
def find_calls(party_id: str, start: str = "2026-09-01T00:00:00", end: str = "2026-12-31T23:59:59") -> dict:
    """Recorded calls and chats made by one employee (e.g. T104) between two UTC times."""
    log = bf.QueryLog()
    calls = bf.calls_from(log, party_id, _parse_time(start, "start"), _parse_time(end, "end"))
    return _plain({"count": len(calls), "calls": sorted(calls, key=lambda c: c["ts"]), **_queries(log)})


@mcp.tool
def get_transcript(comm_id: str) -> dict:
    """The transcript of one call or chat, by its id (e.g. CM-0001)."""
    log = bf.QueryLog()
    return {"comm_id": comm_id, "lines": bf.transcript(log, comm_id), **_queries(log)}


@mcp.tool
def links_for(entity_id: str, date: str) -> dict:
    """
    Every people, ownership, instrument or inferred link touching one person,
    firm, account or instrument (e.g. T104, BO551, ACC-3107, KPL), valid on a
    date (YYYY-MM-DD), with source, provenance and confidence.
    """
    day = datetime.fromisoformat(date).date()
    log = bf.QueryLog()
    links = bf.actor_links_on(log, day) + bf.instrument_links_on(log, day)
    touching = [l for l in links if entity_id in (l["from"], l["to"])]
    return _plain({"as_of": date, "entity": entity_id, "links": touching, **_queries(log)})


DATASETS = {
    "tables": {label: name for label, name in bf.TABLES},
    "notes": [
        "Timestamps are UTC. In SQL, write them as 'yyyy-MM-dd HH:mm:ss.SSS'.",
        "Relationship tables hold valid_from and valid_to; blank end dates were loaded as 2099-12-31.",
        "Filtering on event_id or log_id is unreliable in Black Forest; filter on time windows or "
        "ordinary columns such as account_id instead.",
        "Columns follow the demo specification: order events have order_id, timestamp_utc, "
        "instrument_id, account_id, trader_id, side, limit_price, order_qty, event_type, fill_qty, "
        "fill_price; communications have comm_id, timestamp_utc, from_party, to_party, to_party_id.",
    ],
}
ALLOWED_TABLES = {name.lower() for _, name in bf.TABLES}
WRITE_WORDS = re.compile(r"(?i)\b(insert|update|delete|drop|alter|create|truncate|merge|grant|revoke)\b")


@mcp.tool
def describe_datasets() -> dict:
    """The Black Forest tables behind the investigation and notes on querying them."""
    return DATASETS


@mcp.tool
def query_black_forest(sql: str, limit: int = 100) -> dict:
    """
    Run a read-only SELECT against the investigation's Black Forest tables
    (see describe_datasets). A LIMIT is added if missing (max 500).
    Use for questions the other tools don't cover.
    """
    statement = sql.strip().rstrip(";").strip()

    if ";" in statement or not re.match(r"(?is)^select\b", statement):
        return {"error": "Only a single SELECT statement is allowed"}
    if WRITE_WORDS.search(statement):
        return {"error": "Write or DDL keywords are not allowed"}

    tables = {t.lower() for t in re.findall(r"\b(\w+\.\w+\.\w+)\b", statement)}
    if not tables or not tables <= ALLOWED_TABLES:
        return {"error": "Query must only reference tables listed by describe_datasets"}

    if not re.search(r"(?i)\blimit\s+\d+\s*$", statement):
        statement += f" LIMIT {max(1, min(limit, 500))}"

    log = bf.QueryLog()
    rows = log.run("ask", statement)
    return {"sql": statement, "count": len(rows), "rows": rows, **_queries(log)}


@mcp.tool
def rerun_detection() -> dict:
    """Run the whole detection again against Black Forest and report how long it took."""
    run = _run(refresh=True)
    return {"cases": len(run.cases), "alerts": len(run.alerts), "queries": run.log.count(),
            "black_forest_ms": run.log.ms(), "generated_at": bf.iso(run.started_at)}


# ------------------------------------------------------------
# Startup
# ------------------------------------------------------------


def _warm_up():
    # Run the detection up front so Claude's first question doesn't wait for it.
    try:
        _run()
        print("Detection run complete", file=sys.stderr)
    except Exception as error:
        print(f"Detection warm-up failed: {error}", file=sys.stderr)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--http", action="store_true", help="Serve streamable HTTP instead of stdio")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8002)
    args = parser.parse_args()

    threading.Thread(target=_warm_up, daemon=True).start()

    if args.http:
        mcp.run(transport="http", host=args.host, port=args.port, path="/mcp")
    else:
        mcp.run()
