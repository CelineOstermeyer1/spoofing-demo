"""
Black Forest cross-product spoofing demo: investigation workspace server.

Runs the detection logic from the demo specification against the ten demo
tables in Black Forest and serves the investigation screen (index.html).
Every figure on the screen comes from a Black Forest query, and every query
is timed, so the agent trace shows what was asked and how long it took.

Reads BF_HOST, BF_USER and BF_PASS from the environment (see start.sh).

Black Forest notes that shape the SQL below:
  - Timestamp literals use 'yyyy-MM-dd HH:mm:ss.SSS'.
  - Filtering on a text field that is part of an event table's primary key
    returns no rows or only some of them, so lookups use time windows and
    ordinary columns instead (event_id and log_id are never filtered on).
"""

import math
import pickle
import os
import re
import sys
import threading
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests
import urllib3
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

urllib3.disable_warnings()

HERE = Path(__file__).parent

app = FastAPI(title="Black Forest Spoofing Investigation Demo")
app.mount("/static", StaticFiles(directory=HERE / "static"), name="static")


# ------------------------------------------------------------
# Black Forest configuration
# ------------------------------------------------------------

BF_HOST = os.environ.get("BF_HOST", "")
BF_USER = os.environ.get("BF_USER", "")
BF_PASS = os.environ.get("BF_PASS", "")

# Each table can be overridden with an environment variable, e.g.
#   export ORDERS_TABLE=myOrderTable
# A bare table name is assumed to live in demoCatalog.demoNamespace.
BF_NAMESPACE = os.environ.get("BF_NAMESPACE", "demoCatalog.demoNamespace")


def _table(env_name, default):
    name = os.environ.get(env_name, default)
    return name if "." in name else f"{BF_NAMESPACE}.{name}"


ORDERS_TABLE = _table("ORDERS_TABLE", "orderEventsDataTable")
MARKET_TABLE = _table("MARKET_TABLE", "marketDataTable")
INSTRUMENT_LINKS_TABLE = _table("INSTRUMENT_LINKS_TABLE", "instrumentRelationshipsDataTable")
ACTOR_LINKS_TABLE = _table("ACTOR_LINKS_TABLE", "actorRelationshipsDataTable")
INSTRUMENTS_TABLE = _table("INSTRUMENTS_TABLE", "instrumentsDataTable")
ACTORS_TABLE = _table("ACTORS_TABLE", "actorsDataTable")
ACCOUNTS_TABLE = _table("ACCOUNTS_TABLE", "accountsDataTable")
COMMS_TABLE = _table("COMMS_TABLE", "commsRefDataTable")
TRANSCRIPTS_TABLE = _table("TRANSCRIPTS_TABLE", "transcriptsDataTable")
LOGINS_TABLE = _table("LOGINS_TABLE", "accessLogsDataTable")

TABLES = [
    ("Order events", ORDERS_TABLE),
    ("Market data", MARKET_TABLE),
    ("Instrument links", INSTRUMENT_LINKS_TABLE),
    ("People and account links", ACTOR_LINKS_TABLE),
    ("Instruments", INSTRUMENTS_TABLE),
    ("People and firms", ACTORS_TABLE),
    ("Accounts", ACCOUNTS_TABLE),
    ("Communications", COMMS_TABLE),
    ("Transcripts", TRANSCRIPTS_TABLE),
    ("DMA logins", LOGINS_TABLE),
]

print("Black Forest tables in use:")
for _label, _name in TABLES:
    print(f"  {_label:<26}{_name}")


# ------------------------------------------------------------
# Detection settings (from the demo specification)
# ------------------------------------------------------------
# These separate the synthetic dataset cleanly. They are not calibrated for
# production surveillance.

MIN_EXECUTION_QTY = 25_000        # equity fills worth investigating
LOOKBACK = timedelta(seconds=5)   # pressure orders placed before the sale
LOOKAHEAD = timedelta(seconds=3)  # cancels and price reversion after it
SIZE_MULTIPLE = 5                 # order size vs the account's 95th percentile
MIN_LAYERS = 3                    # same-side orders...
LAYER_WINDOW = timedelta(milliseconds=500)  # ...placed within this window
CANCEL_WITHIN = timedelta(seconds=1)        # all layers cancelled after the sale
MIN_CROSS_IMPACT = 0.7            # inferred future-to-stock lead-lag edges
MAX_HOPS = 4                      # people and ownership links, account to account
CALL_LOOKBACK = timedelta(minutes=10)
LOGIN_LOOKBACK = timedelta(minutes=5)
CHART_BEFORE = timedelta(seconds=2)
CHART_AFTER = timedelta(seconds=5)

# Links that describe who people are, not what they did. Calls and inferred
# trading patterns are kept as supporting evidence rather than used to join.
TRAVERSAL_EXCLUDED = {"COMMUNICATES_WITH", "COMPLEMENTARY_TRADING"}

# Phrases highlighted in transcripts. Keyword rules only: a reviewer should
# always check the audio.
CUE_PHRASES = [
    "order ready",
    "watch the bid",
    "firm up",
    "don't hang about",
    "same as",
    "in the index",
    "when you see it lift",
    "couple of minutes",
]


# ------------------------------------------------------------
# Black Forest access
# ------------------------------------------------------------


RETRY_DELAYS = (1, 3)  # seconds before the second and third attempt


# One connection reused for every query: opening a new HTTPS connection for each
# query cost more than Black Forest's own work on it.
BF_SESSION = requests.Session()


def bf_query(sql, timeout=60):
    if not (BF_HOST and BF_USER and BF_PASS):
        raise RuntimeError("BF_HOST, BF_USER and BF_PASS must be set (see .env.example)")

    # Black Forest sometimes answers a burst of queries with HTTP 5xx
    # ("Failed to connect to database"); a short pause and retry recovers.
    for delay in RETRY_DELAYS + (None,):
        response = BF_SESSION.post(
            f"{BF_HOST}/query-ui/services/query/credentialedSql",
            json={
                "query": sql,
                "username": BF_USER,
                "password": BF_PASS,
            },
            verify=False,
            timeout=timeout,
        )
        if response.status_code < 500 or delay is None:
            break
        time.sleep(delay)

    if not response.ok:
        table_match = re.search(r"FROM\s+([\w.]+)", sql, re.IGNORECASE)
        table_name = table_match.group(1) if table_match else "unknown table"
        raise RuntimeError(
            f"Black Forest returned HTTP {response.status_code} "
            f"for {table_name}: {response.text[:500]}"
        )

    data = response.json()

    if not data.get("success"):
        raise RuntimeError(
            data.get("errorMessage", "Black Forest query failed")
        )

    columns = [
        c["columnName"]
        for c in data.get("columns", [])
    ]

    return [
        dict(zip(columns, row["values"]))
        for row in data.get("rows", [])
    ]


# Swapped out by tests/test_detection.py to run against the CSV files.
run_sql = bf_query

# One query at a time, so the status check and a detection run never
# overlap. The timer starts after the wait, so it measures Black Forest only.
QUERY_LOCK = threading.Lock()


class QueryLog:
    """Runs and times every query, tagged with the agent step it belongs to."""

    def __init__(self):
        self.entries = []

    def run(self, step, sql):
        with QUERY_LOCK:
            started = time.perf_counter()
            rows = run_sql(sql)
        self.entries.append({
            "step": step,
            "sql": " ".join(sql.split()),
            "ms": round((time.perf_counter() - started) * 1000, 1),
            "rows": len(rows),
        })
        # Progress in the terminal, so a long run shows where the time goes.
        entry = self.entries[-1]
        print(f"  query {len(self.entries):>3}  {step:<11} {entry['ms']:>9,.0f} ms  {entry['rows']:>7,} rows",
              file=sys.stderr, flush=True)
        return rows

    def ms(self, step=None):
        return round(sum(e["ms"] for e in self.entries if step in (None, e["step"])), 1)

    def count(self, step=None):
        return sum(1 for e in self.entries if step in (None, e["step"]))


# ------------------------------------------------------------
# Value helpers
# ------------------------------------------------------------


def parse_ts(value):
    """Black Forest may return timestamps as text or epoch numbers."""
    if value in (None, ""):
        return None
    if isinstance(value, (int, float)) or str(value).strip().isdigit():
        number = float(value)
        return datetime.fromtimestamp(number / 1000 if number > 1e11 else number, tz=timezone.utc)
    text = str(value).strip().replace("T", " ").rstrip("Z")
    parsed = datetime.fromisoformat(text)
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def sql_ts(moment):
    return moment.strftime("%Y-%m-%d %H:%M:%S.") + f"{moment.microsecond // 1000:03d}"


def iso(moment):
    return None if moment is None else moment.strftime("%Y-%m-%dT%H:%M:%S.") + f"{moment.microsecond // 1000:03d}Z"


def lit(value):
    return "'" + str(value).replace("'", "''") + "'"


def num(value, default=0.0):
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def day_bounds(day):
    start = datetime(day.year, day.month, day.day, tzinfo=timezone.utc)
    return start, start + timedelta(days=1) - timedelta(milliseconds=1)


def percentile(values, share):
    if not values:
        return 0
    ordered = sorted(values)
    return ordered[max(0, math.ceil(share * len(ordered)) - 1)]


def seconds(delta):
    return round(delta.total_seconds(), 3)


def short_date(moment):
    return f"{moment.day} {moment:%b}"


# ------------------------------------------------------------
# Queries
# ------------------------------------------------------------

ORDER_COLUMNS = (
    "event_id, order_id, timestamp_utc, instrument_id, account_id, trader_id, "
    "side, limit_price, order_qty, event_type, fill_qty, fill_price"
)
LINK_COLUMNS = "rel_id, rel_type, valid_from, valid_to, provenance, source, confidence, notes"


def _order(row):
    return {
        "event_id": row.get("event_id"),
        "order_id": row.get("order_id"),
        "ts": parse_ts(row.get("timestamp_utc")),
        "instrument": row.get("instrument_id"),
        "account": row.get("account_id"),
        "trader": row.get("trader_id") or None,
        "side": row.get("side"),
        "price": num(row.get("limit_price")),
        "qty": int(num(row.get("order_qty"))),
        "type": row.get("event_type"),
        "fill_qty": int(num(row.get("fill_qty"))),
        "fill_price": num(row.get("fill_price"), None),
    }


def _link(row, source_key, target_key):
    return {
        "id": row.get("rel_id"),
        "from": row.get(source_key),
        "to": row.get(target_key),
        "type": row.get("rel_type"),
        "weight": num(row.get("weight"), None),
        "valid_from": parse_ts(row.get("valid_from")),
        "valid_to": parse_ts(row.get("valid_to")),
        "provenance": row.get("provenance"),
        "source": row.get("source"),
        "confidence": num(row.get("confidence"), None),
        "notes": row.get("notes") or "",
    }


def as_of_clause(day):
    start, end = day_bounds(day)
    return f"valid_from <= {lit(sql_ts(end))} AND valid_to >= {lit(sql_ts(start))}"


class Reference:
    """Instruments, people and accounts, loaded once per run."""

    def __init__(self, log):
        self.instruments = {
            r["instrument_id"]: {
                "id": r["instrument_id"],
                "name": r.get("name"),
                "asset_class": r.get("asset_class"),
                "price_unit": r.get("price_unit"),
                "tick": num(r.get("tick_size"), 1.0) or 1.0,
                "underlying": r.get("underlying_id") or None,
            }
            for r in log.run("reference", f"""
                SELECT instrument_id, name, asset_class, price_unit, tick_size, underlying_id
                FROM {INSTRUMENTS_TABLE}
            """)
        }
        self.actors = {
            r["actor_id"]: {
                "id": r["actor_id"],
                "name": r.get("name"),
                "type": r.get("actor_type"),
                "desk": r.get("desk") or "",
                "role": r.get("role") or "",
            }
            for r in log.run("reference", f"""
                SELECT actor_id, name, actor_type, desk, role
                FROM {ACTORS_TABLE}
            """)
        }
        self.accounts = {
            r["account_id"]: {
                "id": r["account_id"],
                "name": r.get("account_name"),
                "type": r.get("account_type"),
                "trader": r.get("responsible_trader") or None,
                "client": r.get("client_id") or None,
            }
            for r in log.run("reference", f"""
                SELECT account_id, account_name, account_type, responsible_trader, client_id
                FROM {ACCOUNTS_TABLE}
            """)
        }

    def asset_class(self, instrument):
        return self.instruments.get(instrument, {}).get("asset_class")

    def tick(self, instrument):
        return self.instruments.get(instrument, {}).get("tick", 1.0)

    def name(self, node):
        if node in self.actors:
            return self.actors[node]["name"]
        if node in self.accounts:
            return self.accounts[node]["name"]
        if node in self.instruments:
            return self.instruments[node]["name"]
        return node

    def owner(self, account):
        """The trader or client behind an account."""
        info = self.accounts.get(account, {})
        return info.get("trader") or info.get("client")


def instrument_links_on(log, day):
    return [
        _link(r, "from_instrument", "to_instrument")
        for r in log.run("instruments", f"""
            SELECT from_instrument, to_instrument, weight, {LINK_COLUMNS}
            FROM {INSTRUMENT_LINKS_TABLE}
            WHERE {as_of_clause(day)}
        """)
    ]


def actor_links_on(log, day):
    return [
        _link(r, "from_actor", "to_actor")
        for r in log.run("people", f"""
            SELECT from_actor, to_actor, {LINK_COLUMNS}
            FROM {ACTOR_LINKS_TABLE}
            WHERE {as_of_clause(day)}
        """)
    ]


def orders_between(log, step, start, end):
    return [
        _order(r)
        for r in log.run(step, f"""
            SELECT {ORDER_COLUMNS}
            FROM {ORDERS_TABLE}
            WHERE timestamp_utc >= {lit(sql_ts(start))}
              AND timestamp_utc <= {lit(sql_ts(end))}
        """)
    ]


def account_activity(log, account):
    return [
        _order(r)
        for r in log.run("history", f"""
            SELECT {ORDER_COLUMNS}
            FROM {ORDERS_TABLE}
            WHERE account_id = {lit(account)}
        """)
    ]


def quotes_between(log, start, end):
    return [
        {
            "ts": parse_ts(r.get("timestamp_utc")),
            "instrument": r.get("instrument_id"),
            "bid": num(r.get("best_bid"), None),
            "ask": num(r.get("best_ask"), None),
        }
        for r in log.run("prices", f"""
            SELECT timestamp_utc, instrument_id, best_bid, best_ask
            FROM {MARKET_TABLE}
            WHERE timestamp_utc >= {lit(sql_ts(start))}
              AND timestamp_utc <= {lit(sql_ts(end))}
        """)
    ]


def calls_from(log, party, start, end):
    return [
        {
            "id": r.get("comm_id"),
            "ts": parse_ts(r.get("timestamp_utc")),
            "channel": r.get("channel"),
            "from": r.get("from_party"),
            "to": r.get("to_party"),
            "to_id": r.get("to_party_id") or None,
            "duration_s": int(num(r.get("duration_s"))),
        }
        for r in log.run("comms", f"""
            SELECT comm_id, timestamp_utc, channel, from_party, to_party, to_party_id, duration_s
            FROM {COMMS_TABLE}
            WHERE from_party = {lit(party)}
              AND timestamp_utc >= {lit(sql_ts(start))}
              AND timestamp_utc <= {lit(sql_ts(end))}
        """)
    ]


def transcript(log, comm_id):
    rows = log.run("comms", f"""
        SELECT comm_id, evidence_ref, line_no, offset_s, speaker_id, speaker_name, text, stt_confidence
        FROM {TRANSCRIPTS_TABLE}
        WHERE comm_id = {lit(comm_id)}
    """)
    return sorted(
        (
            {
                "line": int(num(r.get("line_no"))),
                "evidence": r.get("evidence_ref") or None,
                "speaker_id": r.get("speaker_id"),
                "speaker": r.get("speaker_name"),
                "text": r.get("text") or "",
                "confidence": num(r.get("stt_confidence"), None),
            }
            for r in rows
        ),
        key=lambda line: line["line"],
    )


def logins_between(log, account, start, end):
    return [
        {"id": r.get("log_id"), "ts": parse_ts(r.get("timestamp_utc")), "source_ip": r.get("source_ip")}
        for r in log.run("comms", f"""
            SELECT log_id, timestamp_utc, account_id, source_ip
            FROM {LOGINS_TABLE}
            WHERE account_id = {lit(account)}
              AND timestamp_utc >= {lit(sql_ts(start))}
              AND timestamp_utc <= {lit(sql_ts(end))}
        """)
    ]


# ------------------------------------------------------------
# Detection
# ------------------------------------------------------------


def related_futures(stock, links, ref):
    """Futures linked to a stock on the trade date, with the route that links them."""
    routes = {}

    for link in links:
        if link["type"] == "UNDERLYING" and link["to"] == stock and ref.asset_class(link["from"]) == "Future":
            routes.setdefault(link["from"], [link])

    for constituent in links:
        if constituent["type"] != "CONSTITUENT_OF" or constituent["from"] != stock:
            continue
        for underlying in links:
            if underlying["type"] == "UNDERLYING" and underlying["to"] == constituent["to"]:
                routes.setdefault(underlying["from"], [constituent, underlying])

    for link in links:
        if (
            link["type"] == "CROSS_IMPACT"
            and link["to"] == stock
            and (link["confidence"] or 0) >= MIN_CROSS_IMPACT
        ):
            routes.setdefault(link["from"], [link])

    return routes


def linked_accounts(start_account, links, ref):
    """Accounts reachable from start_account through people and ownership links."""
    neighbours = defaultdict(list)
    for link in links:
        if link["type"] in TRAVERSAL_EXCLUDED:
            continue
        neighbours[link["from"]].append((link["to"], link))
        neighbours[link["to"]].append((link["from"], link))

    # Each path is the list of (node, link used to reach it), from start_account.
    paths = {start_account: [(start_account, None)]}
    frontier = [start_account]
    for _ in range(MAX_HOPS):
        next_frontier = []
        for node in frontier:
            for other, link in neighbours[node]:
                if other not in paths:
                    paths[other] = paths[node] + [(other, link)]
                    next_frontier.append(other)
        frontier = next_frontier

    return {node: path for node, path in paths.items() if node in ref.accounts}


def path_links(path):
    return [link for _, link in path if link]


def path_chain(path, ref):
    """Reads the path from the pressure side: Jordan Kerr → shared address → Riley Kerr → ..."""
    label = lambda node: node if node in ref.accounts else ref.name(node)
    steps = list(reversed(path))
    # path[i] holds the link used to reach node i, so after reversing, the
    # link between steps[i] and steps[i + 1] sits on steps[i].
    hops = [(steps[i][1], steps[i + 1][0]) for i in range(len(steps) - 1)]
    if hops and hops[0][0]["type"] == "TRADES_FOR":
        start, hops = hops[0][1], hops[1:]
    else:
        start = steps[0][0]
    parts = [label(start)]
    for link, node in hops:
        parts += [link["type"].replace("_", " ").lower(), label(node)]
    return " → ".join(parts)


def human_join(items):
    items = list(items)
    return items[0] if len(items) == 1 else ", ".join(items[:-1]) + " and " + items[-1]


def spoof_signatures(window, sale, ref, normal_size):
    """Accounts whose futures orders before the sale look like spoofing pressure."""
    pressure_side = "BUY" if sale["side"] == "SELL" else "SELL"
    news = defaultdict(list)
    for event in window:
        if (
            event["type"] == "NEW"
            and event["side"] == pressure_side
            and ref.asset_class(event["instrument"]) == "Future"
            and sale["ts"] - LOOKBACK <= event["ts"] <= sale["ts"]
        ):
            news[(event["account"], event["instrument"])].append(event)

    found, near_misses = [], []
    for (account, future), orders in news.items():
        orders.sort(key=lambda o: o["ts"])
        burst = next(
            (orders[i:] for i in range(len(orders))
             if len([o for o in orders[i:] if o["ts"] - orders[i]["ts"] <= LAYER_WINDOW]) >= MIN_LAYERS),
            None,
        )
        ids = {o["order_id"] for o in (burst or orders)}
        filled = sum(e["fill_qty"] for e in window if e["order_id"] in ids and e["type"] in ("FILL", "PARTIAL_FILL"))
        cancels = [e for e in window if e["order_id"] in ids and e["type"] == "CANCEL"]
        # The account's history is a slow query, so fetch it only once there is a burst to judge.
        normal = normal_size(account) if burst else None
        checks = {
            "layering": burst is not None,
            "size": bool(burst) and all(o["qty"] >= SIZE_MULTIPLE * normal for o in burst),
            "unfilled": filled == 0,
            "cancelled": bool(burst) and len({c["order_id"] for c in cancels}) == len(ids)
            and all(sale["ts"] <= c["ts"] <= sale["ts"] + CANCEL_WITHIN for c in cancels),
        }
        entry = {
            "account": account,
            "future": future,
            "orders": burst or orders,
            "cancels": sorted(cancels, key=lambda c: c["ts"]),
            "filled_qty": filled,
            "normal": normal,
            "checks": checks,
        }
        (found if all(checks.values()) else near_misses).append(entry)

    return found, near_misses


def price_path(quotes, instrument, start, sale_ts, tick):
    """Did the bid lift between the first pressure order and the sale, then revert?"""
    series = sorted((q for q in quotes if q["instrument"] == instrument and q["bid"] is not None), key=lambda q: q["ts"])
    before = [q for q in series if q["ts"] <= start]
    if not series or not before:
        return {"base": None, "peak": None, "lift_ticks": 0, "reverted": False}
    base = before[-1]["bid"]
    during = [q["bid"] for q in series if start < q["ts"] <= sale_ts] or [base]
    peak = max(during)
    after = [q["bid"] for q in series if sale_ts < q["ts"] <= sale_ts + LOOKAHEAD]
    return {
        "base": base,
        "peak": peak,
        "lift_ticks": round((peak - base) / tick, 2),
        "reverted": any(bid <= base for bid in after),
    }


class Investigation:
    """One full detection run: every query, alert, case and control outcome."""

    def __init__(self):
        self.log = QueryLog()
        self.started_at = datetime.now(timezone.utc)
        self.history = {}
        self.instrument_links = {}
        self.actor_links = {}
        self.alerts = []
        self.outcomes = []

    def links_for(self, day):
        if day not in self.instrument_links:
            self.instrument_links[day] = instrument_links_on(self.log, day)
        return self.instrument_links[day]

    def people_links_for(self, day):
        if day not in self.actor_links:
            self.actor_links[day] = actor_links_on(self.log, day)
        return self.actor_links[day]

    def activity(self, account):
        if account not in self.history:
            self.history[account] = account_activity(self.log, account)
        return self.history[account]

    def normal_size(self, account):
        """95th percentile of the account's futures orders outside every sale window."""
        sizes = [
            o["qty"] for o in self.activity(account)
            if o["type"] == "NEW"
            and self.ref.asset_class(o["instrument"]) == "Future"
            and not any(s["ts"] - LOOKBACK <= o["ts"] <= s["ts"] + LOOKAHEAD for s in self.candidates)
        ]
        return percentile(sizes, 0.95)

    def normal_max(self, account):
        return max(
            (o["qty"] for o in self.activity(account)
             if o["type"] == "NEW"
             and self.ref.asset_class(o["instrument"]) == "Future"
             and not any(s["ts"] - LOOKBACK <= o["ts"] <= s["ts"] + LOOKAHEAD for s in self.candidates)),
            default=0,
        )

    def run(self):
        clock = time.perf_counter()
        self.ref = Reference(self.log)

        # Black Forest does the size filter, so only the large fills come back,
        # however big the order table grows.
        counted = self.log.run("executions", f"SELECT COUNT(*) FROM {ORDERS_TABLE} WHERE event_type = 'FILL'")
        self.total_fills = int(num(next(iter(counted[0].values())))) if counted else 0
        large = [
            _order(r)
            for r in self.log.run("executions", f"""
                SELECT {ORDER_COLUMNS}
                FROM {ORDERS_TABLE}
                WHERE event_type = 'FILL'
                  AND fill_qty >= {MIN_EXECUTION_QTY}
            """)
        ]
        self.candidates = sorted(
            (f for f in large
             if self.ref.asset_class(f["instrument"]) == "Equity" and f["fill_qty"] >= MIN_EXECUTION_QTY),
            key=lambda f: f["ts"],
        )

        for sale in self.candidates:
            self.examine(sale)

        self.cases = build_cases(self)
        self.total_ms = round((time.perf_counter() - clock) * 1000, 1)
        return self

    def examine(self, sale):
        day = sale["ts"].date()
        routes = related_futures(sale["instrument"], self.links_for(day), self.ref)
        window = orders_between(self.log, "search", sale["ts"] - LOOKBACK, sale["ts"] + LOOKAHEAD)
        found, near_misses = spoof_signatures(window, sale, self.ref, self.normal_size)
        outcome = {"sale": sale, "routes": sorted(routes), "found": found, "near_misses": near_misses}
        self.outcomes.append(outcome)

        related = [f for f in found if f["future"] in routes]
        if not found:
            outcome["result"] = "no_pressure"
            return
        if not related:
            outcome["result"] = "no_route"
            return

        links = self.people_links_for(day)
        accounts = linked_accounts(sale["account"], links, self.ref)
        linked = [f for f in related if f["account"] in accounts]
        if not linked:
            outcome["result"] = "no_link"
            return

        pressure = linked[0]
        start = pressure["orders"][0]["ts"]
        quotes = quotes_between(self.log, start - CHART_BEFORE, start + CHART_AFTER)
        stock_path = price_path(quotes, sale["instrument"], start, sale["ts"], self.ref.tick(sale["instrument"]))
        future_path = price_path(quotes, pressure["future"], start, sale["ts"], self.ref.tick(pressure["future"]))
        if stock_path["lift_ticks"] < 1 or not stock_path["reverted"]:
            outcome["result"] = "no_price_effect"
            return

        outcome["result"] = "alert"
        inferred = [
            link for link in links
            if link["type"] == "COMPLEMENTARY_TRADING"
            and {link["from"], link["to"]} == {pressure["account"], sale["account"]}
        ]
        self.alerts.append({
            "sale": sale,
            "pressure": pressure,
            "route": routes[pressure["future"]],
            "path": accounts[pressure["account"]],
            "inferred": inferred,
            "people_links": links,
            "quotes": quotes,
            "stock_path": stock_path,
            "future_path": future_path,
            "other_matches": [f for f in found if f["account"] != pressure["account"]],
            "start": start,
        })


# ------------------------------------------------------------
# Cases: grouping, enrichment and the screen's content
# ------------------------------------------------------------


def build_cases(run):
    groups = defaultdict(list)
    for alert in run.alerts:
        groups[(alert["pressure"]["account"], alert["sale"]["account"])].append(alert)

    ordered = sorted(groups.items(), key=lambda item: item[1][0]["start"])
    cases = []
    for number, ((pressure_account, sale_account), alerts) in enumerate(ordered, start=141):
        cases.append(build_case(run, f"CCC-2026-{number:04d}", pressure_account, sale_account, alerts))
    return {case["id"]: case for case in cases}


def mark_phrases(text, extra_terms):
    lowered = text.lower()
    spans = []
    for phrase in CUE_PHRASES + extra_terms:
        start = lowered.find(phrase.lower())
        while start >= 0:
            spans.append([start, start + len(phrase)])
            start = lowered.find(phrase.lower(), start + 1)
    spans.sort()
    merged = []
    for span in spans:
        if merged and span[0] <= merged[-1][1]:
            merged[-1][1] = max(merged[-1][1], span[1])
        else:
            merged.append(span)
    return merged


def describe_link(link, ref):
    label = link["type"].replace("_", " ").lower()
    if link["confidence"] is not None and link["confidence"] < 1:
        label += f" {link['confidence']:.2f}"
    return label


def describe_route(route, ref):
    parts = []
    for link in route:
        if link["type"] == "CONSTITUENT_OF":
            parts.append(f"{link['from']} in {link['to']} from {short_date(link['valid_from'])}")
        elif link["type"] == "UNDERLYING":
            parts.append(f"{link['from']} on {link['to']}")
        else:
            parts.append(f"{link['from']} leads {link['to']} (inferred {link['confidence']:.2f})")
    return ", ".join(parts)


def chart_for(alert, ref):
    start = alert["start"]
    sale = alert["sale"]
    pressure = alert["pressure"]
    series = []
    for instrument, role in ((pressure["future"], "future"), (sale["instrument"], "stock")):
        path = alert["future_path"] if role == "future" else alert["stock_path"]
        tick = ref.tick(instrument)
        quotes = sorted((q for q in alert["quotes"] if q["instrument"] == instrument and q["bid"] is not None),
                        key=lambda q: q["ts"])
        series.append({
            "instrument": instrument,
            "role": role,
            "venue_name": ref.name(instrument),
            "points": [
                {"t": seconds(q["ts"] - start), "ticks": round((q["bid"] - path["base"]) / tick, 2), "bid": q["bid"]}
                for q in quotes
            ],
        })
    markers = [{"t": seconds(o["ts"] - start), "kind": "pressure", "qty": o["qty"], "price": o["price"]}
               for o in pressure["orders"]]
    markers.append({"t": seconds(sale["ts"] - start), "kind": "sale", "qty": sale["fill_qty"], "price": sale["fill_price"]})
    markers += [{"t": seconds(c["ts"] - start), "kind": "cancel"} for c in pressure["cancels"]]
    end = pressure["cancels"][-1]["ts"] if pressure["cancels"] else sale["ts"]
    return {
        "from": -seconds(CHART_BEFORE),
        "to": seconds(CHART_AFTER),
        "window": [0, seconds(end - start)],
        "series": series,
        "markers": markers,
    }


def build_case(run, case_id, pressure_account, sale_account, alerts):
    ref = run.ref
    log = run.log
    same_account = pressure_account == sale_account
    trader = ref.owner(pressure_account)
    counterparty = ref.owner(sale_account)
    trader_name = ref.name(trader)
    counterparty_name = ref.name(counterparty)
    sale_label = sale_account if same_account else f"{sale_account} ({counterparty_name})"

    episodes = []
    calls = []
    logins = []
    for index, alert in enumerate(alerts, start=1):
        sale, pressure, start = alert["sale"], alert["pressure"], alert["start"]
        label = f"E{index}"
        alert.update(case_id=case_id, episode=label)

        episode_calls = []
        if not same_account and trader:
            for call in calls_from(log, trader, start - CALL_LOOKBACK, start):
                if call["to_id"] != counterparty:
                    continue
                lines = transcript(log, call["id"])
                stocks = [ref.name(sale["instrument"]).split()[0], sale["instrument"]]
                call.update({
                    "episode": label,
                    "before_s": seconds(start - call["ts"]),
                    "evidence": next((l["evidence"] for l in lines if l["evidence"]), call["id"]),
                    "lines": [dict(l, marks=mark_phrases(l["text"], stocks)) for l in lines],
                })
                call["note"] = index_inclusion_note(call, alert, ref)
                episode_calls.append(call)
        calls += episode_calls

        episode_logins = [] if same_account else logins_between(log, sale_account, start - LOGIN_LOOKBACK, start)
        for login in episode_logins:
            login.update({"episode": label, "before_s": seconds(start - login["ts"])})
        logins += episode_logins

        stock_unit = ref.instruments.get(sale["instrument"], {}).get("price_unit")
        lift = (sale["fill_price"] or 0) - (alert["stock_path"]["base"] or 0)
        advantage = lift * sale["fill_qty"] / (100 if stock_unit == "GBX" else 1)
        normal_max = run.normal_max(pressure_account)

        episodes.append({
            "id": label,
            "start": iso(start),
            "pressure": {
                "account": pressure_account,
                "future": pressure["future"],
                "side": pressure["orders"][0]["side"],
                "layers": len(pressure["orders"]),
                "size": pressure["orders"][0]["qty"],
                "total": sum(o["qty"] for o in pressure["orders"]),
                "cancelled": sum(o["qty"] for o in pressure["orders"] if o["order_id"] in {c["order_id"] for c in pressure["cancels"]}),
                "filled": pressure["filled_qty"],
                "prices": [o["price"] for o in pressure["orders"]],
                "size_multiple": round(pressure["orders"][0]["qty"] / normal_max, 1) if normal_max else None,
                "cancel_after_sale_s": seconds(pressure["cancels"][-1]["ts"] - sale["ts"]) if pressure["cancels"] else None,
            },
            "sale": {
                "account": sale_account,
                "stock": sale["instrument"],
                "side": sale["side"],
                "qty": sale["fill_qty"],
                "price": sale["fill_price"],
                "at_raised_bid": sale["fill_price"] == alert["stock_path"]["peak"],
            },
            "gap_s": seconds(sale["ts"] - start),
            "call_before_s": episode_calls[0]["before_s"] if episode_calls else None,
            "login_before_s": episode_logins[0]["before_s"] if episode_logins else None,
            "future_lift_ticks": alert["future_path"]["lift_ticks"],
            "stock_lift_ticks": alert["stock_path"]["lift_ticks"],
            "advantage_gbp": round(advantage, 2),
            "route": describe_route(alert["route"], ref),
            "chart": chart_for(alert, ref),
        })

    pairs = []
    for episode in episodes:
        pair = f"{episode['pressure']['future']} → {episode['sale']['stock']}"
        if pair not in pairs:
            pairs.append(pair)

    first, last = parse_ts(episodes[0]["start"]), parse_ts(episodes[-1]["start"])
    total_advantage = sum(e["advantage_gbp"] for e in episodes)
    multiples = [e["pressure"]["size_multiple"] for e in episodes if e["pressure"]["size_multiple"]]
    with_call = sum(1 for e in episodes if e["call_before_s"] is not None)
    path = path_links(alerts[-1]["path"])
    key_link = next((l for l in path if l["type"] == "SHARED_RESIDENTIAL_ADDRESS"), None)

    case = {
        "id": case_id,
        "kind": "same_account" if same_account else "cross_actor",
        "title": (
            f"Suspected cross-product spoofing: futures pressure by {pressure_account} "
            f"supporting its own cash sales"
            if same_account else
            f"Suspected cross-product, cross-actor spoofing: futures pressure by {pressure_account} "
            f"benefiting cash sales by {sale_account}"
        ),
        "short_title": "Same-account spoofing" if same_account else "Cross-actor spoofing",
        "headline": case_headline(episodes, path, ref, trader, trader_name, counterparty_name, same_account),
        "priority": "High" if not same_account else "Medium",
        "status": "Awaiting analyst decision",
        "date_range": f"{short_date(first)} to {short_date(last)} {last:%Y}" if first.date() != last.date() else f"{short_date(first)} {first:%Y}",
        "pairs": pairs,
        "connected_party": key_link is not None,
        "trader": {"id": trader, "name": trader_name},
        "counterparty": {"id": counterparty, "name": counterparty_name},
        "pressure_account": pressure_account,
        "sale_account": sale_account,
        "episodes": episodes,
        "calls": calls,
        "logins": logins,
        "summary": case_summary(episodes, path, ref, trader, trader_name, pressure_account, sale_label,
                                same_account, with_call, counterparty_name),
        "stats": [
            {"value": f"{len(episodes)} of {len(episodes)}", "label": "Episodes matching the full spoof signature", "tone": "data"},
            {"value": "0%", "label": "Fill rate on pressure orders", "tone": "data"},
            {
                "value": (f"{min(multiples):g}x" if min(multiples) == max(multiples) else f"{min(multiples):g} to {max(multiples):g}x") if multiples else "n/a",
                "label": f"Order size vs {trader_name.split()[0]}'s normal maximum",
                "tone": "alert",
            },
            {"value": f"£{total_advantage:,.0f}", "label": "Estimated price advantage", "tone": "alert"},
        ],
        "findings": case_findings(episodes, alerts, calls, logins, ref, same_account, counterparty_name, trader_name),
        "graph": case_graph(alerts, calls, ref, trader, counterparty, pressure_account, sale_account),
        "baseline": baseline_checks(run, alerts, episodes, pressure_account, sale_account, same_account),
        "trace": case_trace(run, alerts, episodes, calls, logins, same_account),
        "generated_at": iso(datetime.now(timezone.utc)),
    }
    if not same_account:
        case["stats"][0] = {"value": f"{with_call} of {len(episodes)}", "label": "Episodes with a prior call", "tone": "data"}
    return case


def index_inclusion_note(call, alert, ref):
    if not any("index" in line["text"].lower() for line in call["lines"]):
        return None
    for link in alert["route"]:
        if link["type"] == "CONSTITUENT_OF" and link["valid_from"] and call["ts"] - link["valid_from"] <= timedelta(days=31):
            return (
                f"References {link['from']}'s inclusion in {link['to']} on {short_date(link['valid_from'])}, "
                f"consistent with the switch to {alert['pressure']['future']} as the pressure instrument."
            )
    return None


def path_sentence(path, ref, trader, trader_name):
    for link in path:
        if link["type"] != "SHARED_RESIDENTIAL_ADDRESS":
            continue
        other = link["to"] if link["from"] == trader else link["from"]
        owner_of = next((l for l in path if l["type"] == "BENEFICIAL_OWNER_OF" and l["from"] == other), None)
        if owner_of:
            return (f"{trader_name} shares a residential address with {ref.name(other)}, "
                    f"beneficial owner of {ref.name(owner_of['to'])}")
        return f"{trader_name} shares a residential address with {ref.name(other)}"
    return "they are connected through people and ownership records"


def case_headline(episodes, path, ref, trader, trader_name, counterparty_name, same_account):
    """One or two short sentences for the top of the case."""
    futures = human_join(list(dict.fromkeys(e["pressure"]["future"] for e in episodes)))
    times = {1: "once", 2: "twice"}.get(len(episodes), f"{len(episodes)} times")
    if same_account:
        return f"{trader_name} spoofed {futures} {times} and sold the stock from the same account."
    linked = ""
    link = next((l for l in path if l["type"] == "SHARED_RESIDENTIAL_ADDRESS"), None)
    if link:
        other = link["to"] if link["from"] == trader else link["from"]
        owner = any(l["type"] == "BENEFICIAL_OWNER_OF" and l["from"] == other for l in path)
        linked = (f" {trader_name.split()[0]} shares a home address with "
                  f"{counterparty_name}'s owner, {ref.name(other)}." if owner else
                  f" {trader_name.split()[0]} shares a home address with {ref.name(other)}.")
    return f"{trader_name} spoofed {futures} {times}. {counterparty_name} sold into every price lift.{linked}"


def case_summary(episodes, path, ref, trader, trader_name, pressure_account, sale_label, same_account,
                 with_call, counterparty_name):
    layers = episodes[0]["pressure"]["layers"]
    side = "buy" if episodes[0]["pressure"]["side"] == "BUY" else "sell"
    cancel_s = max(e["chart"]["window"][1] for e in episodes)
    opening = (
        f"On {len(episodes)} occasions, trader {trader_name} ({trader}) placed {layers} large, layered {side} "
        f"orders in a future and cancelled them, unfilled, within {math.ceil(cancel_s)} seconds. "
    )
    if same_account:
        return opening + (
            f"Each time, the same account ({pressure_account}) sold the related stock into the briefly higher bid. "
            "A same-account check would catch this; it is shown to confirm the pattern."
        )
    calls = (f", and called {counterparty_name}'s dealing line before {with_call} of {len(episodes)} episodes"
             if with_call else "")
    return opening + (
        f"Each time, {sale_label} sold the related stock into the briefly higher bid. "
        f"The two accounts look unrelated, but {path_sentence(path, ref, trader, trader_name)}{calls}."
    )


def case_findings(episodes, alerts, calls, logins, ref, same_account, counterparty_name, trader_name):
    findings = []
    counts = defaultdict(int)
    for episode in episodes:
        counts[f"{episode['pressure']['future']} → {episode['sale']['stock']}"] += 1
    routes = "; ".join(f"{pair} ({n} episode{'s' if n > 1 else ''})" for pair, n in counts.items())
    findings.append({"text": f"The pattern repeats across {routes}.", "ref": f"E1–E{len(episodes)}"})

    constituent = next((l for a in alerts for l in a["route"] if l["type"] == "CONSTITUENT_OF"), None)
    if constituent:
        first_use = next(e for e, a in zip(episodes, alerts) if constituent in a["route"])
        days = (parse_ts(first_use["start"]).date() - constituent["valid_from"].date()).days
        findings.append({
            "text": (f"{constituent['from']} joined {constituent['to']} on {short_date(constituent['valid_from'])} "
                     f"{constituent['valid_from']:%Y}. The index-future route only exists from that date, and the first "
                     f"episode on it followed {days} days later."),
            "ref": "Point in time",
        })

    key = next((l for l in path_links(alerts[-1]["path"]) if l["type"] == "SHARED_RESIDENTIAL_ADDRESS"), None)
    if key:
        findings.append({
            "text": (f"Hidden link: {ref.name(key['from'])} and {ref.name(key['to'])} share a residential address "
                     f"({key['source']}, confidence {key['confidence']:.2f})."),
            "ref": key["id"],
        })
    if calls:
        lead = sorted(c["before_s"] for c in calls)
        findings.append({
            "text": (f"Recorded calls from {trader_name} to {counterparty_name} precede {len({c['episode'] for c in calls})} "
                     f"of {len(episodes)} episodes, {fmt_duration(lead[0])} to {fmt_duration(lead[-1])} before each."),
            "ref": f"{calls[0]['evidence']}–{calls[-1]['evidence']}",
        })
    if logins:
        lead = sorted(l["before_s"] for l in logins)
        findings.append({
            "text": (f"{counterparty_name} logged in to its DMA gateway {fmt_duration(lead[0])} to "
                     f"{fmt_duration(lead[-1])} before each episode, outside its usual morning login."),
            "ref": "DMA logins",
        })
    if same_account:
        findings.append({"text": "Both legs sit in one account, so the existing same-account check would raise this case too.",
                         "ref": "Baseline"})
    return findings


def fmt_duration(total_seconds):
    total_seconds = int(round(total_seconds))
    minutes, secs = divmod(total_seconds, 60)
    return f"{minutes} m {secs} s" if minutes else f"{secs} s"


def case_graph(alerts, calls, ref, trader, counterparty, pressure_account, sale_account):
    nodes, edges = {}, []

    def add(node, role):
        if node and node not in nodes:
            info = ref.actors.get(node) or {}
            account = ref.accounts.get(node) or {}
            sub = f"{node} · {info.get('role') or info.get('type') or ''}".strip(" ·") if info else ""
            if node in ref.instruments:
                sub = ref.instruments[node]["asset_class"]
            elif account:
                sub = account.get("name") or ""
            nodes[node] = {"id": node, "label": info.get("name") or node, "sub": sub, "role": role}

    same_account = pressure_account == sale_account
    add(trader, "trader")
    add(pressure_account, "pressure_account" if not same_account else "account")
    if not same_account:
        add(counterparty, "counterparty")
        add(sale_account, "sale_account")

    seen = set()
    for link in path_links(alerts[-1]["path"]):
        for node in (link["from"], link["to"]):
            add(node, "link")
        if link["id"] not in seen:
            seen.add(link["id"])
            edges.append({
                "from": link["from"], "to": link["to"],
                "label": describe_link(link, ref).upper(),
                "kind": "key" if link["type"] == "SHARED_RESIDENTIAL_ADDRESS" else "declared",
            })
    if trader and pressure_account and not any({e["from"], e["to"]} == {trader, pressure_account} for e in edges):
        edges.append({"from": trader, "to": pressure_account, "label": "TRADES FOR", "kind": "declared"})

    if calls:
        edges.append({"from": trader, "to": counterparty, "label": f"{len(calls)} RECORDED CALLS", "kind": "declared"})
    for link in alerts[-1]["inferred"]:
        edges.append({
            "from": link["from"], "to": link["to"],
            "label": f"COMPLEMENTARY TRADING (INFERRED) {link['confidence']:.2f}", "kind": "inferred",
        })

    pair_edges = {}
    for alert in alerts:
        future, stock = alert["pressure"]["future"], alert["sale"]["instrument"]
        add(future, "future")
        add(stock, "stock")
        edges.append({"from": pressure_account, "to": future, "label": "", "kind": "activity"})
        edges.append({"from": sale_account, "to": stock, "label": "", "kind": "activity"})
        constituent = next((l for l in alert["route"] if l["type"] == "CONSTITUENT_OF"), None)
        if constituent:
            label = f"CONSTITUENT FROM {short_date(constituent['valid_from']).upper()}"
            kind = "point_in_time"
        else:
            label = alert["route"][0]["type"].replace("_", " ")
            kind = "declared" if alert["route"][0]["provenance"] == "Declared" else "inferred"
        pair_edges[(future, stock)] = {"from": future, "to": stock, "label": label, "kind": kind}
    edges += pair_edges.values()

    unique = []
    for edge in edges:
        if not any(e["from"] == edge["from"] and e["to"] == edge["to"] and e["kind"] == edge["kind"] for e in unique):
            unique.append(edge)
    return {"nodes": list(nodes.values()), "edges": unique, "as_of": iso(alerts[-1]["start"])}


def baseline_checks(run, alerts, episodes, pressure_account, sale_account, same_account):
    """What per-account, per-product checks would see on their own."""
    checks = []
    flagged = sum(1 for a in alerts if a["sale"]["account"] == a["pressure"]["account"])
    checks.append({
        "check": "Same-account spoofing check",
        "scope": "Orders and trades in one account",
        "result": f"Flags {flagged} of {len(alerts)}",
        "verdict": "caught" if flagged == len(alerts) else "missed",
        "detail": (
            "Pressure and sale sit in the same account."
            if flagged == len(alerts) else
            f"{pressure_account} never trades the stock and {sale_account} never touches the future."
        ),
    })

    activity = run.activity(pressure_account)
    futures = {e["pressure"]["future"] for e in episodes}
    episode_days = {parse_ts(e["start"]).date() for e in episodes}
    by_day = defaultdict(lambda: {"new": 0, "cancel": 0})
    for event in activity:
        if event["instrument"] in futures and event["type"] in ("NEW", "CANCEL"):
            by_day[event["ts"].date()]["new" if event["type"] == "NEW" else "cancel"] += 1
    ratio = lambda days: (sum(by_day[d]["cancel"] for d in days) / max(1, sum(by_day[d]["new"] for d in days)))
    other_days = [d for d in by_day if d not in episode_days]
    checks.append({
        "check": f"Cancel ratio, {pressure_account} in {' and '.join(sorted(futures))}",
        "scope": "One account, one product, per day",
        "result": f"{ratio(episode_days):.0%} on episode days vs {ratio(other_days):.0%} on other days",
        "verdict": "partial",
        "detail": "Raises a single-product alert, with nothing to tie it to a trade elsewhere.",
    })

    if not same_account:
        at_bid = sum(1 for e in episodes if e["sale"]["at_raised_bid"])
        checks.append({
            "check": f"Large trade review, {sale_account}",
            "scope": "One account, one product",
            "result": f"{len(episodes)} large sells, {at_bid} at the prevailing best bid",
            "verdict": "missed",
            "detail": "Each sale looks like an ordinary execution when viewed on its own.",
        })
    return checks


def case_trace(run, alerts, episodes, calls, logins, same_account):
    ref = run.ref
    log = run.log
    routes = []
    for episode in episodes:
        if episode["route"] not in routes:
            routes.append(episode["route"])
    others = {f["account"] for a in alerts for f in a["other_matches"]}
    matched = len(alerts)
    chain = path_chain(alerts[-1]["path"], ref)

    steps = [
        {
            "title": f"Pulled every equity fill of {MIN_EXECUTION_QTY:,} shares or more",
            "detail": f"{len(run.candidates):,} large equity fills out of {run.total_fills:,} fills",
            "queries": log.count("executions") + log.count("reference"),
            "ms": log.ms("executions") + log.ms("reference"),
        },
        {
            "title": "Traversed related instruments as of each trade date",
            "detail": "; ".join(routes),
            "queries": log.count("instruments"),
            "ms": log.ms("instruments"),
        },
        {
            "title": f"Searched futures for layered, cancelled orders in the {LOOKBACK.seconds} s before each sale",
            "detail": (f"{alerts[0]['pressure']['account']} matched {matched} of {matched} episodes; "
                       f"{'no other account matched' if not others else ', '.join(sorted(others)) + ' also matched'}"),
            "queries": log.count("search") + log.count("history") + log.count("prices"),
            "ms": log.ms("search") + log.ms("history") + log.ms("prices"),
        },
        {
            "title": f"Traversed people and ownership links as of each trade date, up to {MAX_HOPS} hops",
            "detail": chain if not same_account else "Pressure and sale in the same account",
            "queries": log.count("people"),
            "ms": log.ms("people"),
        },
        {
            "title": "Retrieved calls, transcripts and DMA logins between the parties",
            "detail": (
                f"{len(calls)} recorded calls, {fmt_duration(min(c['before_s'] for c in calls))} to "
                f"{fmt_duration(max(c['before_s'] for c in calls))} before an episode; {len(logins)} DMA logins just before"
                if calls else "No calls or logins between separate parties to check"
            ),
            "queries": log.count("comms"),
            "ms": log.ms("comms"),
        },
        {
            "title": "Tested alternative explanations",
            "detail": " ".join(control_notes(run)),
            "queries": 0,
            "ms": 0,
            "tone": "check",
        },
    ]
    return steps


def control_notes(run):
    notes = []
    no_route = [o for o in run.outcomes if o["result"] == "no_route"]
    for outcome in no_route:
        sale = outcome["sale"]
        futures = " and ".join(sorted({f["future"] for f in outcome["found"]}))
        accounts = " and ".join(sorted({f["account"] for f in outcome["found"]}))
        verb = "sold" if sale["side"] == "SELL" else "bought"
        notes.append(
            f"{short_date(sale['ts'])}: {accounts} layered {futures} before {sale['account']} {verb} "
            f"{sale['fill_qty']:,} {sale['instrument']}, but {sale['instrument']} had no link to {futures} "
            f"that day, so no cross-product alert."
        )

    # Genuine two-legged trading: the futures order filled and nothing was cancelled.
    hedges = defaultdict(lambda: {"days": set(), "stocks": set()})
    for outcome in run.outcomes:
        if outcome["result"] != "no_pressure":
            continue
        for miss in outcome["near_misses"]:
            if miss["filled_qty"] > 0 and not miss["cancels"]:
                entry = hedges[(miss["account"], miss["future"])]
                entry["days"].add(outcome["sale"]["ts"].date())
                entry["stocks"].add(outcome["sale"]["instrument"])
    for (account, future), entry in hedges.items():
        days = sorted(entry["days"])
        when = " and ".join(str(d.day) for d in days) + f" {days[-1]:%b}"
        notes.append(
            f"{when}: {account} traded {human_join([future] + sorted(entry['stocks']))} with both legs "
            f"filled and nothing cancelled, consistent with index arbitrage, so no alert."
        )
    return notes or ["No other large executions showed spoof-like futures activity."]


# ------------------------------------------------------------
# Alerts page: every large execution examined and what happened to it
# ------------------------------------------------------------

OUTCOME_LABELS = {
    "alert": "Alert raised",
    "no_route": "Cleared: no instrument link that day",
    "no_link": "Cleared: no link between the accounts",
    "no_price_effect": "Cleared: price did not move",
}


def examined_executions(run):
    alerts = {id(a["sale"]): a for a in run.alerts}
    rows = []
    for outcome in run.outcomes:
        sale = outcome["sale"]
        alert = alerts.get(id(sale))
        result = outcome["result"]
        if result == "no_pressure":
            hedged = any(m["filled_qty"] > 0 and not m["cancels"] for m in outcome["near_misses"])
            label = "Cleared: both legs filled (arbitrage)" if hedged else "Cleared: no spoof-like futures orders"
        else:
            label = OUTCOME_LABELS[result]
        pressure = alert["pressure"] if alert else (outcome["found"][0] if outcome["found"] else None)
        rows.append({
            "ts": iso(sale["ts"]),
            "account": sale["account"],
            "instrument": sale["instrument"],
            "side": sale["side"],
            "qty": sale["fill_qty"],
            "price": sale["fill_price"],
            "result": result,
            "label": label,
            "pressure_account": pressure["account"] if pressure else None,
            "pressure_future": pressure["future"] if pressure else None,
            "same_account_check": bool(alert) and alert["pressure"]["account"] == sale["account"],
            "case_id": alert.get("case_id") if alert else None,
            "episode": alert.get("episode") if alert else None,
        })
    return rows


# ------------------------------------------------------------
# Graph explorer: the case's network as of any date, queried live
# ------------------------------------------------------------

SHORT_LINK_LABELS = {
    "UNDERLYING": "UNDERLYING",
    "CONSTITUENT_OF": "CONSTITUENT",
    "TRADES_FOR": "TRADES FOR",
    "OWNS": "OWNS",
    "BENEFICIAL_OWNER_OF": "BENEFICIAL OWNER",
    "COMMUNICATES_WITH": "CALLS",
    "SAME_FLOOR": "SAME FLOOR",
}
KIND_RANK = ["key", "declared", "point_in_time", "inferred"]


def link_kind(link):
    if link["type"] == "SHARED_RESIDENTIAL_ADDRESS":
        return "key"
    if link["provenance"] == "Inferred":
        return "inferred"
    if link["type"] == "CONSTITUENT_OF":
        return "point_in_time"
    return "declared"


def link_label(link):
    if link["type"] in SHORT_LINK_LABELS:
        return SHORT_LINK_LABELS[link["type"]]
    names = {"SHARED_RESIDENTIAL_ADDRESS": "SHARED ADDRESS", "CROSS_IMPACT": "LEADS",
             "COMPLEMENTARY_TRADING": "COMPLEMENTARY TRADING"}
    return f"{names.get(link['type'], link['type'].replace('_', ' '))} {link['confidence']:.2f}"


def link_status(link, day):
    if link["valid_from"] and link["valid_from"].date() > day:
        return "not_yet", f"From {short_date(link['valid_from'])}"
    if link["valid_to"] and link["valid_to"].date() < day:
        return "ended", f"Ended {short_date(link['valid_to'])}"
    return "active", "Active"


def graph_as_of(run, case, day):
    log = QueryLog()
    ref = run.ref
    active = {l["id"] for l in instrument_links_on(log, day) + actor_links_on(log, day)}
    every = [
        _link(r, "from_instrument", "to_instrument")
        for r in log.run("instruments", f"SELECT from_instrument, to_instrument, weight, {LINK_COLUMNS} FROM {INSTRUMENT_LINKS_TABLE}")
    ] + [
        _link(r, "from_actor", "to_actor")
        for r in log.run("people", f"SELECT from_actor, to_actor, {LINK_COLUMNS} FROM {ACTOR_LINKS_TABLE}")
    ]

    nodes = {n["id"]: dict(n) for n in case["graph"]["nodes"]}
    instruments = {i for i, n in nodes.items() if n["role"] in ("future", "stock")}
    for link in every:
        if link["from"] in instruments and ref.asset_class(link["to"]) == "Index" and link["to"] not in nodes:
            nodes[link["to"]] = {"id": link["to"], "label": link["to"], "sub": "Index", "role": "index"}

    pairs = defaultdict(list)
    table = []
    for link in every:
        if link["from"] not in nodes or link["to"] not in nodes:
            continue
        status, status_text = (("active", "Active") if link["id"] in active else link_status(link, day))
        pairs[(link["from"], link["to"])].append((link, status))
        table.append({
            "id": link["id"], "type": link["type"].replace("_", " ").lower(),
            "from": link["from"], "to": link["to"],
            "from_name": ref.name(link["from"]), "to_name": ref.name(link["to"]),
            "provenance": link["provenance"], "source": link["source"], "confidence": link["confidence"],
            "valid_from": iso(link["valid_from"]), "valid_to": iso(link["valid_to"]),
            "status": status, "status_text": status_text,
        })

    # A lead-lag edge from an index future to a constituent repeats a route the
    # index already shows, so it stays in the table but is not drawn.
    via_index = {
        (u["from"], c["from"])
        for u in every if u["type"] == "UNDERLYING"
        for c in every if c["type"] == "CONSTITUENT_OF" and c["to"] == u["to"]
    }
    edges = []
    for (source, target), links in pairs.items():
        if (source, target) in via_index and all(l["type"] == "CROSS_IMPACT" for l, _ in links):
            continue
        live = [l for l, s in links if s == "active"]
        if live:
            kind = min((link_kind(l) for l in live), key=KIND_RANK.index)
            # The same link loaded twice would otherwise read "LEADS 0.94 · LEADS 0.94".
            label = " · ".join(dict.fromkeys(link_label(l) for l in live))
        else:
            first = links[0][0]
            kind = "inactive"
            label = f"{link_label(first)} {link_status(first, day)[1].upper()}"
        edges.append({"from": source, "to": target, "label": label, "kind": kind})
    edges += [e for e in case["graph"]["edges"] if e["kind"] == "activity"]

    first_day = min(s["ts"] for s in run.candidates).date()
    last_day = max(s["ts"] for s in run.candidates).date()
    key_dates = [{"date": e["start"][:10], "label": e["id"]} for e in case["episodes"]]
    key_dates += [
        {"date": iso(l["valid_from"])[:10], "label": f"{l['from']} joins {l['to']}"}
        for l in every
        if l["type"] == "CONSTITUENT_OF" and l["from"] in nodes and l["to"] in nodes
        and l["valid_from"] and first_day < l["valid_from"].date() <= last_day
    ]
    key_dates += [
        {"date": iso(o["sale"]["ts"])[:10], "label": "Control"}
        for o in run.outcomes if o["result"] == "no_route"
    ]
    unique_dates = {}
    for item in sorted(key_dates, key=lambda d: d["date"]):
        unique_dates.setdefault(item["date"], []).append(item["label"])

    return {
        "as_of": day.isoformat(),
        "first_day": first_day.isoformat(),
        "last_day": last_day.isoformat(),
        "key_dates": [{"date": d, "label": " · ".join(labels)} for d, labels in unique_dates.items()],
        "nodes": list(nodes.values()),
        "edges": edges,
        "links": sorted(table, key=lambda l: (l["status"] != "active", l["id"])),
        "queries": log.entries,
        "ms": log.ms(),
    }


# ------------------------------------------------------------
# Run state: detection runs in the background at start-up
# ------------------------------------------------------------

STATE = {"status": "idle", "run": None, "error": None}
STATE_LOCK = threading.Lock()


def start_run():
    with STATE_LOCK:
        if STATE["status"] == "running":
            return
        STATE.update(status="running", error=None)

    def work():
        try:
            run = Investigation().run()
            save_run(run)
            with STATE_LOCK:
                STATE.update(status="ready", run=run)
        except Exception as error:
            with STATE_LOCK:
                STATE.update(status="error", error=str(error))

    threading.Thread(target=work, daemon=True).start()


# A finished run is saved so a restart opens straight away. "Run again" replaces it.
SAVED_RUN = HERE / "detection_run.pkl"


def save_run(run):
    if SAVED_RUN is None:
        return
    try:
        SAVED_RUN.write_bytes(pickle.dumps(run))
    except Exception as error:
        print(f"Could not save the detection run: {error}", file=sys.stderr)


def load_saved_run():
    if SAVED_RUN is None or not SAVED_RUN.exists():
        return None
    try:
        return pickle.loads(SAVED_RUN.read_bytes())
    except Exception as error:
        print(f"Saved detection run unreadable, running again: {error}", file=sys.stderr)
        return None


@app.on_event("startup")
def on_startup():
    saved = load_saved_run()
    if saved is None:
        start_run()
        return
    with STATE_LOCK:
        STATE.update(status="ready", run=saved, error=None)
    print(f"Loaded the saved detection run from {saved.started_at:%d %b %H:%M} UTC. "
          f"Use Run again for a fresh one.", file=sys.stderr)


def current_run():
    run = STATE["run"]
    if run is None:
        raise HTTPException(status_code=503, detail=STATE["error"] or "Detection is still running")
    return run


# ------------------------------------------------------------
# API
# ------------------------------------------------------------


@app.get("/api/status")
def status():
    log = QueryLog()
    tables = []
    for label, name in TABLES:
        try:
            rows = log.run("status", f"SELECT COUNT(*) FROM {name}")
            count = int(num(next(iter(rows[0].values())))) if rows else 0
            tables.append({"label": label, "table": name, "rows": count, "ms": log.entries[-1]["ms"]})
        except Exception as error:
            tables.append({"label": label, "table": name, "rows": None, "error": str(error)})
    connected = any(t["rows"] is not None for t in tables)
    return {
        "black_forest": "connected" if connected else "unavailable",
        "tables": tables,
        "total_rows": sum(t["rows"] or 0 for t in tables),
        "ms": log.ms(),
    }


@app.get("/api/investigation")
def investigation():
    run = STATE["run"]
    body = {"status": STATE["status"], "error": STATE["error"]}
    if run is not None:
        body.update({
            "generated_at": iso(run.started_at),
            "queries": run.log.count(),
            "query_ms": run.log.ms(),
            "total_ms": run.total_ms,
            "candidates": len(run.candidates),
            "cases": [
                {
                    "id": case["id"],
                    "title": case["title"],
                    "kind": case["kind"],
                    "priority": case["priority"],
                    "episodes": len(case["episodes"]),
                    "date_range": case["date_range"],
                    "pairs": case["pairs"],
                }
                for case in run.cases.values()
            ],
            "log": run.log.entries,
            "examined": examined_executions(run),
            "rules": [
                f"A stock fill of {MIN_EXECUTION_QTY:,} shares or more",
                f"In the {LOOKBACK.seconds} s before it, {MIN_LAYERS} or more opposite-side futures orders "
                f"within {int(LAYER_WINDOW.total_seconds() * 1000)} ms",
                f"Each order at least {SIZE_MULTIPLE}x the account's normal size, none filled, "
                f"all cancelled within {CANCEL_WITHIN.seconds} s of the sale",
                "The future linked to the stock on that date (underlying, index membership or a lead-lag "
                f"model scoring {MIN_CROSS_IMPACT} or more)",
                f"The two accounts linked through people and ownership records, up to {MAX_HOPS} hops",
                "The stock bid lifted at least 1 tick before the sale and fell back within "
                f"{LOOKAHEAD.seconds} s",
            ],
        })
    return body


@app.post("/api/investigation/run")
def rerun():
    start_run()
    return {"status": "running"}


@app.get("/api/cases/{case_id}")
def case(case_id: str):
    run = current_run()
    if case_id not in run.cases:
        raise HTTPException(status_code=404, detail=f"No case {case_id}")
    return run.cases[case_id]


GRAPH_CACHE = {}


@app.get("/api/graph")
def graph(case_id: str, as_of: str | None = None):
    run = current_run()
    if case_id not in run.cases:
        raise HTTPException(status_code=404, detail=f"No case {case_id}")
    case = run.cases[case_id]
    try:
        day = datetime.fromisoformat(as_of).date() if as_of else parse_ts(case["episodes"][-1]["start"]).date()
    except ValueError:
        raise HTTPException(status_code=400, detail="as_of must be a date like 2026-10-14")
    # Links for a past date don't change, so each date is queried once per detection run.
    key = (run.started_at, case_id, day)
    cached = key in GRAPH_CACHE
    if not cached:
        GRAPH_CACHE[key] = graph_as_of(run, case, day)
    return {**GRAPH_CACHE[key], "cached": cached}



# ------------------------------------------------------------
# Dashboard
# ------------------------------------------------------------


@app.get("/")
def dashboard():
    return FileResponse(HERE / "index.html")
