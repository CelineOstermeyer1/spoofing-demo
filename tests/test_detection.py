"""
Checks the detection logic against the answer key without Black Forest.

Swaps server.run_sql for a tiny SQL reader over the upload-ready CSVs in
ingest/. It understands only the SQL the server writes:
SELECT columns FROM table [WHERE column op 'value' AND ...].

Run from the demo folder:  python tests/test_detection.py
With the scaled-up data:   python tests/test_detection.py --scale
"""

import csv
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(HERE))

import server  # noqa: E402

FILES = {
    server.ORDERS_TABLE: "order_events.csv",
    server.MARKET_TABLE: "market_data_top_of_book.csv",
    server.INSTRUMENT_LINKS_TABLE: "instrument_relationships.csv",
    server.ACTOR_LINKS_TABLE: "actor_relationships.csv",
    server.INSTRUMENTS_TABLE: "instruments.csv",
    server.ACTORS_TABLE: "actors.csv",
    server.ACCOUNTS_TABLE: "accounts.csv",
    server.COMMS_TABLE: "communications.csv",
    server.TRANSCRIPTS_TABLE: "communication_transcripts.csv",
    server.LOGINS_TABLE: "access_logs.csv",
}
TIME_COLUMNS = {"timestamp_utc", "valid_from", "valid_to"}
CACHE = {}


def rows_for(table):
    if table not in CACHE:
        paths = [HERE / "ingest" / FILES[table]]
        # SCALE_DATA=1 adds the generated background rows from ingest_scale/.
        if os.environ.get("SCALE_DATA"):
            paths += sorted((HERE / "ingest_scale").glob(FILES[table].removesuffix(".csv") + "*.csv"))
        CACHE[table] = []
        for path in paths:
            with open(path, newline="", encoding="utf-8") as f:
                CACHE[table] += list(csv.DictReader(f))
    return CACHE[table]


def comparable(column, value):
    if column in TIME_COLUMNS:
        return server.parse_ts(value)
    try:
        return float(value)
    except ValueError:
        return value


def as_black_forest(column, value):
    # Return timestamps the way the Data Grid shows them.
    if column in TIME_COLUMNS and value:
        return server.sql_ts(server.parse_ts(value))
    return value


def fake_sql(sql):
    match = re.match(r"\s*SELECT\s+(.+?)\s+FROM\s+([\w.]+)(?:\s+WHERE\s+(.+))?\s*$", sql, re.S | re.I)
    columns, table, where = match.groups()
    conditions = [
        (column, op, quoted if number == "" else number)
        for column, op, quoted, number in re.findall(r"(\w+)\s*(=|>=|<=)\s*(?:'([^']*)'|(-?\d+(?:\.\d+)?))", where or "")
    ]
    selected = []
    for row in rows_for(table):
        ok = True
        for column, op, literal in conditions:
            left, right = comparable(column, row.get(column, "")), comparable(column, literal)
            if left in ("", None) or type(left) is not type(right):
                ok = ok and op == "=" and str(row.get(column, "")) == literal
                continue
            ok = ok and {"=": left == right, ">=": left >= right, "<=": left <= right}[op]
        if ok:
            selected.append(row)
    if columns.strip().upper() == "COUNT(*)":
        return [{"COUNT(*)": len(selected)}]
    names = [c.strip() for c in columns.split(",")]
    return [{n: as_black_forest(n, r.get(n, "")) for n in names} for r in selected]


def main():
    if "--scale" in sys.argv:
        os.environ["SCALE_DATA"] = "1"
    server.run_sql = fake_sql
    run = server.Investigation().run()

    alerts = {(a["sale"]["ts"].date().isoformat(), a["sale"]["account"], a["sale"]["instrument"]) for a in run.alerts}
    with open(HERE / "data" / "answer_key.csv", newline="", encoding="utf-8-sig") as f:
        key = list(csv.DictReader(f))
    expected = {
        (row["date"], row["execution_account"].split()[0], row["execution_instrument"])
        for row in key if row["expected_outcome"].startswith("ALERT")
    }

    problems = []
    if alerts != expected:
        problems.append(f"alerts differ\n  missing: {sorted(expected - alerts)}\n  extra:   {sorted(alerts - expected)}")
    sizes = sorted(len(c["episodes"]) for c in run.cases.values())
    if sizes != [2, 5]:
        problems.append(f"expected cases of 2 and 5 episodes, got {sizes}")

    print(f"{len(run.alerts)} alerts in {len(run.cases)} cases from {len(run.candidates)} candidate sales, "
          f"{run.log.count()} queries")
    for case in run.cases.values():
        print(f"\n{case['id']}  {case['title']}")
        print(f"  {case['summary']}")
        for stat in case["stats"]:
            print(f"  [{stat['value']}] {stat['label']}")
        for finding in case["findings"]:
            print(f"  - {finding['text']} ({finding['ref']})")
        for episode in case["episodes"]:
            p, s = episode["pressure"], episode["sale"]
            print(f"  {episode['id']} {episode['start']}  {p['future']} {p['side']} {p['layers']}x{p['size']} "
                  f"cancelled {p['cancelled']}/{p['total']}  {s['stock']} {s['side']} {s['qty']:,} @ {s['price']}  "
                  f"gap {episode['gap_s']}s  call {episode['call_before_s']}  login {episode['login_before_s']}  "
                  f"£{episode['advantage_gbp']:,.0f}")
        for check in case["baseline"]:
            print(f"  baseline: {check['check']}: {check['result']} ({check['verdict']})")
        for step in case["trace"]:
            print(f"  trace: {step['title']} | {step['detail']} | {step['queries']} queries")
        for call in case["calls"]:
            print(f"  call {call['evidence']} {call['ts']:%d %b %H:%M:%S} {len(call['lines'])} lines note={bool(call['note'])}")

    if problems:
        print("\nFAILED\n" + "\n".join(problems))
        sys.exit(1)
    print("\nOK: alerts match the answer key")


if __name__ == "__main__":
    main()
