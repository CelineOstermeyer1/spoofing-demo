"""
Generates a large volume of innocent background activity to sit around
Paul's planted scenarios, so the demo can show the same 7 alerts being found
in a much bigger dataset.

Writes only the extra rows, in the same upload-ready format as ingest/, to
ingest_scale/. Upload them into the existing tables; Paul's rows stay as they
are. The output is the same every time for the same settings.

What the extra data is, and is not:
  - Ordinary orders, fills, partial fills and cancels on the same instruments
    and days, from Paul's accounts plus many new ones. Futures orders stay
    small (1 to 20 lots), so nothing looks like layered spoofing pressure.
  - A sprinkling of large, genuine equity fills that the detection has to
    check and clear.
  - A continuous best-bid feed, routine calls and chats, and daily DMA logins.
  - Nothing within 30 seconds of any planted event, no calls from T104 to
    C3107, and no new links to the case's people, so the case evidence is
    unchanged.

Run from the demo folder:
  python scale_data.py                        # default size, about 1.3 million rows
  python scale_data.py --orders-per-day 2000  # smaller, for a quick test
"""

import argparse
import csv
import math
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

HERE = Path(__file__).parent
SRC = HERE / "data"
INGEST = HERE / "ingest"
OUT = HERE / "ingest_scale"

FUTURES = {"NGEZ6", "ALBZ6"}
# Median background prices in Paul's data, the level each random walk starts from.
BASE_PRICES = {
    "NGE": 2448.75, "HRB": 613.0, "KPL": 1175.0, "SVM": 881.0, "BRT": 142.75,
    "DWR": 321.25, "NGEZ6": 2461.0, "ALBZ6": 8315.5,
}
DWR_LAST_DAY = "2026-09-18"  # DWR left the index and stops trading after this
DAY_OPEN, DAY_CLOSE = 8 * 3600, 16 * 3600 + 30 * 60
BLACKOUT = timedelta(seconds=30)
CHUNK_ROWS = 200_000

FIRST_NAMES = ["Avery", "Blake", "Casey", "Drew", "Elliot", "Finley", "Harper", "Jamie", "Kai", "Logan",
               "Morgan", "Noel", "Parker", "Quinn", "Reese", "Rowan", "Sage", "Taylor", "Tristan", "Wren"]
LAST_NAMES = ["Ashdown", "Brierley", "Calloway", "Dunmore", "Elstree", "Fairley", "Garrow", "Hollins",
              "Ingham", "Jessop", "Kirkby", "Lowther", "Marlow", "Norwood", "Oakes", "Pemberton",
              "Quarrie", "Redfern", "Stanway", "Thorne"]
DESKS = ["Equity Derivatives Trading", "Cash Equities", "Index Arbitrage", "Delta One"]
INTERNAL = ["Internal: Risk", "Internal: Sales", "Internal: Compliance", "Internal: Operations"]
# Routine chatter. None of it uses the phrases the transcript rules highlight.
LINES = [
    "Morning, anything on your side today?",
    "Just the usual flow, nothing urgent.",
    "Can you confirm the settlement instructions for yesterday?",
    "We're rebalancing a little ahead of month end.",
    "Risk limits are fine, carry on.",
    "I'll send the allocation file over shortly.",
    "Thanks, that's all for now.",
    "Can we push the call to this afternoon?",
    "The client wants a quote for next week.",
    "Booked and confirmed.",
    "Any news on the dividend dates?",
    "I'll check with operations and come back to you.",
]


def ts(moment):
    return moment.strftime("%Y-%m-%dT%H:%M:%S.") + f"{moment.microsecond:06d}Z"


def parse(value):
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def read(name):
    with open(SRC / name, newline="", encoding="utf-8-sig") as f:
        return list(csv.DictReader(f))


class Writer:
    """Writes rows with the same columns and predicate as the matching ingest/ file."""

    def __init__(self, name, chunk=False):
        with open(INGEST / name, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            self.fields = reader.fieldnames
            first = next(reader)
        self.predicate = first.get("predicate")
        self.stem = name.removesuffix(".csv")
        self.chunk = chunk
        self.part, self.rows, self.total, self.file = 0, 0, 0, None
        self.files = []

    def _open(self):
        self.part += 1
        path = OUT / (f"{self.stem}_{self.part:03d}.csv" if self.chunk else f"{self.stem}.csv")
        self.file = open(path, "w", newline="", encoding="utf-8")
        self.writer = csv.DictWriter(self.file, fieldnames=self.fields, lineterminator="\n", extrasaction="ignore")
        self.writer.writeheader()
        self.files.append(path.name)
        self.rows = 0

    def write(self, row):
        if self.file is None or (self.chunk and self.rows >= CHUNK_ROWS):
            if self.file:
                self.file.close()
            self._open()
        if "predicate" in self.fields:
            row.setdefault("predicate", self.predicate)
        self.writer.writerow(row)
        self.rows += 1
        self.total += 1

    def close(self):
        if self.file:
            self.file.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--orders-per-day", type=int, default=10_000, help="new background orders per trading day")
    parser.add_argument("--accounts", type=int, default=100, help="extra accounts to add (half proprietary, half client)")
    parser.add_argument("--quote-seconds", type=int, default=10, help="seconds between best-bid snapshots per instrument")
    parser.add_argument("--calls-per-day", type=int, default=150, help="extra calls and chats per trading day")
    parser.add_argument("--large-fills", type=int, default=100, help="genuine large equity fills (25,000+ shares) to plant in total")
    parser.add_argument("--seed", type=int, default=2026)
    args = parser.parse_args()

    rng = random.Random(args.seed)
    OUT.mkdir(exist_ok=True)
    for old in OUT.glob("*.csv"):
        old.unlink()

    paul_orders = read("order_events.csv")
    days = sorted({r["timestamp_utc"][:10] for r in paul_orders})
    planted = sorted(parse(r["timestamp_utc"]) for r in paul_orders + read("market_data_top_of_book.csv") if r["scenario_tag"])
    instruments = {r["instrument_id"]: r for r in read("instruments.csv")}
    tick = {i: float(r["tick_size"]) for i, r in instruments.items()}

    def clear_of_planted(moment):
        # Binary search: is any planted event within the blackout either side?
        lo, hi = 0, len(planted)
        while lo < hi:
            mid = (lo + hi) // 2
            if planted[mid] < moment - BLACKOUT:
                lo = mid + 1
            else:
                hi = mid
        return lo == len(planted) or planted[lo] > moment + BLACKOUT

    # ---------------- Reference data: new traders, clients, owners and accounts ----------------
    accounts_out, actors_out, links_out = Writer("accounts.csv"), Writer("actors.csv"), Writer("actor_relationships.csv")
    prop, clients = [], []
    for n in range(1, args.accounts // 2 + 1):
        trader, account = f"T2{n:03d}", f"ACC-4{n:03d}"
        name = f"{rng.choice(FIRST_NAMES)} {rng.choice(LAST_NAMES)}"
        actors_out.write({"subject": trader, "actor_id": trader, "name": name, "actor_type": "Employee",
                          "desk": rng.choice(DESKS), "role": "Trader"})
        accounts_out.write({"subject": account, "account_id": account, "account_name": f"Prop book {n:03d}",
                            "account_type": "Proprietary", "responsible_trader": trader, "client_id": ""})
        links_out.write({"subject": f"AR-1{n:03d}", "rel_id": f"AR-1{n:03d}", "from_actor": trader, "to_actor": account,
                         "rel_type": "TRADES_FOR", "valid_from": "2024-01-02T00:00:00.000000Z",
                         "valid_to": "2099-12-31T00:00:00.000000Z", "provenance": "Declared",
                         "source": "Entitlements system", "confidence": "1.0", "notes": ""})
        prop.append((account, trader))
    for n in range(1, args.accounts - args.accounts // 2 + 1):
        client, owner, account = f"C4{n:03d}", f"BO7{n:03d}", f"ACC-5{n:03d}"
        owner_name = f"{rng.choice(FIRST_NAMES)} {rng.choice(LAST_NAMES)}"
        actors_out.write({"subject": client, "actor_id": client, "name": f"Client {n:03d} Ltd",
                          "actor_type": "Client legal entity", "desk": "", "role": "Institutional client"})
        actors_out.write({"subject": owner, "actor_id": owner, "name": owner_name, "actor_type": "Beneficial owner",
                          "desk": "", "role": f"UBO of Client {n:03d} Ltd"})
        accounts_out.write({"subject": account, "account_id": account, "account_name": f"Client {n:03d} main",
                            "account_type": "Client", "responsible_trader": "", "client_id": client})
        for i, (source, target, kind, origin) in enumerate([
            (client, account, "OWNS", "Client onboarding / KYC"),
            (owner, client, "BENEFICIAL_OWNER_OF", "Client onboarding / KYC"),
        ]):
            rel = f"AR-{2 + i}{n:03d}"
            links_out.write({"subject": rel, "rel_id": rel, "from_actor": source, "to_actor": target, "rel_type": kind,
                             "valid_from": "2024-01-02T00:00:00.000000Z", "valid_to": "2099-12-31T00:00:00.000000Z",
                             "provenance": "Declared", "source": origin, "confidence": "1.0", "notes": ""})
        clients.append((account, client))
    for w in (accounts_out, actors_out, links_out):
        w.close()

    # Paul's accounts trade in the background too, at their usual small sizes.
    paul_accounts = [(r["account_id"], r["responsible_trader"] or None) for r in read("accounts.csv")]
    all_accounts = paul_accounts + [(a, t) for a, t in prop] + [(a, None) for a, _ in clients]
    traders = [r["actor_id"] for r in read("actors.csv") if r["actor_type"] == "Employee"] + [t for _, t in prop]
    client_lines = [(r["actor_id"], f"{r['actor_id']} dealing line") for r in read("actors.csv")
                    if r["actor_type"] == "Client legal entity"] + [(c, f"{c} dealing line") for _, c in clients]

    # ---------------- Prices: a gentle random walk per instrument per day ----------------
    def walk(day_index, instrument):
        drift = rng.gauss(0, 0.004)
        return BASE_PRICES[instrument] * (1 + drift)

    day_mid = {(d, i): walk(n, i) for n, d in enumerate(days) for i in BASE_PRICES}

    def price_at(day, instrument, second):
        # A smooth intraday wiggle around the day's level, rounded to the tick.
        phase = sum(map(ord, instrument)) % 7  # stable per instrument, unlike hash()
        mid = day_mid[(day, instrument)] * (1 + 0.0015 * math.sin(second / 2400 + phase))
        return round(round(mid / tick[instrument]) * tick[instrument], 4)

    def tradeable(day, instrument):
        return instrument != "DWR" or day <= DWR_LAST_DAY

    # ---------------- Orders ----------------
    orders_out = Writer("order_events.csv", chunk=True)
    event_no, order_no = 2_000_000, 500_000
    equity = [i for i in BASE_PRICES if i not in FUTURES]
    large_days = [rng.choice(days) for _ in range(args.large_fills)]

    def order_rows(day_start, day, second, account, trader, instrument, side, qty, outcome):
        nonlocal event_no, order_no
        order_no += 1
        order_id = f"O{order_no}"
        price = price_at(day, instrument, second)
        placed = day_start + timedelta(seconds=second, microseconds=rng.randrange(1_000_000))
        events = [(placed, "NEW", 0, None, qty)]
        later = placed + timedelta(seconds=rng.uniform(0.2, 240))
        if outcome == "fill":
            events.append((later, "FILL", qty, price, 0))
        elif outcome == "partial":
            part = max(1, int(qty * rng.uniform(0.2, 0.8)))
            events.append((later, "PARTIAL_FILL", part, price, qty - part))
            events.append((later + timedelta(seconds=rng.uniform(5, 300)), "CANCEL", 0, None, 0))
        else:
            events.append((later, "CANCEL", 0, None, 0))
        if not all(clear_of_planted(moment) for moment, *_ in events):
            return []
        rows = []
        for moment, kind, fill_qty, fill_price, remaining in events:
            event_no += 1
            rows.append({
                "event_id": f"E{event_no:07d}", "order_id": order_id, "timestamp_utc": ts(moment),
                "venue": "XALF" if instrument in FUTURES else "XALB", "instrument_id": instrument,
                "account_id": account, "trader_id": trader or "", "side": side, "order_type": "LIMIT",
                "limit_price": price, "order_qty": qty, "event_type": kind,
                "fill_qty": fill_qty or "", "fill_price": fill_price or "", "remaining_qty": remaining,
            })
        return rows

    for day in days:
        day_start = datetime.fromisoformat(day).replace(tzinfo=timezone.utc)
        batch = []
        for _ in range(args.orders_per_day):
            account, trader = rng.choice(all_accounts)
            instrument = rng.choice([i for i in BASE_PRICES if tradeable(day, i)])
            if instrument in FUTURES:
                qty = rng.randint(1, 20)
            else:
                qty = int(min(20_000, max(100, rng.lognormvariate(8.3, 0.9))) // 100 * 100)
            roll = rng.random()
            outcome = "fill" if roll < 0.65 else "partial" if roll < 0.85 else "cancel"
            batch += order_rows(day_start, day, rng.uniform(DAY_OPEN, DAY_CLOSE - 400), account, trader,
                                instrument, rng.choice(["BUY", "SELL"]), qty, outcome)
        # Genuine large trades: big, fully filled, and nothing in the futures around them.
        for _ in range(large_days.count(day)):
            account, trader = rng.choice(all_accounts)
            batch += order_rows(day_start, day, rng.uniform(DAY_OPEN, DAY_CLOSE - 400), account, trader,
                                rng.choice([i for i in equity if tradeable(day, i)]), rng.choice(["BUY", "SELL"]),
                                rng.randrange(25_000, 80_000, 500), "fill")
        for row in sorted(batch, key=lambda r: r["timestamp_utc"]):
            orders_out.write(row)
    orders_out.close()

    # ---------------- Best-bid feed ----------------
    quotes_out = Writer("market_data_top_of_book.csv", chunk=True)
    for day in days:
        day_start = datetime.fromisoformat(day).replace(tzinfo=timezone.utc)
        for second in range(DAY_OPEN, DAY_CLOSE, args.quote_seconds):
            moment = day_start + timedelta(seconds=second)
            if not clear_of_planted(moment):
                continue
            for instrument in BASE_PRICES:
                if not tradeable(day, instrument):
                    continue
                bid = price_at(day, instrument, second)
                quotes_out.write({"timestamp_utc": ts(moment), "instrument_id": instrument,
                                  "best_bid": bid, "best_ask": round(bid + tick[instrument], 4)})
    quotes_out.close()

    # ---------------- Calls, transcripts and logins ----------------
    calls_out, lines_out, logins_out = Writer("communications.csv"), Writer("communication_transcripts.csv", chunk=True), Writer("access_logs.csv")
    comm_no, log_no = 100_000, 100_000
    for day in days:
        day_start = datetime.fromisoformat(day).replace(tzinfo=timezone.utc)
        for _ in range(args.calls_per_day):
            caller = rng.choice(traders)
            if rng.random() < 0.6:
                target_id, target = rng.choice(client_lines)
            else:
                target_id, target = "", rng.choice(INTERNAL)
            if caller == "T104" and target_id == "C3107":
                continue  # Kerr's calls to Fenwick are evidence; never add more
            moment = day_start + timedelta(seconds=rng.uniform(DAY_OPEN, DAY_CLOSE))
            if not clear_of_planted(moment):
                continue
            comm_no += 1
            comm_id = f"CM-{comm_no}"
            duration = rng.randint(15, 420)
            calls_out.write({"subject": comm_id, "comm_id": comm_id, "timestamp_utc": ts(moment),
                             "channel": rng.choice(["Recorded work mobile", "Recorded landline", "Approved chat"]),
                             "from_party": caller, "to_party": target, "to_party_id": target_id, "duration_s": duration})
            for line in range(1, rng.randint(2, 5) + 1):
                speaker = caller if line % 2 else (target_id or "DESK")
                lines_out.write({"subject": f"{comm_id}_{line}", "comm_id": comm_id, "evidence_ref": "", "line_no": line,
                                 "offset_s": round((line - 1) * duration / 5, 1), "speaker_id": speaker,
                                 "speaker_name": speaker, "text": rng.choice(LINES),
                                 "stt_confidence": round(rng.uniform(0.84, 0.98), 2)})
        for account, _ in clients:
            log_no += 1
            moment = day_start + timedelta(hours=7, minutes=rng.randint(0, 59), seconds=rng.randint(0, 59))
            logins_out.write({"log_id": f"AL-{log_no}", "timestamp_utc": ts(moment), "account_id": account,
                              "event": "DMA session login", "system": "FIX gateway",
                              "source_ip": f"SYN-IP-10.{rng.randint(0, 255)}.{rng.randint(0, 255)}.{rng.randint(1, 254)}"})
    for w in (calls_out, lines_out, logins_out):
        w.close()

    total = 0
    print(f"Wrote {OUT}/")
    for w in (accounts_out, actors_out, links_out, orders_out, quotes_out, calls_out, lines_out, logins_out):
        total += w.total
        print(f"  {w.stem:32} {w.total:>10,} rows  {', '.join(w.files)}")
    print(f"  {'total':32} {total:>10,} rows")


if __name__ == "__main__":
    main()
