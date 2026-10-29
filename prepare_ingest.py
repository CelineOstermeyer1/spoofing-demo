"""Prepare Paul's CSVs for Black Forest ingest.

Reads the originals from data/ and writes upload-ready copies to ingest/.
Paul's files are never modified, so this can be rerun at any time.

  - every table: drops the demo-only columns (scenario_tag, flagged,
    flag_reason) and writes plain LF line endings
  - reference tables: adds subject (the table's primary key) and predicate
  - relationship tables: turns valid_from / valid_to dates into full UTC
    timestamps and fills blank valid_to with 2099-12-31 (still valid)
  - communications: adds to_party_id ("C3107 dealing line" -> "C3107",
    internal desks left blank)

answer_key.csv is deliberately left out: it is the marking sheet, not data.
"""

import csv
from pathlib import Path

HERE = Path(__file__).parent
SRC = HERE / "data"
OUT = HERE / "ingest"

DEMO_ONLY = {"scenario_tag", "flagged", "flag_reason"}
OPEN_ENDED = "2099-12-31"

# file: (subject column(s), predicate schema name); None = event table
TABLES = {
    "instrument_relationships.csv": ("rel_id", "instrumentRelationshipsPredicateSchema"),
    "actor_relationships.csv": ("rel_id", "actorRelationshipsDataPredicateSchema"),
    "instruments.csv": ("instrument_id", "instrumentsDataPredicateSchema"),
    "actors.csv": ("actor_id", "actorsDataPredicateSchema"),
    "accounts.csv": ("account_id", "accountsDataPredicateSchema"),
    "order_events.csv": None,
    "market_data_top_of_book.csv": None,
    # Reference style: comm_id lookups failed when it was part of the event key.
    "communications.csv": ("comm_id", "commsRefPredicateSchema"),
    # Reference style: comm_id lookups failed when it was part of a two-field key.
    "communication_transcripts.csv": (("comm_id", "line_no"), "commTranscriptsDataPredicateSchema"),
    "access_logs.csv": None,
}


def to_timestamp(date):
    return f"{date}T00:00:00.000000Z"


def to_party_id(label):
    # Client lines start with the client's actor_id; internal desks have none.
    first = label.split()[0] if label else ""
    return first if first.startswith("C") and first[1:].isdigit() else ""


def main():
    OUT.mkdir(exist_ok=True)
    for name, reference in TABLES.items():
        with open(SRC / name, newline="", encoding="utf-8-sig") as f:
            rows = list(csv.DictReader(f))

        fields = [k for k in rows[0] if k not in DEMO_ONLY]

        if "valid_from" in fields:
            for row in rows:
                row["valid_from"] = to_timestamp(row["valid_from"])
                row["valid_to"] = to_timestamp(row["valid_to"] or OPEN_ENDED)

        if name == "communications.csv":
            fields.insert(fields.index("to_party") + 1, "to_party_id")
            for row in rows:
                row["to_party_id"] = to_party_id(row["to_party"])

        if reference:
            key, predicate = reference
            fields = ["subject", "predicate"] + fields
            for row in rows:
                row["subject"] = "_".join(row[k] for k in key) if isinstance(key, tuple) else row[key]
                row["predicate"] = predicate

        with open(OUT / name, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fields, lineterminator="\n", extrasaction="ignore")
            writer.writeheader()
            writer.writerows(rows)

        print(f"{name:32} {len(rows):5} rows")


if __name__ == "__main__":
    main()
