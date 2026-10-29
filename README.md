# Cross-Product, Cross-Actor Spoofing Demo

A market abuse investigation dashboard running live on Black Forest. It checks every large equity sale for layered, cancelled futures orders in a linked future, placed by a linked account in the seconds before, and builds cases with the people, ownership links, calls and logins behind them.

All people, firms, instruments, prices and calls are synthetic. The data is Paul G's demo set in `data/`.

## What you need

- Access to a Black Forest instance, with a user that can create tables and run SQL
- Python 3.10 or later (tested on 3.14)
- Linux, macOS or WSL on Windows

## 1. Load the data into Black Forest

Everything lives in `demoCatalog.demoNamespace`. Each folder holds one JSON file per table, ready to drag and drop into Black Forest:

| Folder | What it is |
|---|---|
| `schemas/` | Column definitions |
| `pix/` | Primary indexes |
| `tables/` | Table definitions |
| `ingest/` | The data, one CSV per table |

Upload the schemas first, then the indexes, then the tables, then the data:

| Table | Schema | Index | Data file | Rows |
|---|---|---|---|---|
| `orderEventsDataTable` | `orderEventsDataSchema` | `orderEventsDataPIX` | `order_events.csv` | 2,500 |
| `marketDataTable` | `marketDataSchema` | `marketPIX` | `market_data_top_of_book.csv` | 133 |
| `instrumentsDataTable` | `instrumentsSchema` | `instrumentsDataPIX` | `instruments.csv` | 9 |
| `instrumentRelationshipsDataTable` | `instrumentRelationshipsSchema` | `instrumentRelationshipsDataIndex` | `instrument_relationships.csv` | 12 |
| `actorsDataTable` | `actorsSchema` | `actorsDataPIX` | `actors.csv` | 10 |
| `actorRelationshipsDataTable` | `actorRelationshipsSchema` | `actorRelationshipsDataPIX` | `actor_relationships.csv` | 14 |
| `accountsDataTable` | `accountsSchema` | `accountsDataPIX` | `accounts.csv` | 8 |
| `commsRefDataTable` | `commsRefSchema` | `commsRefDataPIX` | `communications.csv` | 101 |
| `transcriptsDataTable` | `transcriptsSchema` | `transcriptsDataPIX` | `communication_transcripts.csv` | 342 |
| `accessLogsDataTable` | `accessLogsSchema` | `accessLogsDataPIX` | `access_logs.csv` | 77 |

That is 3,206 rows in total, the figure the dashboard shows in its top right corner.

Check each table's row count after loading. Loading a file twice doubles its rows, and duplicate rows show up in the dashboard as doubled labels in the graph. To check one table:

```sql
SELECT COUNT(*) FROM demoCatalog.demoNamespace.accountsDataTable
```

The files in `ingest/` are made from Paul's originals in `data/` by `prepare_ingest.py`. You only need to run it again if the originals change.

## 2. Set up the connection

```bash
cp .env.example .env
```

Fill in `.env` with your Black Forest address and login:

```
BF_HOST=https://your-black-forest-host
BF_USER=your-user
BF_PASS=your-password
```

`.env` is never committed. Keep your login out of every other file.

## 3. Start the dashboard

```bash
./start.sh
```

The first run creates a Python environment in `.venv` and installs the pinned packages from `requirements.txt`. Open http://localhost:8000 in a browser.

The first start runs the full detection against Black Forest, which takes a minute or two. The result is saved to `detection_run.pkl`, so later starts open straight away. Use **Run again** on the Metrics page for a fresh run.

## 4. Check it worked

The dashboard should open on Alerts with **7 alerts** across **2 cases**:

- CCC-2026-0141: same account, 2 episodes
- CCC-2026-0142: cross-actor, 5 episodes

To check the detection without a browser, run the test. It reads the CSVs locally, so it needs no Black Forest connection:

```bash
.venv/bin/python tests/test_detection.py
```

It should end with `OK: alerts match the answer key`.

## The pages

| Page | What it shows |
|---|---|
| Alerts | Every large sale checked, when the alerts happened and how big they were |
| Cases | The episodes, price pressure charts, calls, transcripts and logins for each case |
| Graph explorer | The knowledge graph of people, accounts and instruments, with every link and its source |
| Metrics | The detection steps and every Black Forest query, with timings and an exportable audit trail |

## Optional extras

### Use Claude to investigate (MCP server)

`mcp_server.py` gives Claude Desktop or Claude Code the same detection and Black Forest queries as tools. It reads the same `.env`:

```bash
./run_mcp.sh          # for Claude Desktop or Claude Code
./run_mcp.sh --http   # over the network on port 8002
```

### Show detection at scale

`scale_data.py` generates about 1.3 million rows of innocent background trading to load on top of Paul's data. The dashboard then finds the same 7 alerts in a much larger dataset.

```bash
.venv/bin/python scale_data.py
```

It writes the extra rows to `ingest_scale/`. Upload them into the existing tables after the normal data. They are not in the repository because they take 77 MB, and the script makes the same rows every time.

### Preview without Black Forest

To check a layout change without a connection:

```bash
.venv/bin/python tests/serve_from_csv.py
```

This serves the dashboard at http://localhost:8010 from the local CSVs. The query times it shows are local file reads, not Black Forest timings, so never use it for the demo itself.

## Folder layout

```
server.py            detection, cases and the dashboard's API
index.html           the dashboard
mcp_server.py        MCP tools for Claude
start.sh             starts the dashboard
run_mcp.sh           starts the MCP server
requirements.txt     pinned Python packages
data/                Paul's original demo data and answer key
ingest/              the same data, ready to load into Black Forest
schemas/ pix/ tables/  Black Forest definitions to drag and drop
prepare_ingest.py    rebuilds ingest/ from data/
scale_data.py        generates the background data for the scale demo
static/              fonts and logo used by the dashboard
Brand Assets/        Craxel brand files
tests/               detection test, MCP tools test and the CSV preview server
```
