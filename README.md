# Claude Code Usage Analytics Platform

A data pipeline and interactive dashboard for analysing Claude Code telemetry across an engineering organisation. Ingests raw OpenTelemetry logs and employee data, stores them in a structured SQLite database, and exposes insights through a Streamlit dashboard.

---

## Architecture

```
data/raw/
  telemetry_logs.jsonl   ─┐
  employees.csv           ─┤─► src/data_ingestion.py ─► data/processed/telemetry.db
                                  │                              │
                                  │  • flattens nested JSONL     │  events table (454k rows)
                                  │  • type coercion             │  employees table (100 rows)
                                  │  • post-load integrity checks│  3 query indexes
                                  │  • creates indexes           │
                                  └──────────────────────────────┘
                                                                  │
                                          src/analytics.py ◄──────┘
                                          7 query functions, each returns a DataFrame
                                                                  │
                                          src/dashboard.py ◄──────┘
                                          Streamlit app, 4 tabs, sidebar filters
```

Each line of `telemetry_logs.jsonl` is a JSON envelope containing a `logEvents` array. Each event's `message` field is a nested JSON string that is parsed again during ingestion. Five event types are emitted: `api_request`, `user_prompt`, `tool_decision`, `tool_result`, `api_error`.

---

## Folder Structure

```
├── data/
│   ├── raw/
│   │   ├── telemetry_logs.jsonl   # Raw OTel log batches
│   │   └── employees.csv          # Employee directory (email, name, practice, level, location)
│   ├── processed/
│   │   └── telemetry.db           # SQLite database (created by ingestion)
│   └── generate_fake_data.py      # Data generator used to produce the sample dataset
│
├── src/
│   ├── data_ingestion.py          # ETL pipeline: parse → flatten → validate → load → index
│   ├── analytics.py               # Query functions returning pandas DataFrames
│   └── dashboard.py               # Streamlit dashboard
│
├── scripts/
│   ├── validate_telemetry.py      # Pre-ingestion JSONL validation (nulls, duplicates, ranges)
│   ├── sample_event_types.py      # Prints one example row per event type
│   ├── validate_batch_fields.py   # Checks outer envelope fields (messageType, owner, etc.)
│   └── validate_joins_and_types.py# Post-ingestion integrity checks on the SQLite DB
│
└── docs/
    ├── schema_design.md           # Schema design decisions and trade-offs
    └── llm_usage_log.md           # Log of AI tool usage throughout the project
```

---

## Setup

**Requirements:** Python 3.11+

```bash
# 1. Clone and enter the project
git clone <repo-url>
cd claude-code-analytics

# 2. Create and activate a virtual environment
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate

# 3. Install dependencies
pip install pandas plotly streamlit

# 4. Run the ingestion pipeline
python src/data_ingestion.py
```

The ingestion script will print a summary including row counts, integrity check results, and index creation status. Re-running is safe — duplicate rows are skipped via `INSERT OR IGNORE`.

```bash
# 5. Launch the dashboard
streamlit run src/dashboard.py
```

Open [http://localhost:8501](http://localhost:8501) in your browser.

---

## Validation Scripts

Run these independently to inspect data quality before or after ingestion:

```bash
python scripts/validate_telemetry.py        # Null counts, duplicates, timestamp range, event types
python scripts/sample_event_types.py        # One example row per event type
python scripts/validate_batch_fields.py     # Envelope-level field uniqueness checks
python scripts/validate_joins_and_types.py  # Join coverage and negative value checks on the DB
```

---

## Dashboard

Four tabs, all filtered by date range, practice, and engineer level from the sidebar:

| Tab | Contents |
|---|---|
| **Cost Overview** | Cost by practice, cost by model, model efficiency table, daily cost time series |
| **Usage Patterns** | Events by hour, events by day of week, tool approval rates |
| **Team & User Insights** | Top-N users table (configurable), session count and duration by practice |
| **Token Analysis** | Input vs output tokens by model, cost per 1K tokens, model usage by practice, cache hit ratio |

---

## Technologies

| Component | Technology |
|---|---|
| Data storage | SQLite 3 (via Python `sqlite3`) |
| Data processing | pandas 3.0 |
| Visualisation | Plotly 6, Streamlit 1.56 |
| Language | Python 3.11+ |

---

## Documentation

- **[docs/schema_design.md](docs/schema_design.md)** — Documents the single-table schema design, compares it against a split-table alternative, explains the rationale for the chosen approach, and includes the full `CREATE TABLE` and `CREATE INDEX` statements.

- **[docs/llm_usage_log.md](docs/llm_usage_log.md)** — Chronological log of how AI tools (Claude.ai and Claude Code) were used throughout the project: requirements analysis, architecture decisions, code generation, debugging, and documentation.
