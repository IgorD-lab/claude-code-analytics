# Claude Code Usage Analytics Platform

Analyse Claude Code telemetry across an engineering organisation. Ingests raw OpenTelemetry logs and employee data into SQLite, then surfaces insights through an interactive Streamlit dashboard and a typed REST API.

## Quick Start

**1. Clone and install**

```bash
git clone https://github.com/IgorD-lab/claude-code-analytics.git && cd claude-code-analytics
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt        # fastapi, uvicorn, pandas, plotly, pydantic, streamlit
```

**2. Generate sample data**

```bash
python3 data/generate_fake_data.py --num-users 100 --num-sessions 5000 --days 60 --output-dir data/raw
```

**3. Ingest data into SQLite database.**

```bash
python src/data_ingestion.py
```

**4. Run the dashboard**

```bash
streamlit run src/dashboard.py
```

**5. Run the REST API** _(separate terminal)_

```bash
uvicorn src.api:app --reload
# Raw        → http://localhost:8000
# Swagger UI → http://localhost:8000/docs
# ReDoc      → http://localhost:8000/redoc
```

## Data Pipeline

1. **Raw data** — `data/raw/telemetry_logs.jsonl` (nested log batches) and `data/raw/employees.csv`
2. **Ingestion** — `src/data_ingestion.py` flattens the nested JSONL, coerces types, runs integrity checks, and loads everything into SQLite with three query indexes
3. **Database** — `data/processed/telemetry.db` with an `events` table (454k rows, five event types) and an `employees` table joined on email
4. **Analytics** — `src/analytics.py` exposes ten query functions, each returning a pandas DataFrame, shared by both the dashboard and the API
5. **Dashboard** — `src/dashboard.py` renders a four-tab Streamlit app with sidebar filters
6. **API** — `src/api.py` exposes all analytics as typed REST endpoints with the same filter set

Re-running ingestion is safe — duplicates are skipped via `INSERT OR IGNORE`.

## Structure

```
data/raw/               Raw JSONL and CSV inputs
data/processed/         SQLite database (generated)
src/
  data_ingestion.py     ETL pipeline with validation and indexing
  analytics.py          Query functions returning DataFrames (shared layer)
  dashboard.py          Streamlit dashboard
  api.py                FastAPI REST API
  models.py             Pydantic response models and filter enums
scripts/                Standalone validation and inspection scripts
docs/                   Schema design notes and LLM usage log
```

## Dashboard

Four tabs, all responding to the sidebar filters (date range, practice, engineer level):

| Tab                      | Contents                                                                   |
| ------------------------ | -------------------------------------------------------------------------- |
| **Cost Overview**        | Cost by practice and model, model efficiency table, daily cost time series |
| **Usage Patterns**       | Events by hour and day of week, tool approval rates                        |
| **Team & User Insights** | Top-N users table (configurable), session count and duration by practice   |
| **Token Analysis**       | Token breakdown by model and practice, cost per 1K tokens, cache hit ratio |

## REST API

Programmatic access to all analytics with the same filters as the dashboard: `date_from`, `date_to`, `practice`, `level`, `location`. Filter values are validated against enums — invalid inputs return 422. Interactive docs at `/docs` (Swagger UI) and `/redoc` (ReDoc).

| Endpoint                                  | Description                                                  |
| ----------------------------------------- | ------------------------------------------------------------ |
| `GET /api/summary`                        | Metric cards: total events, API cost, unique users, sessions |
| `GET /api/filters`                        | Available filter values and date range                       |
| `GET /api/cost-by-practice`               | Total cost and token usage by practice team                  |
| `GET /api/cost-by-model`                  | Cost, token usage, and cost share % by model                 |
| `GET /api/daily-cost`                     | Daily cost time series with zero-filled gaps                 |
| `GET /api/model-efficiency`               | Request share, cost share, avg cost per request by model     |
| `GET /api/cost-per-token`                 | Cost per 1,000 tokens (input + output) by model              |
| `GET /api/usage-by-hour`                  | Event count by UTC hour (all 24 hours always present)        |
| `GET /api/usage-by-day-of-week`           | Event count by day of week (all 7 days always present)       |
| `GET /api/tool-usage`                     | Tool decision counts and approval rates                      |
| `GET /api/top-users`                      | Top N users by spend, enriched with employee details         |
| `GET /api/sessions-by-practice`           | Session count and avg/max duration by practice               |
| `GET /api/requests-by-model-and-practice` | API request count for every (practice × model) combination   |
| `GET /api/cache-stats`                    | Cache read/creation tokens and hit ratio % by model          |

## Bonus Features

### API Access

All dashboard analytics are available as filtered REST endpoints with automatic request validation and interactive documentation at `/docs` and `/redoc`.

### Advanced Statistical Analysis

- **Model cost-efficiency comparison** — cost share vs request share identifies models that consume disproportionate budget relative to their usage volume
- **Cost-per-token normalisation** — divides total spend by input + output tokens to compare true pricing efficiency across models with different usage volumes
- **Token I/O ratio analysis** — reveals a two-tier usage strategy: Haiku as a triage layer (≈8:1 input/output ratio) vs Opus as a generation layer (≈1:1.4 ratio)
- **Cache hit ratio analysis** — per-model breakdown of cache read vs creation tokens quantifies prompt-prefix reuse efficiency
- **Cross-dimensional model usage** — request counts across every practice × model combination surface team-specific adoption patterns

## Technologies

|               |                                       |
| ------------- | ------------------------------------- |
| Storage       | SQLite 3                              |
| Processing    | Python 3.11-3.13, pandas 3.0              |
| Visualisation | Plotly 6, Streamlit 1.56              |
| API           | FastAPI 0.115, Pydantic 2.11, uvicorn |

## Database Documentation

- **[docs/schema_design.md](docs/schema_design.md)** — Single-table vs split-table schema trade-offs, rationale, and full DDL

## LLM Usage

Built using Claude.ai (chat) for planning and architecture decisions, and Claude Code (CLI) for code generation. Key examples:

- **Data exploration:** Prompted Claude Code to flatten nested JSONL and run validation checks (nulls, duplicates, timestamp ranges). Verified output by cross-referencing with manual JSON inspection.
- **Schema design:** Evaluated single-table vs split-table approaches with Claude Code. Chose single table based on cross-type query needs.
- **Bug discovery:** Dashboard showed incorrect daily costs. Manual SQL verification revealed a column alias collision in SQLite. Also found a timestamp formatting inconsistency causing silent data loss of 5,445 rows in pandas aggregations.

All AI-generated code was validated through manual SQL queries, DB Browser inspection, and output verification against expected values.

Full log: **[docs/llm_usage_log.md](docs/llm_usage_log.md)**
