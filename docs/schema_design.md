# Schema Design: telemetry.db

## Current Schema

### `events` table (454,428 rows)

| Column                   | Type    | Notes                                                   |
| ------------------------ | ------- | ------------------------------------------------------- |
| `event_id`               | TEXT    | Primary key                                             |
| `event_timestamp_ms`     | INTEGER | Unix epoch in milliseconds                              |
| `event_timestamp`        | TEXT    | ISO-8601 UTC datetime string                            |
| `body`                   | TEXT    | Event type discriminator — see values below             |
| `log_group`              | TEXT    | Always `/claude-code/telemetry`                         |
| `log_stream`             | TEXT    | Always `otel-collector`                                 |
| `year`                   | INTEGER | Partition hint from the envelope                        |
| `month`                  | INTEGER |                                                         |
| `day`                    | INTEGER |                                                         |
| `organization_id`        | TEXT    |                                                         |
| `session_id`             | TEXT    | Groups events within one Claude Code session            |
| `terminal_type`          | TEXT    | e.g. `vscode`                                           |
| `user_account_uuid`      | TEXT    |                                                         |
| `user_email`             | TEXT    | Join key → `employees.email`                            |
| `user_id`                | TEXT    | Hashed user identifier                                  |
| `event_name`             | TEXT    | Mirrors `body` without the `claude_code.` prefix        |
| `prompt`                 | TEXT    | **`user_prompt` only** — always `<REDACTED>`            |
| `prompt_length`          | INTEGER | **`user_prompt` only**                                  |
| `cache_creation_tokens`  | INTEGER | **`api_request` only**                                  |
| `cache_read_tokens`      | INTEGER | **`api_request` only**                                  |
| `cost_usd`               | REAL    | **`api_request` only**                                  |
| `duration_ms`            | INTEGER | **`api_request`, `api_error`, `tool_result`**           |
| `input_tokens`           | INTEGER | **`api_request` only**                                  |
| `model`                  | TEXT    | **`api_request`, `api_error`**                          |
| `output_tokens`          | INTEGER | **`api_request` only**                                  |
| `decision`               | TEXT    | **`tool_decision` only** — `accept` or `reject`         |
| `source`                 | TEXT    | **`tool_decision` only** — e.g. `config`, `user_reject` |
| `tool_name`              | TEXT    | **`tool_decision`, `tool_result`**                      |
| `scope_name`             | TEXT    | OTel instrumentation scope                              |
| `scope_version`          | TEXT    |                                                         |
| `host_arch`              | TEXT    | e.g. `x86_64`, `arm64`                                  |
| `host_name`              | TEXT    |                                                         |
| `os_type`                | TEXT    | e.g. `darwin`, `linux`                                  |
| `os_version`             | TEXT    |                                                         |
| `service_name`           | TEXT    |                                                         |
| `service_version`        | TEXT    |                                                         |
| `resource_user_practice` | TEXT    | Team/practice from OTel resource block                  |
| `resource_user_profile`  | TEXT    |                                                         |
| `resource_user_serial`   | TEXT    | Hardware serial number                                  |

**Event type distribution:**

| `body` value                | Row count | Populated fields (beyond shared)                 |
| --------------------------- | --------- | ------------------------------------------------ |
| `claude_code.tool_decision` | 151,461   | `decision`, `source`, `tool_name`                |
| `claude_code.tool_result`   | 148,418   | `duration_ms`, `tool_name`                       |
| `claude_code.api_request`   | 118,014   | token fields, `cost_usd`, `model`, `duration_ms` |
| `claude_code.user_prompt`   | 35,173    | `prompt`, `prompt_length`                        |
| `claude_code.api_error`     | 1,362     | `model`, `duration_ms`                           |

### `employees` table (100 rows)

| Column      | Type | Notes                                          |
| ----------- | ---- | ---------------------------------------------- |
| `email`     | TEXT | Primary key, join key from `events.user_email` |
| `full_name` | TEXT |                                                |
| `practice`  | TEXT | Engineering team                               |
| `level`     | TEXT | e.g. `L3`, `L7`                                |
| `location`  | TEXT |                                                |

---

## Option A: Single `events` Table (current)

One table holds all event types. Event-specific columns are nullable for rows where they don't apply.

**Pros:**

- Simple ingestion: one `INSERT` path regardless of event type.
- Trivial cross-type queries — session timelines, per-user event counts, hourly distributions — need no joins.
- Schema migrations are a single `ALTER TABLE`.
- `event_id` primary key prevents duplicates from re-ingestion automatically.
- SQLite handles sparse rows (many NULLs) efficiently via its dynamic column storage.

**Cons:**

- 13 of 39 columns are event-type-specific; a reader can't know which fields are meaningful without checking `body`.
- No database-level enforcement that `cost_usd` is non-null on `api_request` rows — only application logic guarantees it.
- Queries that join to `employees` and aggregate cost will silently mix event types unless filtered with `WHERE body = 'claude_code.api_request'`.
- Column count grows linearly as new event types are added.

---

## Option B: Separate Table Per Event Type

A shared `sessions` or `event_base` table holds the common fields. Five typed tables hold event-specific columns and reference it.

**Pros:**

- Schema is self-documenting: the table name tells you exactly which fields are valid.
- Foreign key constraints enforce referential integrity between event tables and a shared base.
- Easier to extend: adding a new event type is a new table, not a wider existing one.
- Typed tables enable stricter `NOT NULL` constraints on required fields.

**Cons:**

- Cross-type queries (e.g. full session timeline, events-per-hour across all types) require `UNION ALL` across five tables or a view — more complex and slower to iterate on.
- Five ingestion paths instead of one; the dispatcher logic must be kept in sync with the schema.
- Exploratory queries during development are significantly more verbose.
- SQLite has no partial indexes or table inheritance, so the common fields (`session_id`, `user_email`, etc.) are either duplicated in every table or pushed into a separate base table requiring a join for every query.
- The dashboard and API will almost always need data from multiple event types in the same request — Option B makes those the expensive case.

---

## Recommendation: Keep Option A

**Option A is the better fit for this project.**

The primary consumers are a Streamlit dashboard and FastAPI endpoints. Both require frequent cross-type aggregations: cost over time (only `api_request`), session timelines (all types), tool accept rates (`tool_decision`), user activity (all types). With Option B, every one of these needs either a `UNION ALL` or a multi-table join, adding query complexity with no analytical benefit.

The practical cost of Option A's nullable columns is low. SQLite does not allocate storage for NULL values in a row, so the sparse columns carry negligible overhead. The validation checks already confirm there are no unexpected nulls in the fields that matter. The `body` column serves as a reliable discriminator, and the queries in `analytics.py` already filter on it correctly.

Option B would make sense if the event types were written by separate teams, had conflicting field names, or if the tables needed independent access control. None of those apply here.

One targeted improvement over the current schema: add a filtered index on `body` + `event_timestamp` to accelerate the time-series and per-type queries that the dashboard will run most often.

---
