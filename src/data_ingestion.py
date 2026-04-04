"""
Ingest telemetry_logs.jsonl and employees.csv into a SQLite database.

Outputs:
  data/processed/telemetry.db
    - events    : one row per log event, all attributes flattened
    - employees : one row per employee from employees.csv
"""

import csv
import json
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).parent.parent
JSONL_PATH = ROOT / "data" / "raw" / "telemetry_logs.jsonl"
CSV_PATH = ROOT / "data" / "raw" / "employees.csv"
DB_PATH = ROOT / "data" / "processed" / "telemetry.db"

# ── Type coercion helpers ────────────────────────────────────────────────────

FLOAT_FIELDS = {"cost_usd"}
INT_FIELDS = {
    "duration_ms", "input_tokens", "output_tokens",
    "cache_creation_tokens", "cache_read_tokens", "prompt_length",
}

def coerce_numeric(key: str, value):
    """Convert string numbers to int or float; return None on failure."""
    if value is None or value == "":
        return None
    try:
        if key in FLOAT_FIELDS:
            return float(value)
        if key in INT_FIELDS:
            return int(value)
    except (ValueError, TypeError):
        return None
    return value


def parse_event_timestamp(value: str | None) -> str | None:
    """Parse ISO-8601 timestamp string → UTC datetime stored as ISO string."""
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return dt.astimezone(timezone.utc).isoformat()
    except (ValueError, TypeError):
        return None


# ── Event flattening ─────────────────────────────────────────────────────────

def flatten_event(envelope: dict, raw_event: dict) -> dict | None:
    """Return one flat dict for a single log event, or None on parse error."""
    try:
        msg = json.loads(raw_event["message"])
    except (KeyError, json.JSONDecodeError, TypeError):
        return None

    attrs = msg.get("attributes") or {}
    scope = msg.get("scope") or {}
    resource = msg.get("resource") or {}

    row: dict = {
        # Envelope fields
        "log_group":  envelope.get("logGroup"),
        "log_stream": envelope.get("logStream"),
        "year":       envelope.get("year"),
        "month":      envelope.get("month"),
        "day":        envelope.get("day"),
        # Raw event fields
        "event_id":            raw_event.get("id"),
        "event_timestamp_ms":  raw_event.get("timestamp"),
        # Parsed message
        "body":        msg.get("body"),
        # Common attributes
        "event_timestamp":   parse_event_timestamp(attrs.get("event.timestamp")),
        "organization_id":   attrs.get("organization.id"),
        "session_id":        attrs.get("session.id"),
        "terminal_type":     attrs.get("terminal.type"),
        "user_account_uuid": attrs.get("user.account_uuid"),
        "user_email":        attrs.get("user.email"),
        "user_id":           attrs.get("user.id"),
        "event_name":        attrs.get("event.name"),
        # user_prompt fields
        "prompt":        attrs.get("prompt"),
        "prompt_length": coerce_numeric("prompt_length", attrs.get("prompt_length")),
        # api_request / api_error fields
        "cache_creation_tokens": coerce_numeric("cache_creation_tokens", attrs.get("cache_creation_tokens")),
        "cache_read_tokens":     coerce_numeric("cache_read_tokens",     attrs.get("cache_read_tokens")),
        "cost_usd":              coerce_numeric("cost_usd",              attrs.get("cost_usd")),
        "duration_ms":           coerce_numeric("duration_ms",           attrs.get("duration_ms")),
        "input_tokens":          coerce_numeric("input_tokens",          attrs.get("input_tokens")),
        "model":                 attrs.get("model"),
        "output_tokens":         coerce_numeric("output_tokens",         attrs.get("output_tokens")),
        # tool_decision fields
        "decision":  attrs.get("decision"),
        "source":    attrs.get("source"),
        "tool_name": attrs.get("tool_name"),
        # Scope
        "scope_name":    scope.get("name"),
        "scope_version": scope.get("version"),
        # Resource
        "host_arch":              resource.get("host.arch"),
        "host_name":              resource.get("host.name"),
        "os_type":                resource.get("os.type"),
        "os_version":             resource.get("os.version"),
        "service_name":           resource.get("service.name"),
        "service_version":        resource.get("service.version"),
        "resource_user_practice": resource.get("user.practice"),
        "resource_user_profile":  resource.get("user.profile"),
        "resource_user_serial":   resource.get("user.serial"),
    }
    return row


# ── Database setup ────────────────────────────────────────────────────────────

EVENTS_DDL = """
CREATE TABLE IF NOT EXISTS events (
    event_id                TEXT PRIMARY KEY,
    event_timestamp_ms      INTEGER,
    event_timestamp         TEXT,
    body                    TEXT,
    log_group               TEXT,
    log_stream              TEXT,
    year                    INTEGER,
    month                   INTEGER,
    day                     INTEGER,
    organization_id         TEXT,
    session_id              TEXT,
    terminal_type           TEXT,
    user_account_uuid       TEXT,
    user_email              TEXT,
    user_id                 TEXT,
    event_name              TEXT,
    prompt                  TEXT,
    prompt_length           INTEGER,
    cache_creation_tokens   INTEGER,
    cache_read_tokens       INTEGER,
    cost_usd                REAL,
    duration_ms             INTEGER,
    input_tokens            INTEGER,
    model                   TEXT,
    output_tokens           INTEGER,
    decision                TEXT,
    source                  TEXT,
    tool_name               TEXT,
    scope_name              TEXT,
    scope_version           TEXT,
    host_arch               TEXT,
    host_name               TEXT,
    os_type                 TEXT,
    os_version              TEXT,
    service_name            TEXT,
    service_version         TEXT,
    resource_user_practice  TEXT,
    resource_user_profile   TEXT,
    resource_user_serial    TEXT
)
"""

EMPLOYEES_DDL = """
CREATE TABLE IF NOT EXISTS employees (
    email       TEXT PRIMARY KEY,
    full_name   TEXT,
    practice    TEXT,
    level       TEXT,
    location    TEXT
)
"""

EVENTS_INSERT = """
INSERT OR IGNORE INTO events VALUES (
    :event_id, :event_timestamp_ms, :event_timestamp, :body,
    :log_group, :log_stream, :year, :month, :day,
    :organization_id, :session_id, :terminal_type,
    :user_account_uuid, :user_email, :user_id, :event_name,
    :prompt, :prompt_length,
    :cache_creation_tokens, :cache_read_tokens, :cost_usd,
    :duration_ms, :input_tokens, :model, :output_tokens,
    :decision, :source, :tool_name,
    :scope_name, :scope_version,
    :host_arch, :host_name, :os_type, :os_version,
    :service_name, :service_version,
    :resource_user_practice, :resource_user_profile, :resource_user_serial
)
"""

EMPLOYEES_INSERT = """
INSERT OR IGNORE INTO employees (email, full_name, practice, level, location)
VALUES (:email, :full_name, :practice, :level, :location)
"""


# ── Ingestion routines ────────────────────────────────────────────────────────

BATCH_SIZE = 5_000

def ingest_events(conn: sqlite3.Connection) -> dict:
    envelopes_read = 0
    rows_attempted = 0
    rows_inserted = 0
    parse_errors = 0

    before = conn.execute("SELECT COUNT(*) FROM events").fetchone()[0]

    with JSONL_PATH.open("r", encoding="utf-8") as f:
        batch: list[dict] = []

        for lineno, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                envelope = json.loads(line)
            except json.JSONDecodeError as exc:
                print(f"  [WARN] Line {lineno}: envelope parse error — {exc}", file=sys.stderr)
                parse_errors += 1
                continue

            envelopes_read += 1
            for raw_event in envelope.get("logEvents") or []:
                row = flatten_event(envelope, raw_event)
                if row is None:
                    parse_errors += 1
                    continue
                rows_attempted += 1
                batch.append(row)

                if len(batch) >= BATCH_SIZE:
                    conn.executemany(EVENTS_INSERT, batch)
                    batch.clear()

        if batch:
            conn.executemany(EVENTS_INSERT, batch)

    conn.commit()
    after = conn.execute("SELECT COUNT(*) FROM events").fetchone()[0]
    rows_inserted = after - before
    return {
        "envelopes_read": envelopes_read,
        "rows_attempted": rows_attempted,
        "rows_inserted": rows_inserted,
        "parse_errors": parse_errors,
    }


def ingest_employees(conn: sqlite3.Connection) -> dict:
    rows_attempted = 0
    rows_inserted = 0
    parse_errors = 0

    before = conn.execute("SELECT COUNT(*) FROM employees").fetchone()[0]

    with CSV_PATH.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        batch: list[dict] = []

        for row in reader:
            if not row.get("email"):
                parse_errors += 1
                continue
            rows_attempted += 1
            batch.append(row)

            if len(batch) >= BATCH_SIZE:
                conn.executemany(EMPLOYEES_INSERT, batch)
                batch.clear()

        if batch:
            conn.executemany(EMPLOYEES_INSERT, batch)

    conn.commit()
    after = conn.execute("SELECT COUNT(*) FROM employees").fetchone()[0]
    rows_inserted = after - before
    return {
        "rows_attempted": rows_attempted,
        "rows_inserted": rows_inserted,
        "parse_errors": parse_errors,
    }


# ── Post-load integrity checks ────────────────────────────────────────────────
# Runs after every load so data quality issues surface immediately rather than
# silently corrupting dashboard metrics or API responses.

def run_integrity_checks(conn: sqlite3.Connection) -> dict:
    """Run join and value sanity checks; return a results dict."""

    null_email = conn.execute(
        "SELECT COUNT(*) FROM events WHERE user_email IS NULL OR user_email = ''"
    ).fetchone()[0]

    unmatched_event_emails = conn.execute("""
        SELECT COUNT(DISTINCT e.user_email)
        FROM events e
        LEFT JOIN employees emp ON e.user_email = emp.email
        WHERE emp.email IS NULL
          AND e.user_email IS NOT NULL AND e.user_email != ''
    """).fetchone()[0]

    unmatched_employee_emails = conn.execute("""
        SELECT COUNT(DISTINCT emp.email)
        FROM employees emp
        LEFT JOIN events e ON emp.email = e.user_email
        WHERE e.user_email IS NULL
    """).fetchone()[0]

    negative_checks = {
        "cost_usd (api_request)":     conn.execute(
            "SELECT COUNT(*) FROM events WHERE body='claude_code.api_request' AND cost_usd < 0"
        ).fetchone()[0],
        "duration_ms (api_request)":  conn.execute(
            "SELECT COUNT(*) FROM events WHERE body='claude_code.api_request' AND duration_ms < 0"
        ).fetchone()[0],
        "input_tokens (api_request)": conn.execute(
            "SELECT COUNT(*) FROM events WHERE body='claude_code.api_request' AND input_tokens < 0"
        ).fetchone()[0],
        "output_tokens (api_request)": conn.execute(
            "SELECT COUNT(*) FROM events WHERE body='claude_code.api_request' AND output_tokens < 0"
        ).fetchone()[0],
    }

    return {
        "null_email": null_email,
        "unmatched_event_emails": unmatched_event_emails,
        "unmatched_employee_emails": unmatched_employee_emails,
        "negative_checks": negative_checks,
    }


# ── Index creation ────────────────────────────────────────────────────────────
# The dashboard and API hit three query patterns constantly: filtering by event
# type + time (cost charts), joining on user_email (user-level aggregations),
# and grouping sessions by time (session duration). Without indexes SQLite
# does a full table scan on 450k rows for each. These three cover all three.

INDEXES = [
    ("idx_events_body_ts",   "CREATE INDEX IF NOT EXISTS idx_events_body_ts ON events (body, event_timestamp)"),
    ("idx_events_user_email","CREATE INDEX IF NOT EXISTS idx_events_user_email ON events (user_email)"),
    ("idx_events_session",   "CREATE INDEX IF NOT EXISTS idx_events_session ON events (session_id, event_timestamp_ms)"),
]

def create_indexes(conn: sqlite3.Connection) -> list[str]:
    """Create indexes if they don't exist; return list of names created."""
    existing = {
        row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='index'")
    }
    created = []
    for name, ddl in INDEXES:
        conn.execute(ddl)
        if name not in existing:
            created.append(name)
    conn.commit()
    return created


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    for path, label in [(JSONL_PATH, "JSONL"), (CSV_PATH, "CSV")]:
        if not path.exists():
            print(f"ERROR: {label} file not found: {path}", file=sys.stderr)
            sys.exit(1)

    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    print(f"Connecting to database: {DB_PATH}")
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute(EVENTS_DDL)
    conn.execute(EMPLOYEES_DDL)
    conn.commit()

    print("\nIngesting events from JSONL …")
    ev = ingest_events(conn)

    print("Ingesting employees from CSV …")
    em = ingest_employees(conn)

    print("Running integrity checks …")
    ic = run_integrity_checks(conn)

    print("Creating indexes …")
    created_indexes = create_indexes(conn)

    conn.close()

    w = 48
    print(f"\n{'═'*w}")
    print("  INGESTION SUMMARY")
    print(f"{'─'*w}")
    print(f"  {'JSONL envelopes read':<32} {ev['envelopes_read']:>8,}")
    print(f"  {'Events attempted':<32} {ev['rows_attempted']:>8,}")
    print(f"  {'Events inserted':<32} {ev['rows_inserted']:>8,}")
    print(f"  {'Events skipped (duplicates)':<32} {ev['rows_attempted'] - ev['rows_inserted']:>8,}")
    print(f"  {'Event parse errors':<32} {ev['parse_errors']:>8,}")
    print(f"{'─'*w}")
    print(f"  {'Employees attempted':<32} {em['rows_attempted']:>8,}")
    print(f"  {'Employees inserted':<32} {em['rows_inserted']:>8,}")
    print(f"  {'Employees skipped (duplicates)':<32} {em['rows_attempted'] - em['rows_inserted']:>8,}")
    print(f"  {'Employee parse errors':<32} {em['parse_errors']:>8,}")
    print(f"{'─'*w}")
    print("  INTEGRITY CHECKS")
    print(f"{'─'*w}")
    null_status    = "FAIL" if ic["null_email"] else "OK"
    unmatch_ev_st  = "FAIL" if ic["unmatched_event_emails"] else "OK"
    unmatch_emp_st = "WARN" if ic["unmatched_employee_emails"] else "OK"
    print(f"  {'Null/empty user_email':<32} {ic['null_email']:>6,}  [{null_status}]")
    print(f"  {'Event emails not in employees':<32} {ic['unmatched_event_emails']:>6,}  [{unmatch_ev_st}]")
    print(f"  {'Employee emails not in events':<32} {ic['unmatched_employee_emails']:>6,}  [{unmatch_emp_st}]")
    for field, count in ic["negative_checks"].items():
        status = "FAIL" if count else "OK"
        print(f"  {'Negative ' + field:<32} {count:>6,}  [{status}]")
    print(f"{'─'*w}")
    print("  INDEXES")
    print(f"{'─'*w}")
    for name, _ in INDEXES:
        tag = "created" if name in created_indexes else "already existed"
        print(f"  {name:<36}  {tag}")
    print(f"{'─'*w}")
    print(f"  Database: {DB_PATH}")
    print(f"{'═'*w}\n")


if __name__ == "__main__":
    main()