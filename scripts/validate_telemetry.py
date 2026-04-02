#!/usr/bin/env python3
"""Validation script for telemetry_logs.jsonl — flattens all log events and reports data quality."""

import json
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path

JSONL_PATH = Path(__file__).parent.parent / "data" / "raw" / "telemetry_logs.jsonl"

# All columns present in a fully populated flattened row
ENVELOPE_COLS = ["messageType", "owner", "logGroup", "logStream", "year", "month", "day"]
EVENT_COLS = ["event_id", "event_timestamp_ms"]
MESSAGE_BODY_COL = ["body"]
ATTRIBUTES_COLS = [
    "event.timestamp", "organization.id", "session.id", "terminal.type",
    "user.account_uuid", "user.email", "user.id", "event.name",
    "prompt", "prompt_length",
    "cache_creation_tokens", "cache_read_tokens", "cost_usd",
    "duration_ms", "input_tokens", "model", "output_tokens",
    "decision", "source", "tool_name",
]
SCOPE_COLS = ["scope.name", "scope.version"]
RESOURCE_COLS = [
    "resource.host.arch", "resource.host.name", "resource.os.type",
    "resource.os.version", "resource.service.name", "resource.service.version",
    "resource.user.email", "resource.user.practice", "resource.user.profile",
    "resource.user.serial",
]
ALL_COLS = ENVELOPE_COLS + EVENT_COLS + MESSAGE_BODY_COL + ATTRIBUTES_COLS + SCOPE_COLS + RESOURCE_COLS


def flatten_event(envelope: dict, raw_event: dict) -> dict | None:
    """Return one flat dict per log event; returns None if message JSON is unparseable."""
    row: dict = {}

    for col in ENVELOPE_COLS:
        row[col] = envelope.get(col)

    row["event_id"] = raw_event.get("id")
    row["event_timestamp_ms"] = raw_event.get("timestamp")

    raw_msg = raw_event.get("message", "")
    try:
        msg = json.loads(raw_msg)
    except (json.JSONDecodeError, TypeError):
        return None

    row["body"] = msg.get("body")

    attrs = msg.get("attributes") or {}
    for col in ATTRIBUTES_COLS:
        row[col] = attrs.get(col)

    scope = msg.get("scope") or {}
    row["scope.name"] = scope.get("name")
    row["scope.version"] = scope.get("version")

    resource = msg.get("resource") or {}
    for col in RESOURCE_COLS:
        key = col[len("resource."):]  # strip "resource." prefix
        row[col] = resource.get(key)

    return row


def load_rows(path: Path) -> tuple[list[dict], int, int]:
    """Parse JSONL and flatten. Returns (rows, envelope_count, parse_error_count)."""
    rows: list[dict] = []
    envelope_count = 0
    parse_errors = 0

    with path.open("r", encoding="utf-8") as f:
        for lineno, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                envelope = json.loads(line)
            except json.JSONDecodeError as e:
                print(f"  [WARN] Line {lineno}: envelope JSON parse error — {e}", file=sys.stderr)
                parse_errors += 1
                continue

            envelope_count += 1
            for raw_event in envelope.get("logEvents") or []:
                row = flatten_event(envelope, raw_event)
                if row is None:
                    parse_errors += 1
                else:
                    rows.append(row)

    return rows, envelope_count, parse_errors


def null_counts(rows: list[dict]) -> dict[str, int]:
    counts: dict[str, int] = defaultdict(int)
    for row in rows:
        for col in ALL_COLS:
            val = row.get(col)
            if val is None or val == "":
                counts[col] += 1
    return dict(counts)


def duplicate_event_ids(rows: list[dict]) -> list[str]:
    id_counts = Counter(row["event_id"] for row in rows if row.get("event_id"))
    return [eid for eid, cnt in id_counts.items() if cnt > 1]


def timestamp_range(rows: list[dict]) -> tuple[datetime | None, datetime | None]:
    ts_values = [row["event_timestamp_ms"] for row in rows if row.get("event_timestamp_ms") is not None]
    if not ts_values:
        return None, None
    ts_min = min(ts_values)
    ts_max = max(ts_values)
    to_dt = lambda ms: datetime.fromtimestamp(ms / 1000, tz=timezone.utc)
    return to_dt(ts_min), to_dt(ts_max)


def sep(char="─", width=70) -> str:
    return char * width


def main() -> None:
    if not JSONL_PATH.exists():
        print(f"ERROR: File not found: {JSONL_PATH}", file=sys.stderr)
        sys.exit(1)

    print(f"\n{'═'*70}")
    print(f"  Telemetry Validation Report")
    print(f"  Source: {JSONL_PATH}")
    print(f"{'═'*70}\n")

    rows, envelope_count, parse_errors = load_rows(JSONL_PATH)
    total_rows = len(rows)

    # ── 1. Counts ───────────────────────────────────────────────────────────
    print(sep())
    print("  INGESTION SUMMARY")
    print(sep())
    print(f"  Envelopes (JSONL lines) read : {envelope_count:,}")
    print(f"  Total flattened event rows   : {total_rows:,}")
    print(f"  Message parse errors         : {parse_errors:,}")

    # ── 2. Null / missing fields ────────────────────────────────────────────
    print(f"\n{sep()}")
    print("  NULL / MISSING FIELD COUNTS  (only columns with at least 1 null shown)")
    print(sep())
    nulls = null_counts(rows)
    has_nulls = {col: cnt for col, cnt in nulls.items() if cnt > 0}
    if has_nulls:
        col_width = max(len(c) for c in has_nulls)
        for col, cnt in sorted(has_nulls.items(), key=lambda x: -x[1]):
            pct = cnt / total_rows * 100 if total_rows else 0
            print(f"  {col:<{col_width}}  {cnt:>7,}  ({pct:5.1f}%)")
    else:
        print("  No missing values found.")

    # ── 3. Duplicate event IDs ──────────────────────────────────────────────
    print(f"\n{sep()}")
    print("  DUPLICATE EVENT IDs")
    print(sep())
    dupes = duplicate_event_ids(rows)
    if dupes:
        print(f"  {len(dupes):,} duplicate event ID(s) found:")
        for eid in dupes[:20]:
            print(f"    {eid}")
        if len(dupes) > 20:
            print(f"    … and {len(dupes) - 20} more")
    else:
        print("  No duplicate event IDs found.")

    # ── 4. Timestamp range ──────────────────────────────────────────────────
    print(f"\n{sep()}")
    print("  TIMESTAMP RANGE")
    print(sep())
    ts_min, ts_max = timestamp_range(rows)
    if ts_min and ts_max:
        print(f"  Earliest : {ts_min.strftime('%Y-%m-%d %H:%M:%S UTC')}")
        print(f"  Latest   : {ts_max.strftime('%Y-%m-%d %H:%M:%S UTC')}")
        span = ts_max - ts_min
        print(f"  Span     : {str(span)}")
    else:
        print("  No timestamp data available.")

    # ── 5. Unique event types ────────────────────────────────────────────────
    print(f"\n{sep()}")
    print("  UNIQUE EVENT TYPES  (body field)")
    print(sep())
    event_type_counts = Counter(row["body"] for row in rows if row.get("body"))
    if event_type_counts:
        col_width = max(len(k) for k in event_type_counts)
        for etype, cnt in sorted(event_type_counts.items(), key=lambda x: -x[1]):
            print(f"  {etype:<{col_width}}  {cnt:>7,}")
    else:
        print("  No event type data found.")

    # ── 6. Unique models ────────────────────────────────────────────────────
    print(f"\n{sep()}")
    print("  UNIQUE MODELS  (from api_request events)")
    print(sep())
    model_counts = Counter(
        row["model"] for row in rows
        if row.get("model") and row.get("body") == "claude_code.api_request"
    )
    if model_counts:
        col_width = max(len(k) for k in model_counts)
        for model, cnt in sorted(model_counts.items(), key=lambda x: -x[1]):
            print(f"  {model:<{col_width}}  {cnt:>7,}")
    else:
        print("  No model data found.")

    # ── 7. Row counts per event type ────────────────────────────────────────
    print(f"\n{sep()}")
    print("  ROW COUNTS PER EVENT TYPE")
    print(sep())
    if event_type_counts:
        col_width = max(len(k) for k in event_type_counts)
        total_typed = sum(event_type_counts.values())
        untyped = total_rows - total_typed
        for etype, cnt in sorted(event_type_counts.items(), key=lambda x: -x[1]):
            pct = cnt / total_rows * 100 if total_rows else 0
            print(f"  {etype:<{col_width}}  {cnt:>7,}  ({pct:5.1f}%)")
        if untyped:
            print(f"  {'(no body/event type)':<{col_width}}  {untyped:>7,}")
    else:
        print("  No data.")

    print(f"\n{'═'*70}\n")


if __name__ == "__main__":
    main()