#!/usr/bin/env python3
"""Print one full example row (non-null fields only) per unique event type."""

import json
import sys
from pathlib import Path

# ── reuse the same parsing helpers ──────────────────────────────────────────
sys.path.insert(0, str(Path(__file__).parent))
from validate_telemetry import JSONL_PATH, ALL_COLS, load_rows


def main() -> None:
    rows, envelope_count, parse_errors = load_rows(JSONL_PATH)
    print(f"Loaded {len(rows):,} rows from {envelope_count:,} envelopes.\n")

    # Pick the first row seen for each body value
    seen: dict[str, dict] = {}
    for row in rows:
        body = row.get("body") or "(no body)"
        if body not in seen:
            seen[body] = row

    width = 70
    for body, row in sorted(seen.items()):
        print("═" * width)
        print(f"  EVENT TYPE: {body}")
        print("─" * width)
        for col in ALL_COLS:
            val = row.get(col)
            if val is not None and val != "":
                # Truncate very long values for readability
                display = str(val)
                if len(display) > 120:
                    display = display[:117] + "..."
                print(f"  {col:<35}  {display}")
        print()

    print("═" * width)
    print(f"  {len(seen)} unique event type(s) shown.")
    print("═" * width)


if __name__ == "__main__":
    main()