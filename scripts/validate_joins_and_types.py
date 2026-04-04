#!/usr/bin/env python3
"""Validate join integrity and field sanity in telemetry.db."""

import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "data" / "processed" / "telemetry.db"

W = 60


def section(title):
    print(f"\n{'─'*W}")
    print(f"  {title}")
    print(f"{'─'*W}")


def main():
    conn = sqlite3.connect(DB_PATH)

    print(f"\n{'═'*W}")
    print("  Join & Type Validation Report")
    print(f"  Database: {DB_PATH}")
    print(f"{'═'*W}")

    # ── 1. Null / empty user_email ───────────────────────────────────────────
    section("1. NULL OR EMPTY user_email IN events")

    null_email = conn.execute(
        "SELECT COUNT(*) FROM events WHERE user_email IS NULL OR user_email = ''"
    ).fetchone()[0]
    total_events = conn.execute("SELECT COUNT(*) FROM events").fetchone()[0]
    print(f"  Total events          : {total_events:,}")
    print(f"  Null/empty user_email : {null_email:,}  ({null_email/total_events*100:.1f}%)")

    # ── 2. Email join coverage ───────────────────────────────────────────────
    section("2. EMAIL JOIN COVERAGE")

    # Unique emails in events not in employees
    unmatched_events = conn.execute("""
        SELECT COUNT(DISTINCT e.user_email)
        FROM events e
        LEFT JOIN employees emp ON e.user_email = emp.email
        WHERE emp.email IS NULL
          AND e.user_email IS NOT NULL
          AND e.user_email != ''
    """).fetchone()[0]

    unique_event_emails = conn.execute("""
        SELECT COUNT(DISTINCT user_email) FROM events
        WHERE user_email IS NOT NULL AND user_email != ''
    """).fetchone()[0]

    # Unique emails in employees not in events
    unmatched_employees = conn.execute("""
        SELECT COUNT(DISTINCT emp.email)
        FROM employees emp
        LEFT JOIN events e ON emp.email = e.user_email
        WHERE e.user_email IS NULL
    """).fetchone()[0]

    total_employee_emails = conn.execute("SELECT COUNT(*) FROM employees").fetchone()[0]

    print(f"  Unique emails in events                   : {unique_event_emails:,}")
    print(f"  Event emails NOT in employees             : {unmatched_events:,}  ({unmatched_events/unique_event_emails*100:.1f}%)")
    print(f"  Total employees                           : {total_employee_emails:,}")
    print(f"  Employee emails NOT present in events     : {unmatched_employees:,}  ({unmatched_employees/total_employee_emails*100:.1f}%)")

    if unmatched_events > 0:
        samples = conn.execute("""
            SELECT DISTINCT e.user_email
            FROM events e
            LEFT JOIN employees emp ON e.user_email = emp.email
            WHERE emp.email IS NULL
              AND e.user_email IS NOT NULL
              AND e.user_email != ''
            LIMIT 5
        """).fetchall()
        print(f"\n  Sample unmatched event emails (up to 5):")
        for (email,) in samples:
            print(f"    {email}")

    # ── 3. Negative values and unexpected nulls ──────────────────────────────
    section("3. NEGATIVE VALUES AND UNEXPECTED NULLS")

    checks = [
        ("cost_usd",               "claude_code.api_request"),
        ("duration_ms",            "claude_code.api_request"),
        ("input_tokens",           "claude_code.api_request"),
        ("output_tokens",          "claude_code.api_request"),
        ("cache_creation_tokens",  "claude_code.api_request"),
        ("cache_read_tokens",      "claude_code.api_request"),
        ("duration_ms",            "claude_code.tool_result"),
        ("prompt_length",          "claude_code.user_prompt"),
    ]

    any_issue = False
    for field, event_type in checks:
        rows_in_scope = conn.execute(
            f"SELECT COUNT(*) FROM events WHERE body = ? AND {field} IS NOT NULL",
            (event_type,)
        ).fetchone()[0]

        nulls = conn.execute(
            f"SELECT COUNT(*) FROM events WHERE body = ? AND {field} IS NULL",
            (event_type,)
        ).fetchone()[0]

        negatives = conn.execute(
            f"SELECT COUNT(*) FROM events WHERE body = ? AND {field} IS NOT NULL AND {field} < 0",
            (event_type,)
        ).fetchone()[0]

        label = f"{field}  [{event_type.split('.')[-1]}]"
        issues = []
        if nulls:   issues.append(f"{nulls:,} nulls")
        if negatives: issues.append(f"{negatives:,} negatives")
        status = ", ".join(issues) if issues else "OK"

        if issues:
            any_issue = True
        print(f"  {label:<48}  {status}")

    if not any_issue:
        print("  All fields clean — no negatives or unexpected nulls.")

    # ── 4. Malformed email addresses ─────────────────────────────────────────
    section("4. MALFORMED user_email VALUES (missing '@')")

    bad_emails = conn.execute("""
        SELECT DISTINCT user_email, COUNT(*) AS n
        FROM events
        WHERE user_email IS NOT NULL
          AND user_email != ''
          AND user_email NOT LIKE '%@%'
        GROUP BY user_email
        ORDER BY n DESC
        LIMIT 10
    """).fetchall()

    if bad_emails:
        print(f"  Found {len(bad_emails)} malformed email(s):")
        for email, n in bad_emails:
            print(f"    {email!r:40}  ({n:,} events)")
    else:
        print("  No malformed emails found — all contain '@'.")

    print(f"\n{'═'*W}\n")
    conn.close()


if __name__ == "__main__":
    main()