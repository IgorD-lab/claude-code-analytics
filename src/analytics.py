"""
analytics.py — Query functions for telemetry.db, each returning a pandas DataFrame.

Usage:
    from src.analytics import get_db, cost_by_practice, ...

    conn = get_db()
    df = cost_by_practice(conn)
"""

import sqlite3
from pathlib import Path

import pandas as pd

DB_PATH = Path(__file__).parent.parent / "data" / "processed" / "telemetry.db"


def get_db(path: Path = DB_PATH) -> sqlite3.Connection:
    """Open a read-only connection to telemetry.db."""
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


# ── 1. Total cost and token usage by practice ────────────────────────────────

def cost_by_practice(conn: sqlite3.Connection) -> pd.DataFrame:
    """
    Total cost and token usage (input, output, cache) for api_request events,
    grouped by the employee's practice team.

    Join: events.user_email → employees.email
    Falls back to resource_user_practice when the email is not in employees.
    """
    sql = """
        SELECT
            COALESCE(emp.practice, e.resource_user_practice, 'Unknown') AS practice,
            COUNT(*)                                AS api_requests,
            ROUND(SUM(e.cost_usd), 4)              AS total_cost_usd,
            SUM(e.input_tokens)                    AS total_input_tokens,
            SUM(e.output_tokens)                   AS total_output_tokens,
            SUM(e.cache_creation_tokens)           AS total_cache_creation_tokens,
            SUM(e.cache_read_tokens)               AS total_cache_read_tokens
        FROM events e
        LEFT JOIN employees emp ON e.user_email = emp.email
        WHERE e.body = 'claude_code.api_request'
        GROUP BY 1
        ORDER BY total_cost_usd DESC
    """
    return pd.read_sql(sql, conn)


# ── 2. Event count by hour of day ────────────────────────────────────────────

def events_by_hour(conn: sqlite3.Connection) -> pd.DataFrame:
    """
    Total event count broken down by UTC hour of day (0–23), across all event types.
    Useful for spotting peak-usage windows.
    """
    sql = """
        SELECT
            CAST(strftime('%H', event_timestamp) AS INTEGER) AS hour_utc,
            COUNT(*) AS event_count
        FROM events
        WHERE event_timestamp IS NOT NULL
        GROUP BY 1
        ORDER BY 1
    """
    df = pd.read_sql(sql, conn)
    # Ensure all 24 hours are present even if some have zero events
    df = (
        pd.DataFrame({"hour_utc": range(24)})
        .merge(df, on="hour_utc", how="left")
        .fillna({"event_count": 0})
        .astype({"event_count": int})
    )
    return df


# ── 3. Tool usage and accept/reject rates ────────────────────────────────────

def tool_usage(conn: sqlite3.Connection) -> pd.DataFrame:
    """
    For each tool seen in tool_decision events: total decisions made,
    count accepted, count rejected, and accept rate (%).

    Note: tool_result events record execution time but not accept/reject outcome;
    tool_decision events carry the decision and are the correct source for this metric.
    """
    sql = """
        SELECT
            tool_name,
            COUNT(*)                                              AS total_decisions,
            SUM(CASE WHEN decision = 'accept' THEN 1 ELSE 0 END) AS accepted,
            SUM(CASE WHEN decision = 'reject' THEN 1 ELSE 0 END) AS rejected,
            ROUND(
                100.0 * SUM(CASE WHEN decision = 'accept' THEN 1 ELSE 0 END) / COUNT(*),
                1
            )                                                     AS accept_rate_pct
        FROM events
        WHERE body = 'claude_code.tool_decision'
          AND tool_name IS NOT NULL
        GROUP BY 1
        ORDER BY total_decisions DESC
    """
    return pd.read_sql(sql, conn)


# ── 4. Cost breakdown by model ───────────────────────────────────────────────

def cost_by_model(conn: sqlite3.Connection) -> pd.DataFrame:
    """
    Total cost and token usage split by model, from api_request events only.
    Includes a cost_share_pct column showing each model's fraction of total spend.
    """
    sql = """
        SELECT
            model,
            COUNT(*)                      AS api_requests,
            ROUND(SUM(cost_usd), 4)       AS total_cost_usd,
            SUM(input_tokens)             AS total_input_tokens,
            SUM(output_tokens)            AS total_output_tokens,
            SUM(cache_read_tokens)        AS total_cache_read_tokens,
            ROUND(AVG(cost_usd), 6)       AS avg_cost_per_request,
            ROUND(AVG(duration_ms), 0)    AS avg_duration_ms
        FROM events
        WHERE body = 'claude_code.api_request'
          AND model IS NOT NULL
        GROUP BY 1
        ORDER BY total_cost_usd DESC
    """
    df = pd.read_sql(sql, conn)
    total = df["total_cost_usd"].sum()
    df["cost_share_pct"] = (df["total_cost_usd"] / total * 100).round(1)
    return df


# ── 5. Top 10 users by total cost ────────────────────────────────────────────

def top_users_by_cost(conn: sqlite3.Connection, n: int = 10) -> pd.DataFrame:
    """
    Top-N users ranked by total API spend, enriched with full_name and practice
    from the employees table.
    """
    sql = f"""
        SELECT
            e.user_email,
            emp.full_name,
            COALESCE(emp.practice, e.resource_user_practice, 'Unknown') AS practice,
            emp.level,
            COUNT(*)                    AS api_requests,
            ROUND(SUM(e.cost_usd), 4)  AS total_cost_usd,
            SUM(e.input_tokens)         AS total_input_tokens,
            SUM(e.output_tokens)        AS total_output_tokens
        FROM events e
        LEFT JOIN employees emp ON e.user_email = emp.email
        WHERE e.body = 'claude_code.api_request'
        GROUP BY e.user_email
        ORDER BY total_cost_usd DESC
        LIMIT {int(n)}
    """
    return pd.read_sql(sql, conn)


# ── 6. Daily cost time series ────────────────────────────────────────────────

def daily_cost(conn: sqlite3.Connection) -> pd.DataFrame:
    """
    Total API cost per calendar day (UTC), returned as a continuous time series
    with zero-filled gaps for days with no activity.
    """
    # NOTE: alias must not be "day" — the events table has an integer column named "day"
    # (day-of-month from the envelope). GROUP BY day would silently group by that column
    # instead of the alias, collapsing Dec 8 + Jan 8 into one bucket, etc.
    # GROUP BY 1 (positional) or a distinct alias like "event_date" avoids the collision.
    sql = """
        SELECT
            DATE(event_timestamp) AS event_date,
            ROUND(SUM(cost_usd), 4)    AS total_cost_usd,
            COUNT(*)                   AS api_requests
        FROM events
        WHERE body = 'claude_code.api_request'
          AND event_timestamp IS NOT NULL
        GROUP BY 1
        ORDER BY 1
    """
    df = pd.read_sql(sql, conn, parse_dates=["event_date"])
    df = df.rename(columns={"event_date": "date"})

    # Fill in any missing dates in the range with zeros
    full_range = pd.date_range(df["date"].min(), df["date"].max(), freq="D")
    df = (
        df.set_index("date")
        .reindex(full_range)
        .fillna({"total_cost_usd": 0.0, "api_requests": 0})
        .astype({"api_requests": int})
        .reset_index()
        .rename(columns={"index": "date"})
    )
    return df


# ── 7. Session count and average duration by practice ────────────────────────

def sessions_by_practice(conn: sqlite3.Connection) -> pd.DataFrame:
    """
    Per practice: number of unique sessions and average session duration in minutes.

    Session duration = time from first to last event in that session_id.
    Sessions with only one event (duration = 0) are included in the average.
    Practice is sourced from resource_user_practice on the events themselves,
    which is populated from the OTel resource block for every event type.
    """
    sql = """
        SELECT
            COALESCE(resource_user_practice, 'Unknown') AS practice,
            COUNT(DISTINCT session_id)                  AS session_count,
            ROUND(
                AVG(
                    (MAX(event_timestamp_ms) - MIN(event_timestamp_ms)) / 60000.0
                ),
                1
            )                                           AS avg_session_duration_min,
            ROUND(
                MAX(
                    (MAX(event_timestamp_ms) - MIN(event_timestamp_ms)) / 60000.0
                ),
                1
            )                                           AS max_session_duration_min
        FROM events
        WHERE session_id IS NOT NULL
        GROUP BY session_id, resource_user_practice  -- aggregate per session first
    """
    # Two-level aggregation: first collapse to one row per session, then group by practice
    per_session_sql = """
        SELECT
            COALESCE(resource_user_practice, 'Unknown') AS practice,
            session_id,
            (MAX(event_timestamp_ms) - MIN(event_timestamp_ms)) / 60000.0 AS duration_min
        FROM events
        WHERE session_id IS NOT NULL
        GROUP BY session_id, practice
    """
    df = pd.read_sql(per_session_sql, conn)
    result = (
        df.groupby("practice")
        .agg(
            session_count=("session_id", "count"),
            avg_session_duration_min=("duration_min", "mean"),
            max_session_duration_min=("duration_min", "max"),
        )
        .round(1)
        .reset_index()
        .sort_values("session_count", ascending=False)
    )
    return result

if __name__ == '__main__':
    conn = get_db()

    def show(title, df):
        print(f"\n=== {title} ===")
        if df is None or df.empty:
            print("(no data)")
        else:
            print(df.to_string(index=False))

    show("Cost by practice", cost_by_practice(conn))
    show("Events by hour", events_by_hour(conn))
    show("Tool usage", tool_usage(conn))
    show("Cost by model", cost_by_model(conn))
    show("Top users by cost", top_users_by_cost(conn))
    show("Daily cost", daily_cost(conn))
    show("Sessions by practice", sessions_by_practice(conn))
    
    