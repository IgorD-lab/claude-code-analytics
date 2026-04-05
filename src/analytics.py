"""
analytics.py — Query functions for telemetry.db, each returning a pandas DataFrame.

Usage:
    from src.analytics import get_db, Filters, cost_by_practice, ...

    conn = get_db()
    df = cost_by_practice(conn)
    df_filtered = cost_by_practice(conn, Filters(date_from="2025-12-01", practices=["ML Engineering"]))
"""

from dataclasses import dataclass, field
import sqlite3
from pathlib import Path

import pandas as pd

DB_PATH = Path(__file__).parent.parent / "data" / "processed" / "telemetry.db"


def get_db(path: Path = DB_PATH) -> sqlite3.Connection:
    """Open a read-only connection to telemetry.db."""
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


# ── Filter support ─────────────────────────────────────────────────────────────

@dataclass
class Filters:
    """Optional query filters, matching the dashboard sidebar and API query params."""
    date_from: str | None = None
    date_to: str | None = None
    practices: list[str] = field(default_factory=list)
    levels: list[str] = field(default_factory=list)
    locations: list[str] = field(default_factory=list)


def _where(filters: "Filters | None", alias: str = "") -> tuple[str, list]:
    """
    Build an AND-prefixed WHERE fragment and positional params for common filters.

    alias: events table alias used in the calling query (e.g. "e" for FROM events e).
           Use "" for queries without a table alias.
    Employee-table filters (practice, level, location) use correlated subqueries
    so no JOIN is required on the calling side. The practice filter matches both
    the resource_user_practice field and the employees table to cover all cases.
    Returns ("", []) when filters is None or all fields are unset.
    """
    if not filters:
        return "", []

    col = f"{alias}." if alias else ""
    parts: list[str] = []
    params: list = []

    if filters.date_from:
        parts.append(f"DATE({col}event_timestamp) >= ?")
        params.append(filters.date_from)
    if filters.date_to:
        parts.append(f"DATE({col}event_timestamp) <= ?")
        params.append(filters.date_to)
    if filters.practices:
        ph = ",".join("?" * len(filters.practices))
        # Match via resource field (OTel resource block) OR via employees table join
        parts.append(
            f"({col}resource_user_practice IN ({ph})"
            f" OR {col}user_email IN (SELECT email FROM employees WHERE practice IN ({ph})))"
        )
        params.extend(filters.practices)
        params.extend(filters.practices)  # second set for the subquery placeholder
    if filters.levels:
        ph = ",".join("?" * len(filters.levels))
        parts.append(f"{col}user_email IN (SELECT email FROM employees WHERE level IN ({ph}))")
        params.extend(filters.levels)
    if filters.locations:
        ph = ",".join("?" * len(filters.locations))
        parts.append(f"{col}user_email IN (SELECT email FROM employees WHERE location IN ({ph}))")
        params.extend(filters.locations)

    return ("AND " + " AND ".join(parts)) if parts else "", params


# ── 1. Total cost and token usage by practice ──────────────────────────────────

def cost_by_practice(conn: sqlite3.Connection, filters: Filters | None = None) -> pd.DataFrame:
    """
    Total cost and token usage (input, output, cache) for api_request events,
    grouped by the employee's practice team.

    Join: events.user_email → employees.email
    Falls back to resource_user_practice when the email is not in employees.
    """
    extra, params = _where(filters, alias="e")
    sql = f"""
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
          {extra}
        GROUP BY 1
        ORDER BY total_cost_usd DESC
    """
    return pd.read_sql(sql, conn, params=params or None)


# ── 2. Event count by hour of day ──────────────────────────────────────────────

def events_by_hour(conn: sqlite3.Connection, filters: Filters | None = None) -> pd.DataFrame:
    """
    Total event count broken down by UTC hour of day (0–23), across all event types.
    All 24 hours are always present (zero-filled for hours with no events).
    """
    extra, params = _where(filters)
    sql = f"""
        SELECT
            CAST(strftime('%H', event_timestamp) AS INTEGER) AS hour_utc,
            COUNT(*) AS event_count
        FROM events
        WHERE event_timestamp IS NOT NULL
          {extra}
        GROUP BY 1
        ORDER BY 1
    """
    df = pd.read_sql(sql, conn, params=params or None)
    df = (
        pd.DataFrame({"hour_utc": range(24)})
        .merge(df, on="hour_utc", how="left")
        .fillna({"event_count": 0})
        .astype({"event_count": int})
    )
    return df


# ── 3. Tool usage and accept/reject rates ──────────────────────────────────────

def tool_usage(conn: sqlite3.Connection, filters: Filters | None = None) -> pd.DataFrame:
    """
    For each tool seen in tool_decision events: total decisions made,
    count accepted, count rejected, and accept rate (%).
    """
    extra, params = _where(filters)
    sql = f"""
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
          {extra}
        GROUP BY 1
        ORDER BY total_decisions DESC
    """
    return pd.read_sql(sql, conn, params=params or None)


# ── 4. Cost breakdown by model ─────────────────────────────────────────────────

def cost_by_model(conn: sqlite3.Connection, filters: Filters | None = None) -> pd.DataFrame:
    """
    Total cost and token usage split by model, from api_request events only.
    Includes a cost_share_pct column showing each model's fraction of total spend.
    """
    extra, params = _where(filters)
    sql = f"""
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
          {extra}
        GROUP BY 1
        ORDER BY total_cost_usd DESC
    """
    df = pd.read_sql(sql, conn, params=params or None)
    total = df["total_cost_usd"].sum()
    df["cost_share_pct"] = (df["total_cost_usd"] / total * 100).round(1) if total else 0.0
    return df


# ── 5. Top N users by total cost ───────────────────────────────────────────────

def top_users_by_cost(
    conn: sqlite3.Connection, n: int = 10, filters: Filters | None = None
) -> pd.DataFrame:
    """
    Top-N users ranked by total API spend, enriched with full_name and practice
    from the employees table.
    """
    extra, params = _where(filters, alias="e")
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
          {extra}
        GROUP BY e.user_email
        ORDER BY total_cost_usd DESC
        LIMIT {int(n)}
    """
    return pd.read_sql(sql, conn, params=params or None)


# ── 6. Daily cost time series ──────────────────────────────────────────────────

def daily_cost(conn: sqlite3.Connection, filters: Filters | None = None) -> pd.DataFrame:
    """
    Total API cost per calendar day (UTC), returned as a continuous time series
    with zero-filled gaps for days with no activity.

    NOTE: alias must not be "day" — the events table has an integer column named "day"
    (day-of-month from the envelope). GROUP BY 1 (positional) avoids the collision.
    """
    extra, params = _where(filters)
    sql = f"""
        SELECT
            DATE(event_timestamp) AS event_date,
            ROUND(SUM(cost_usd), 4)    AS total_cost_usd,
            COUNT(*)                   AS api_requests
        FROM events
        WHERE body = 'claude_code.api_request'
          AND event_timestamp IS NOT NULL
          {extra}
        GROUP BY 1
        ORDER BY 1
    """
    df = pd.read_sql(sql, conn, params=params or None, parse_dates=["event_date"])
    df = df.rename(columns={"event_date": "date"})

    if df.empty:
        return df

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


# ── 7. Session count and average duration by practice ─────────────────────────

def sessions_by_practice(
    conn: sqlite3.Connection, filters: Filters | None = None
) -> pd.DataFrame:
    """
    Per practice: number of unique sessions and average session duration in minutes.

    Two-level aggregation: collapse to one row per session first, then group by practice.
    Sessions with only one event (duration = 0) are included in the average.
    """
    extra, params = _where(filters)
    per_session_sql = f"""
        SELECT
            COALESCE(resource_user_practice, 'Unknown') AS practice,
            session_id,
            (MAX(event_timestamp_ms) - MIN(event_timestamp_ms)) / 60000.0 AS duration_min
        FROM events
        WHERE session_id IS NOT NULL
          {extra}
        GROUP BY session_id, practice
    """
    df = pd.read_sql(per_session_sql, conn, params=params or None)
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


# ── 8. Model efficiency ────────────────────────────────────────────────────────

def model_efficiency(
    conn: sqlite3.Connection, filters: Filters | None = None
) -> pd.DataFrame:
    """
    Per model: request count, cost, percentage shares of total requests and total
    cost, average cost per request, and average duration.

    Extends cost_by_model with pct_requests and pct_cost columns, which require
    cross-row totals and cannot be expressed in a single GROUP BY query.
    """
    extra, params = _where(filters)
    sql = f"""
        SELECT
            model,
            COUNT(*)                    AS requests,
            ROUND(SUM(cost_usd), 4)     AS total_cost_usd,
            SUM(input_tokens)           AS total_input_tokens,
            SUM(output_tokens)          AS total_output_tokens,
            ROUND(AVG(cost_usd), 6)     AS avg_cost_per_request,
            ROUND(AVG(duration_ms), 0)  AS avg_duration_ms
        FROM events
        WHERE body = 'claude_code.api_request'
          AND model IS NOT NULL
          {extra}
        GROUP BY 1
        ORDER BY total_cost_usd DESC
    """
    df = pd.read_sql(sql, conn, params=params or None)
    total_req = df["requests"].sum()
    total_cost = df["total_cost_usd"].sum()
    df["pct_requests"] = (df["requests"] / total_req * 100).round(1) if total_req else 0.0
    df["pct_cost"] = (df["total_cost_usd"] / total_cost * 100).round(1) if total_cost else 0.0
    return df


# ── 9. Cost per 1,000 tokens by model ─────────────────────────────────────────

def cost_per_token(
    conn: sqlite3.Connection, filters: Filters | None = None
) -> pd.DataFrame:
    """
    Cost per 1,000 tokens (input + output combined) for each model.
    Rows where total_tokens is zero are excluded via NULLIF.
    """
    extra, params = _where(filters)
    sql = f"""
        SELECT
            model,
            ROUND(SUM(cost_usd), 4)                                        AS total_cost_usd,
            SUM(input_tokens)                                              AS total_input_tokens,
            SUM(output_tokens)                                             AS total_output_tokens,
            SUM(input_tokens) + SUM(output_tokens)                         AS total_tokens,
            ROUND(
                1000.0 * SUM(cost_usd)
                / NULLIF(SUM(input_tokens) + SUM(output_tokens), 0),
                6
            )                                                              AS cost_per_1k_tokens
        FROM events
        WHERE body = 'claude_code.api_request'
          AND model IS NOT NULL
          {extra}
        GROUP BY 1
        ORDER BY cost_per_1k_tokens DESC
    """
    return pd.read_sql(sql, conn, params=params or None)


# ── 11. Events by day of week ─────────────────────────────────────────────────

def events_by_day_of_week(
    conn: sqlite3.Connection, filters: Filters | None = None
) -> pd.DataFrame:
    """
    Total event count for each day of the week (Mon–Sun), across all event types.
    Always returns exactly 7 rows in Mon-first order; days with no events are zero-filled.

    SQLite strftime('%w') returns 0=Sun, 1=Mon, …, 6=Sat.
    The sort key (i-1)%7 rotates that so Mon sorts first (0) and Sun sorts last (6).
    """
    extra, params = _where(filters)
    sql = f"""
        SELECT
            CAST(strftime('%w', event_timestamp) AS INTEGER) AS dow_index,
            COUNT(*) AS event_count
        FROM events
        WHERE event_timestamp IS NOT NULL
          {extra}
        GROUP BY 1
    """
    df = pd.read_sql(sql, conn, params=params or None)
    dow_names = {0: "Sun", 1: "Mon", 2: "Tue", 3: "Wed", 4: "Thu", 5: "Fri", 6: "Sat"}
    df = (
        pd.DataFrame({"dow_index": range(7)})
        .merge(df, on="dow_index", how="left")
        .fillna({"event_count": 0})
        .astype({"event_count": int})
        .assign(
            day_of_week=lambda d: d["dow_index"].map(dow_names),
            _sort=lambda d: d["dow_index"].map(lambda i: (i - 1) % 7),
        )
        .sort_values("_sort")
        [["day_of_week", "event_count"]]
        .reset_index(drop=True)
    )
    return df


# ── 12. API requests broken down by model and practice ────────────────────────

def requests_by_model_and_practice(
    conn: sqlite3.Connection, filters: Filters | None = None
) -> pd.DataFrame:
    """
    Request count for each (practice, model) combination from api_request events.

    Drives the "API Requests by Model and Practice" stacked bar chart.
    Practice is resolved from employees first, falling back to resource_user_practice.
    """
    extra, params = _where(filters, alias="e")
    sql = f"""
        SELECT
            COALESCE(emp.practice, e.resource_user_practice, 'Unknown') AS practice,
            e.model,
            COUNT(*) AS requests
        FROM events e
        LEFT JOIN employees emp ON e.user_email = emp.email
        WHERE e.body = 'claude_code.api_request'
          AND e.model IS NOT NULL
          {extra}
        GROUP BY 1, 2
        ORDER BY practice, requests DESC
    """
    return pd.read_sql(sql, conn, params=params or None)


# ── 13. Cache token stats by model ────────────────────────────────────────────

def cache_stats_by_model(
    conn: sqlite3.Connection, filters: Filters | None = None
) -> pd.DataFrame:
    """
    Cache read and creation token counts per model, with hit ratio (%).

    hit_ratio_pct = cache_read / (cache_read + cache_creation) * 100.
    Rows where total cache tokens is zero produce NULL hit_ratio_pct via NULLIF.
    Drives the "Cache Hit Ratio by Model" stacked bar chart.
    """
    extra, params = _where(filters)
    sql = f"""
        SELECT
            model,
            SUM(cache_read_tokens)     AS cache_read_tokens,
            SUM(cache_creation_tokens) AS cache_creation_tokens,
            SUM(cache_read_tokens) + SUM(cache_creation_tokens) AS total_cache_tokens,
            ROUND(
                100.0 * SUM(cache_read_tokens)
                / NULLIF(SUM(cache_read_tokens) + SUM(cache_creation_tokens), 0),
                1
            ) AS hit_ratio_pct
        FROM events
        WHERE body = 'claude_code.api_request'
          AND model IS NOT NULL
          {extra}
        GROUP BY 1
        ORDER BY total_cache_tokens DESC
    """
    return pd.read_sql(sql, conn, params=params or None)


# ── 14. Summary stats (metric cards) ──────────────────────────────────────────

def summary_stats(
    conn: sqlite3.Connection, filters: Filters | None = None
) -> pd.DataFrame:
    """
    Single-row summary matching the four metric cards at the top of the dashboard:
    total events, total API cost, unique users, and unique sessions.
    """
    extra, params = _where(filters)
    sql = f"""
        SELECT
            COUNT(*)                                                                    AS total_events,
            ROUND(SUM(CASE WHEN body = 'claude_code.api_request' THEN cost_usd ELSE 0 END), 4)
                                                                                        AS total_api_cost_usd,
            COUNT(DISTINCT user_email)                                                  AS unique_users,
            COUNT(DISTINCT session_id)                                                  AS unique_sessions
        FROM events
        WHERE 1=1
          {extra}
    """
    return pd.read_sql(sql, conn, params=params or None)


# ── 15. Available filter values ────────────────────────────────────────────────

def get_filters(conn: sqlite3.Connection) -> dict:
    """
    Return all distinct filter values available in the database.
    Used by /api/filters to populate client-side dropdowns.
    """
    practices = [r[0] for r in conn.execute(
        "SELECT DISTINCT practice FROM employees WHERE practice IS NOT NULL ORDER BY practice"
    )]
    levels = [r[0] for r in conn.execute(
        "SELECT DISTINCT level FROM employees WHERE level IS NOT NULL ORDER BY level"
    )]
    locations = [r[0] for r in conn.execute(
        "SELECT DISTINCT location FROM employees WHERE location IS NOT NULL ORDER BY location"
    )]
    row = conn.execute(
        "SELECT MIN(DATE(event_timestamp)), MAX(DATE(event_timestamp))"
        " FROM events WHERE event_timestamp IS NOT NULL"
    ).fetchone()
    return {
        "practices": practices,
        "levels": levels,
        "locations": locations,
        "date_min": row[0],
        "date_max": row[1],
    }


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
    show("Model efficiency", model_efficiency(conn))
    show("Cost per token", cost_per_token(conn))
    show("Events by day of week", events_by_day_of_week(conn))
    show("Requests by model and practice", requests_by_model_and_practice(conn))
    show("Cache stats by model", cache_stats_by_model(conn))
    show("Summary stats", summary_stats(conn))
