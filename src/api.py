"""
api.py — FastAPI REST API for Claude Code Usage Analytics.

All queries are delegated to analytics.py — the API layer contains no SQL.
Both this API and the Streamlit dashboard share the same analytics functions,
so the numbers are always consistent.

Run with:
    uvicorn src.api:app --reload

Interactive docs: http://localhost:8000/docs
"""

import sqlite3
import sys
from datetime import date
from pathlib import Path
from typing import Annotated

import pandas as pd
from fastapi import Depends, FastAPI, HTTPException, Query

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.analytics import (
    Filters,
    cache_stats_by_model,
    cost_by_model,
    cost_by_practice,
    cost_per_token,
    daily_cost,
    events_by_day_of_week,
    events_by_hour,
    get_db,
    get_filters,
    model_efficiency,
    requests_by_model_and_practice,
    sessions_by_practice,
    summary_stats,
    tool_usage,
    top_users_by_cost,
)
from src.models import (
    CacheStatsByModelRow,
    CostByModelRow,
    CostByPracticeRow,
    CostPerTokenRow,
    DailyCostRow,
    EventsByDayRow,
    FiltersResponse,
    Level,
    Location,
    ModelEfficiencyRow,
    Practice,
    RequestsByModelAndPracticeRow,
    RootResponse,
    RouteInfo,
    SessionsByPracticeRow,
    SummaryStats,
    ToolUsageRow,
    TopUserRow,
    UsageByHourRow,
)

# ── App ────────────────────────────────────────────────────────────────────────

app = FastAPI(
    title="Claude Code Analytics API",
    description=(
        "REST API exposing Claude Code telemetry analytics. "
        "All endpoints accept optional filters matching the dashboard filters."
    ),
    version="1.0.0",
)

# ── DB dependency ──────────────────────────────────────────────────────────────

def get_conn():
    """Yield a read-only SQLite connection; close it after the request."""
    conn = get_db()
    try:
        yield conn
    finally:
        conn.close()

DBConn = Annotated[sqlite3.Connection, Depends(get_conn)]

# ── Shared filter dependency ───────────────────────────────────────────────────
# Defined once here; every endpoint uses CommonFilters so the parameter set
# stays in sync automatically. FastAPI inlines these into each endpoint's
# OpenAPI schema, so Swagger shows the full set of filters on eversy operation.

def common_filters(
    date_from: Annotated[
        date | None,
        Query(description="Start date, inclusive."),
    ] = None,
    date_to: Annotated[
        date | None,
        Query(description="End date, inclusive."),
    ] = None,
    practice: Annotated[
        list[Practice],
        Query(description="Filter by practice team. Repeat for multiple values."),
    ] = [],
    level: Annotated[
        list[Level],
        Query(description="Filter by engineer level (L1–L10). Repeat for multiple values."),
    ] = [],
    location: Annotated[
        list[Location],
        Query(description="Filter by office location. Repeat for multiple values."),
    ] = [],
) -> Filters:
    # Cross-field validation: date range must be chronologically valid
    if date_from and date_to and date_from > date_to:
        raise HTTPException(
            status_code=422,
            detail=[
                {
                    "loc": ["query", "date_from"],
                    "msg": "date_from must not be later than date_to",
                    "type": "value_error.date_range",
                }
            ],
        )
    return Filters(
        date_from=date_from.isoformat() if date_from else None,
        date_to=date_to.isoformat() if date_to else None,
        # .value gives the plain str from the str-Enum, safe for SQLite params
        practices=[p.value for p in practice],
        levels=[lv.value for lv in level],
        locations=[loc.value for loc in location],
    )

CommonFilters = Annotated[Filters, Depends(common_filters)]

# ── Response helper ────────────────────────────────────────────────────────────

def _records(df: pd.DataFrame) -> list[dict]:
    """
    Convert a DataFrame to a list of plain dicts for Pydantic serialisation.
    Uses pd.isna() to normalise float NaN, pd.NA, and pd.NaT to None so
    Optional fields don't blow up during Pydantic validation.
    """
    return [
        {k: (None if pd.isna(v) else v) for k, v in row.items()}
        for row in df.to_dict(orient="records")
    ]

# ── Route index ────────────────────────────────────────────────────────────────

_ROUTES = [
    RouteInfo(path="/",                                  description="This index — all available routes with descriptions"),
    RouteInfo(path="/api/filters",                       description="Available filter values: practices, levels, locations, date range"),
    RouteInfo(path="/api/summary",                       description="Top-level metric cards: total events, API cost, unique users, unique sessions"),
    RouteInfo(path="/api/cost-by-practice",              description="Total API cost and token usage grouped by practice team"),
    RouteInfo(path="/api/cost-by-model",                 description="Total API cost, token usage, and cost share percentage by model"),
    RouteInfo(path="/api/daily-cost",                    description="Daily API cost time series with zero-filled gaps"),
    RouteInfo(path="/api/model-efficiency",              description="Request share, cost share, and avg cost per request by model"),
    RouteInfo(path="/api/cost-per-token",                description="Cost per 1,000 tokens (input + output) by model"),
    RouteInfo(path="/api/usage-by-hour",                 description="Total event count by UTC hour of day (0–23, all hours always present)"),
    RouteInfo(path="/api/usage-by-day-of-week",          description="Total event count by day of week (Mon–Sun, all 7 days always present)"),
    RouteInfo(path="/api/tool-usage",                    description="Tool decision counts and approval rates, ordered by volume"),
    RouteInfo(path="/api/top-users",                     description="Top N users by API spend with employee details (limit param, 1–100, default 10)"),
    RouteInfo(path="/api/sessions-by-practice",          description="Session count and average/max duration by practice"),
    RouteInfo(path="/api/requests-by-model-and-practice", description="API request count broken down by every (practice, model) combination"),
    RouteInfo(path="/api/cache-stats",                   description="Cache read/creation token counts and hit ratio percentage by model"),
]

# Shared 422 response documented on every endpoint.
_422 = {422: {"description": "Validation error — invalid query parameter value or combination"}}

# ── Meta endpoints ─────────────────────────────────────────────────────────────

@app.get("/", response_model=RootResponse, tags=["meta"])
def root():
    """List all available API routes with one-line descriptions."""
    return RootResponse(title="Claude Code Analytics API", version="1.0.0", routes=_ROUTES)


@app.get("/api/summary", response_model=SummaryStats, responses=_422, tags=["meta"])
def api_summary(conn: DBConn, filters: CommonFilters):
    """
    Top-level metric cards matching the four numbers at the top of the dashboard.

    Returns a single object (not a list) with total_events, total_api_cost_usd,
    unique_users, and unique_sessions, all scoped to the active filters.
    """
    rows = _records(summary_stats(conn, filters))
    return rows[0] if rows else SummaryStats(
        total_events=0, total_api_cost_usd=0.0, unique_users=0, unique_sessions=0
    )


@app.get("/api/filters", response_model=FiltersResponse, tags=["meta"])
def api_filters(conn: DBConn):
    """
    Return all available filter values for populating client-side dropdowns.

    The response contains the full list of practices, engineer levels, and office
    locations present in the employees table, plus the earliest and latest event
    dates in the telemetry data.
    """
    return FiltersResponse(**get_filters(conn))


# ── Cost endpoints ─────────────────────────────────────────────────────────────

@app.get(
    "/api/cost-by-practice",
    response_model=list[CostByPracticeRow],
    responses=_422,
    tags=["cost"],
)
def api_cost_by_practice(conn: DBConn, filters: CommonFilters):
    """
    Total API cost and token breakdown grouped by practice team.

    The practice is resolved from the employees table when available, falling
    back to the `resource_user_practice` field embedded in each telemetry event.
    Rows are ordered by total cost descending.
    """
    return _records(cost_by_practice(conn, filters))


@app.get(
    "/api/cost-by-model",
    response_model=list[CostByModelRow],
    responses=_422,
    tags=["cost"],
)
def api_cost_by_model(conn: DBConn, filters: CommonFilters):
    """
    Total API cost, token usage, and cost share percentage, one row per model.

    `cost_share_pct` is each model's fraction of total spend across all models
    in the filtered result, useful for understanding model mix.
    Rows are ordered by total cost descending.
    """
    return _records(cost_by_model(conn, filters))


@app.get(
    "/api/daily-cost",
    response_model=list[DailyCostRow],
    responses=_422,
    tags=["cost"],
)
def api_daily_cost(conn: DBConn, filters: CommonFilters):
    """
    Daily API cost time series with zero-filled gaps.

    Returns one row per calendar day (UTC) between the first and last event date
    in the filtered result. Days with no activity are included with cost 0.0 and
    api_requests 0 so charting libraries receive a continuous series.
    """
    df = daily_cost(conn, filters)
    if df.empty:
        return []
    df["date"] = df["date"].dt.strftime("%Y-%m-%d")
    return _records(df)


@app.get(
    "/api/model-efficiency",
    response_model=list[ModelEfficiencyRow],
    responses=_422,
    tags=["cost"],
)
def api_model_efficiency(conn: DBConn, filters: CommonFilters):
    """
    Per-model comparison of request volume, cost, and average cost per request.

    `pct_requests` and `pct_cost` show each model's share of totals within the
    filtered result, enabling cost-efficiency comparisons across models.
    Rows are ordered by total cost descending.
    """
    return _records(model_efficiency(conn, filters))


@app.get(
    "/api/cost-per-token",
    response_model=list[CostPerTokenRow],
    responses=_422,
    tags=["cost"],
)
def api_cost_per_token(conn: DBConn, filters: CommonFilters):
    """
    Cost per 1,000 tokens (input + output combined) for each model.

    Models where total token count is zero are excluded. This metric normalises
    cost across models with different usage volumes and is useful for comparing
    the pricing efficiency of each model.
    Rows are ordered by cost_per_1k_tokens descending (most expensive first).
    """
    return _records(cost_per_token(conn, filters))


# ── Usage endpoints ────────────────────────────────────────────────────────────

@app.get(
    "/api/usage-by-hour",
    response_model=list[UsageByHourRow],
    responses=_422,
    tags=["usage"],
)
def api_usage_by_hour(conn: DBConn, filters: CommonFilters):
    """
    Total event count by UTC hour of day across all event types.

    Always returns exactly 24 rows (hours 0–23). Hours with no events in the
    filtered range are included with event_count 0, making it safe to plot
    directly as a histogram without client-side gap-filling.
    """
    return _records(events_by_hour(conn, filters))


@app.get(
    "/api/usage-by-day-of-week",
    response_model=list[EventsByDayRow],
    responses=_422,
    tags=["usage"],
)
def api_usage_by_day_of_week(conn: DBConn, filters: CommonFilters):
    """
    Total event count by day of the week across all event types.

    Always returns exactly 7 rows in Mon-first order (Mon, Tue, …, Sun). Days
    with no events in the filtered range are included with event_count 0.
    """
    return _records(events_by_day_of_week(conn, filters))


@app.get(
    "/api/tool-usage",
    response_model=list[ToolUsageRow],
    responses=_422,
    tags=["usage"],
)
def api_tool_usage(conn: DBConn, filters: CommonFilters):
    """
    Tool decision counts and approval rates from tool_decision events.

    `accept_rate_pct` is the percentage of decisions where the user approved the
    tool call. Rows are ordered by total_decisions descending so the most-used
    tools appear first.
    """
    return _records(tool_usage(conn, filters))


# ── User / team endpoints ──────────────────────────────────────────────────────

@app.get(
    "/api/top-users",
    response_model=list[TopUserRow],
    responses=_422,
    tags=["users"],
)
def api_top_users(
    conn: DBConn,
    filters: CommonFilters,
    limit: Annotated[
        int,
        Query(
            ge=1,
            le=100,
            description="Number of users to return, ranked by total API spend. Min 1, max 100.",
        ),
    ] = 10,
):
    """
    Top N users by total API spend, enriched with employee details.

    `full_name`, `practice`, and `level` are sourced from the employees table.
    `practice` falls back to `resource_user_practice` for users not in the
    employees table. Rows are ordered by total_cost_usd descending.
    """
    return _records(top_users_by_cost(conn, n=limit, filters=filters))


@app.get(
    "/api/sessions-by-practice",
    response_model=list[SessionsByPracticeRow],
    responses=_422,
    tags=["users"],
)
def api_sessions_by_practice(conn: DBConn, filters: CommonFilters):
    """
    Session count and duration statistics by practice team.

    A session spans from its first to its last event timestamp. Sessions with
    only one event have duration 0. `avg_session_duration_min` and
    `max_session_duration_min` are in minutes.
    Rows are ordered by session_count descending.
    """
    return _records(sessions_by_practice(conn, filters))


# ── Token / cache endpoints ────────────────────────────────────────────────────

@app.get(
    "/api/requests-by-model-and-practice",
    response_model=list[RequestsByModelAndPracticeRow],
    responses=_422,
    tags=["tokens"],
)
def api_requests_by_model_and_practice(conn: DBConn, filters: CommonFilters):
    """
    API request count for every (practice, model) combination.

    Drives the stacked bar chart showing how each practice distributes its usage
    across models. Practice resolves from the employees table, falling back to
    the resource_user_practice field. Ordered by practice then requests descending.
    """
    return _records(requests_by_model_and_practice(conn, filters))


@app.get(
    "/api/cache-stats",
    response_model=list[CacheStatsByModelRow],
    responses=_422,
    tags=["tokens"],
)
def api_cache_stats(conn: DBConn, filters: CommonFilters):
    """
    Cache read and creation token counts per model, with hit ratio.

    `hit_ratio_pct` = cache_read / (cache_read + cache_creation) × 100. A high
    hit ratio means the model is reusing cached prompt prefixes effectively.
    `hit_ratio_pct` is null when total cache tokens is zero.
    Rows are ordered by total_cache_tokens descending.
    """
    return _records(cache_stats_by_model(conn, filters))
