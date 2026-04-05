"""
models.py — Pydantic response models and filter enums for the Claude Code Analytics REST API.

Each response model maps 1:1 to the columns returned by the corresponding analytics.py function.
Enums are derived from the actual values in the database and drive Swagger UI dropdowns.
"""

from enum import Enum

from pydantic import BaseModel


# ── Filter enums (sourced from the employees table) ────────────────────────────

class Practice(str, Enum):
    backend  = "Backend Engineering"
    data     = "Data Engineering"
    frontend = "Frontend Engineering"
    ml       = "ML Engineering"
    platform = "Platform Engineering"


class Level(str, Enum):
    l1  = "L1"
    l2  = "L2"
    l3  = "L3"
    l4  = "L4"
    l5  = "L5"
    l6  = "L6"
    l7  = "L7"
    l8  = "L8"
    l9  = "L9"
    l10 = "L10"


class Location(str, Enum):
    canada  = "Canada"
    germany = "Germany"
    poland  = "Poland"
    uk      = "United Kingdom"
    us      = "United States"


# ── Cost endpoints ─────────────────────────────────────────────────────────────

class CostByPracticeRow(BaseModel):
    practice: str | None
    api_requests: int
    total_cost_usd: float | None
    total_input_tokens: int | None
    total_output_tokens: int | None
    total_cache_creation_tokens: int | None
    total_cache_read_tokens: int | None

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "practice": "ML Engineering",
                    "api_requests": 4821,
                    "total_cost_usd": 142.3812,
                    "total_input_tokens": 18234521,
                    "total_output_tokens": 3421098,
                    "total_cache_creation_tokens": 892341,
                    "total_cache_read_tokens": 5234198,
                }
            ]
        }
    }


class CostByModelRow(BaseModel):
    model: str
    api_requests: int
    total_cost_usd: float | None
    total_input_tokens: int | None
    total_output_tokens: int | None
    total_cache_read_tokens: int | None
    avg_cost_per_request: float | None
    avg_duration_ms: float | None
    cost_share_pct: float | None

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "model": "claude-3-5-sonnet-20241022",
                    "api_requests": 9234,
                    "total_cost_usd": 287.4421,
                    "total_input_tokens": 34123098,
                    "total_output_tokens": 6234109,
                    "total_cache_read_tokens": 9821034,
                    "avg_cost_per_request": 0.031129,
                    "avg_duration_ms": 4823.0,
                    "cost_share_pct": 68.3,
                }
            ]
        }
    }


class DailyCostRow(BaseModel):
    date: str          # ISO date: "YYYY-MM-DD"
    total_cost_usd: float
    api_requests: int

    model_config = {
        "json_schema_extra": {
            "examples": [
                {"date": "2025-12-15", "total_cost_usd": 18.7234, "api_requests": 612}
            ]
        }
    }


class ModelEfficiencyRow(BaseModel):
    model: str
    requests: int
    total_cost_usd: float | None
    total_input_tokens: int | None
    total_output_tokens: int | None
    avg_cost_per_request: float | None
    avg_duration_ms: float | None
    pct_requests: float | None
    pct_cost: float | None

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "model": "claude-3-5-sonnet-20241022",
                    "requests": 9234,
                    "total_cost_usd": 287.4421,
                    "total_input_tokens": 34123098,
                    "total_output_tokens": 6234109,
                    "avg_cost_per_request": 0.031129,
                    "avg_duration_ms": 4823.0,
                    "pct_requests": 72.1,
                    "pct_cost": 68.3,
                }
            ]
        }
    }


class CostPerTokenRow(BaseModel):
    model: str
    total_cost_usd: float | None
    total_input_tokens: int | None
    total_output_tokens: int | None
    total_tokens: int | None
    cost_per_1k_tokens: float | None

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "model": "claude-3-5-sonnet-20241022",
                    "total_cost_usd": 287.4421,
                    "total_input_tokens": 34123098,
                    "total_output_tokens": 6234109,
                    "total_tokens": 40357207,
                    "cost_per_1k_tokens": 0.007122,
                }
            ]
        }
    }


# ── Usage endpoints ────────────────────────────────────────────────────────────

class UsageByHourRow(BaseModel):
    hour_utc: int
    event_count: int

    model_config = {
        "json_schema_extra": {
            "examples": [{"hour_utc": 14, "event_count": 3821}]
        }
    }


class ToolUsageRow(BaseModel):
    tool_name: str
    total_decisions: int
    accepted: int
    rejected: int
    accept_rate_pct: float | None

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "tool_name": "Bash",
                    "total_decisions": 28341,
                    "accepted": 27903,
                    "rejected": 438,
                    "accept_rate_pct": 98.5,
                }
            ]
        }
    }


# ── User / team endpoints ──────────────────────────────────────────────────────

class TopUserRow(BaseModel):
    user_email: str
    full_name: str | None
    practice: str | None
    level: str | None
    api_requests: int
    total_cost_usd: float | None
    total_input_tokens: int | None
    total_output_tokens: int | None

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "user_email": "alice@example.com",
                    "full_name": "Alice Nguyen",
                    "practice": "ML Engineering",
                    "level": "L5",
                    "api_requests": 1823,
                    "total_cost_usd": 54.2109,
                    "total_input_tokens": 7234109,
                    "total_output_tokens": 1234098,
                }
            ]
        }
    }


class SessionsByPracticeRow(BaseModel):
    practice: str
    session_count: int
    avg_session_duration_min: float | None
    max_session_duration_min: float | None

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "practice": "ML Engineering",
                    "session_count": 1243,
                    "avg_session_duration_min": 37.4,
                    "max_session_duration_min": 312.8,
                }
            ]
        }
    }


# ── Summary / metric-card endpoint ────────────────────────────────────────────

class SummaryStats(BaseModel):
    total_events: int
    total_api_cost_usd: float | None
    unique_users: int
    unique_sessions: int

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "total_events": 454821,
                    "total_api_cost_usd": 420.1834,
                    "unique_users": 98,
                    "unique_sessions": 4923,
                }
            ]
        }
    }


# ── Usage endpoints (day-of-week) ──────────────────────────────────────────────

class EventsByDayRow(BaseModel):
    day_of_week: str   # "Mon" … "Sun", always Mon-first order
    event_count: int

    model_config = {
        "json_schema_extra": {
            "examples": [{"day_of_week": "Wed", "event_count": 87431}]
        }
    }


# ── Token / cache endpoints ────────────────────────────────────────────────────

class RequestsByModelAndPracticeRow(BaseModel):
    practice: str
    model: str
    requests: int

    model_config = {
        "json_schema_extra": {
            "examples": [
                {"practice": "ML Engineering", "model": "claude-3-5-sonnet-20241022", "requests": 2341}
            ]
        }
    }


class CacheStatsByModelRow(BaseModel):
    model: str
    cache_read_tokens: int | None
    cache_creation_tokens: int | None
    total_cache_tokens: int | None
    hit_ratio_pct: float | None

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "model": "claude-3-5-sonnet-20241022",
                    "cache_read_tokens": 9821034,
                    "cache_creation_tokens": 1234098,
                    "total_cache_tokens": 11055132,
                    "hit_ratio_pct": 88.9,
                }
            ]
        }
    }


# ── Meta endpoints ─────────────────────────────────────────────────────────────

class FiltersResponse(BaseModel):
    practices: list[str]
    levels: list[str]
    locations: list[str]
    date_min: str | None
    date_max: str | None

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "practices": [
                        "Backend Engineering", "Data Engineering",
                        "Frontend Engineering", "ML Engineering", "Platform Engineering",
                    ],
                    "levels": ["L1", "L2", "L3", "L4", "L5", "L6", "L7", "L8", "L9", "L10"],
                    "locations": ["Canada", "Germany", "Poland", "United Kingdom", "United States"],
                    "date_min": "2025-12-03",
                    "date_max": "2026-01-31",
                }
            ]
        }
    }


class RouteInfo(BaseModel):
    path: str
    description: str


class RootResponse(BaseModel):
    title: str
    version: str
    routes: list[RouteInfo]
