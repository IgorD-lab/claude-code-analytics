# LLM Usage Log

## Tools Used

- Claude.ai (chat) — requirements analysis, architecture decisions, data structure analysis
- Claude Code (CLI) — code generation, validation scripts, pipeline, analytics, dashboard, API

---

## Entry 1 — Requirements analysis and planning

**Tool:** Claude chat
**Prompt:** Provided assignment spec and raw data examples. Mapped out phased plan: validate → explore → ingest → analyze → dashboard.
**Result:** Identified JSONL double-parse requirement, 5 event types with distinct schemas. Chose SQLite + Streamlit + FastAPI architecture.
**Validation:** Manually inspected raw JSONL in a JSON viewer to confirm nesting structure.

---

## Entry 2 — Data validation and exploration

**Tool:** Claude Code
**Prompt:** Series of scripts to: flatten and validate all events (nulls, duplicates, timestamp range), print one sample per event type, and verify outer batch fields are uniform.
**Result:** 454,428 events, zero parse errors, zero duplicates. Confirmed 5 event types with distinct field sets. Batch wrapper uniform across all 82,661 batches.
**Validation:** Cross-referenced null percentages against event type proportions. Verified batch uniformity with code rather than assuming.

---

## Entry 3 — Ingestion pipeline and schema design

**Tool:** Claude Code
**Prompt:** Build ingestion script (flatten, type-convert, validate, load to SQLite with indexes). Separately, evaluate single-table vs split-table schema.
**Result:** Full pipeline with post-ingestion integrity checks and three indexes. Chose single table — cross-type queries would require UNION ALL with split tables, and SQLite doesn't store NULLs so sparse columns are free.
**Validation:** Verification script confirmed row counts and correct types. Visual inspection in DB Browser for SQLite.

---

## Entry 4 — Analytics module

**Tool:** Claude Code
**Prompt:** Generate analytics functions returning DataFrames for cost by practice, cost by model, usage by hour, tool success rates, top users, daily cost, and session stats.
**Result:** Key findings: Haiku handles 39% of requests for 3.1% of cost. Haiku shows 8:1 input/output ratio (triage) vs Opus at 1:1.4 (generation). ML Engineering leads spend.
**Validation:** Added print statements to each function and verified output against known model pricing and expected patterns.

---

## Entry 5 — Dashboard and bug discovery

**Tool:** Claude Code + manual SQL verification
**Prompt:** Built 4-tab Streamlit dashboard with sidebar filters and metric cards. Iteratively added model efficiency, cost-per-token, and cache hit ratio charts.
**Result:** Caught two bugs through manual verification:

1. SQLite threading error on filter use — fixed with `check_same_thread=False`
2. SQL alias `day` collided with existing integer column, collapsing Dec+Jan into same groups. Dashboard was correct; manual verification query had the bug.
3. Timestamp formatting inconsistency: 5,445 rows stored without microseconds causing silent NaT values in pandas.
   **Validation:** Compared all chart values against direct SQL queries. Total cost $6,001.43 across 60 days confirmed matching.

---

## Entry 6 — FastAPI REST API

**Tool:** Claude Code
**Prompt:** Create FastAPI app wrapping all analytics functions. Use Pydantic response models, Enum-based filter parameters (practice, level, location), date validation, and shared filter dependency. Ensure every dashboard chart has a matching API endpoint.
**Result:** 16 endpoints with tagged grouping (meta, cost, usage, users, tokens), interactive Swagger docs at /docs and ReDoc at /redoc. All filters match dashboard sidebar.
**Validation:** Tested endpoints against dashboard values — numbers match. Verified Swagger UI shows proper dropdowns and descriptions.

---

## Entry 7 — Documentation

**Tool:** Claude Code
**Prompt:** Generate README with Quick Start, architecture overview, API endpoint listing, and bonus features section. Separate schema design doc with trade-off analysis.
**Result:** Concise README with working end-to-end setup. Schema design documents single-table rationale with full DDL.
**Validation:** Ran Quick Start from clean clone to verify all steps work.
