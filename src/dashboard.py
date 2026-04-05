"""
Streamlit dashboard for Claude Code Usage Analytics.

Run with:
    streamlit run src/dashboard.py
"""

import sys
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

sys.path.insert(0, str(Path(__file__).parent.parent))

# ── Page config ───────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="Claude Code Usage Analytics",
    page_icon="📊",
    layout="wide",
)

st.title("Claude Code Usage Analytics")

# ── DB connection ─────────────────────────────────────────────────────────────
# A new connection is created per call with check_same_thread=False so Streamlit's
# threading model doesn't raise "objects created in a thread can only be used in
# that same thread".

def new_conn():
    from pathlib import Path
    import sqlite3 as _sqlite3
    db_path = Path(__file__).parent.parent / "data" / "processed" / "telemetry.db"
    return _sqlite3.connect(f"file:{db_path}?mode=ro", uri=True,
                            check_same_thread=False)


# ── Raw filtered data (cached per filter state) ───────────────────────────────

@st.cache_data(ttl=300)
def load_base(date_min, date_max, practices, levels, locations):
    """Return the filtered events + employees joined DataFrame."""
    params = [date_min, date_max]
    practice_clause = ""
    level_clause = ""
    location_clause = ""
    if practices:
        placeholders = ",".join("?" * len(practices))
        practice_clause = f"AND COALESCE(emp.practice, e.resource_user_practice) IN ({placeholders})"
        params += list(practices)
    if levels:
        placeholders = ",".join("?" * len(levels))
        level_clause = f"AND emp.level IN ({placeholders})"
        params += list(levels)
    if locations:
        placeholders = ",".join("?" * len(locations))
        location_clause = f"AND emp.location IN ({placeholders})"
        params += list(locations)

    sql = f"""
        SELECT
            e.*,
            emp.full_name,
            emp.practice,
            emp.level,
            emp.location
        FROM events e
        LEFT JOIN employees emp ON e.user_email = emp.email
        WHERE DATE(e.event_timestamp) BETWEEN ? AND ?
          {practice_clause}
          {level_clause}
          {location_clause}
    """
    with new_conn() as c:
        return pd.read_sql(sql, c, params=params,
                           parse_dates={"event_timestamp": {"format": "ISO8601", "utc": True}})


# ── Sidebar filters ───────────────────────────────────────────────────────────

with st.sidebar:
    st.header("Filters")

    with new_conn() as _c:
        date_bounds = _c.execute(
            "SELECT MIN(DATE(event_timestamp)), MAX(DATE(event_timestamp)) FROM events"
        ).fetchone()
        all_practices = [r[0] for r in _c.execute(
            "SELECT DISTINCT practice FROM employees ORDER BY practice"
        )]
        all_levels = [r[0] for r in _c.execute(
            "SELECT DISTINCT level FROM employees ORDER BY level"
        )]
        all_locations = [r[0] for r in _c.execute(
            "SELECT DISTINCT location FROM employees ORDER BY location"
        )]

    d_min = pd.to_datetime(date_bounds[0]).date()
    d_max = pd.to_datetime(date_bounds[1]).date()

    date_range = st.date_input(
        "Date range",
        value=(d_min, d_max),
        min_value=d_min,
        max_value=d_max,
    )
    if isinstance(date_range, (list, tuple)) and len(date_range) == 2:
        sel_min, sel_max = str(date_range[0]), str(date_range[1])
    else:
        sel_min, sel_max = str(d_min), str(d_max)

    sel_practices = st.multiselect("Practice", all_practices, default=[])
    sel_levels = st.multiselect("Engineer level", all_levels, default=[])
    sel_locations = st.multiselect("Location", all_locations, default=[])

df = load_base(sel_min, sel_max, tuple(sel_practices), tuple(sel_levels), tuple(sel_locations))

api   = df[df["body"] == "claude_code.api_request"]
tools = df[df["body"] == "claude_code.tool_decision"]
prompts = df[df["body"] == "claude_code.user_prompt"]

# ── Metric cards ──────────────────────────────────────────────────────────────

m1, m2, m3, m4 = st.columns(4)
m1.metric("Total events",    f"{len(df):,}")
m2.metric("Total API cost",  f"${api['cost_usd'].sum():,.2f}")
m3.metric("Unique users",    f"{df['user_email'].nunique():,}")
m4.metric("Unique sessions", f"{df['session_id'].nunique():,}")

st.divider()

# ── Tabs ──────────────────────────────────────────────────────────────────────

tab1, tab2, tab3, tab4 = st.tabs(
    ["💰 Cost Overview", "📈 Usage Patterns", "👥 Team & User Insights", "🔢 Token Analysis"]
)

# ════════════════════════════════════════════════════════════════════════════
# Tab 1 — Cost Overview
# ════════════════════════════════════════════════════════════════════════════
with tab1:
    # Compute both datasets first so height can be shared
    practice_cost = (
        api.assign(practice=lambda d: d["practice"].fillna(d["resource_user_practice"]))
        .groupby("practice", dropna=False)["cost_usd"]
        .sum()
        .reset_index()
        .sort_values("cost_usd", ascending=True)
    )
    model_cost = (
        api.groupby("model")["cost_usd"]
        .sum()
        .reset_index()
        .sort_values("cost_usd", ascending=True)
    )
    bar_height = max(220, max(len(practice_cost), len(model_cost)) * 52)

    col_a, col_b = st.columns(2)

    # Cost by practice
    with col_a:
        st.subheader("Cost by Practice")
        fig = px.bar(
            practice_cost, x="cost_usd", y="practice", orientation="h",
            labels={"cost_usd": "Total Cost (USD)", "practice": ""},
            color="cost_usd", color_continuous_scale="Blues",
            custom_data=["cost_usd"],
        )
        fig.update_traces(hovertemplate="<b>%{y}</b><br>Cost: $%{customdata[0]:,.2f}<extra></extra>")
        fig.update_layout(coloraxis_showscale=False, margin=dict(l=0),
                          xaxis=dict(tickprefix="$", tickformat=",.2f"),
                          height=bar_height)
        st.plotly_chart(fig, use_container_width=True)

    # Cost by model
    with col_b:
        st.subheader("Cost by Model")
        fig = px.bar(
            model_cost, x="cost_usd", y="model", orientation="h",
            labels={"cost_usd": "Total Cost (USD)", "model": ""},
            color="cost_usd", color_continuous_scale="Purples",
            custom_data=["cost_usd"],
        )
        fig.update_traces(hovertemplate="<b>%{y}</b><br>Cost: $%{customdata[0]:,.2f}<extra></extra>")
        fig.update_layout(coloraxis_showscale=False, margin=dict(l=0),
                          xaxis=dict(tickprefix="$", tickformat=",.2f"),
                          height=bar_height)
        st.plotly_chart(fig, use_container_width=True)

    # Model efficiency table (new — no existing chart covers %, avg cost/req)
    st.subheader("Model Efficiency")
    if api.empty:
        st.info("No API request events in the selected range.")
    else:
        total_req = len(api)
        total_cost = api["cost_usd"].sum()
        model_eff = (
            api.groupby("model")
            .agg(requests=("event_id", "count"), total_cost=("cost_usd", "sum"),
                 total_tokens_in=("input_tokens", "sum"),
                 total_tokens_out=("output_tokens", "sum"))
            .reset_index()
            .assign(
                pct_requests=lambda d: (d["requests"] / total_req * 100).round(1),
                pct_cost=lambda d: (d["total_cost"] / total_cost * 100).round(1),
                avg_cost_per_req=lambda d: d["total_cost"] / d["requests"],
            )
            .sort_values("total_cost", ascending=False)
            .reset_index(drop=True)
        )
        display_eff = model_eff[["model", "requests", "pct_requests", "total_cost",
                                  "pct_cost", "avg_cost_per_req"]].copy()
        display_eff["requests"]       = display_eff["requests"].map("{:,}".format)
        display_eff["pct_requests"]   = display_eff["pct_requests"].map("{:.1f}%".format)
        display_eff["total_cost"]     = display_eff["total_cost"].map("${:,.2f}".format)
        display_eff["pct_cost"]       = display_eff["pct_cost"].map("{:.1f}%".format)
        display_eff["avg_cost_per_req"] = display_eff["avg_cost_per_req"].map("${:.4f}".format)
        display_eff.columns = ["Model", "Requests", "% of Requests",
                                "Total Cost", "% of Cost", "Avg Cost / Request"]
        st.dataframe(display_eff, use_container_width=True, hide_index=True)

    # Daily cost time series (full width)
    st.subheader("Daily Cost Over Time")
    daily = (
        api.dropna(subset=["event_timestamp"])
        .assign(date=lambda d: d["event_timestamp"].dt.date)
        .groupby("date")["cost_usd"]
        .sum()
        .reset_index()
        .sort_values("date")
    )
    # Fill gaps
    if not daily.empty:
        full_range = pd.date_range(daily["date"].min(), daily["date"].max(), freq="D")
        daily = (
            daily.set_index("date")
            .reindex(full_range.date)
            .fillna(0)
            .reset_index()
            .rename(columns={"index": "date"})
        )
    fig = px.line(
        daily, x="date", y="cost_usd",
        labels={"date": "Date", "cost_usd": "Cost (USD)"},
        custom_data=["cost_usd"],
    )
    fig.update_traces(
        line_color="#4f8ef7", fill="tozeroy", fillcolor="rgba(79,142,247,0.1)",
        hovertemplate="<b>%{x}</b><br>Cost: $%{customdata[0]:,.2f}<extra></extra>",
    )
    fig.update_layout(margin=dict(t=0),
                      yaxis=dict(tickprefix="$", tickformat=",.2f"))
    st.plotly_chart(fig, use_container_width=True)


# ════════════════════════════════════════════════════════════════════════════
# Tab 2 — Usage Patterns
# ════════════════════════════════════════════════════════════════════════════
with tab2:
    col_a, col_b = st.columns(2)

    # Events by hour
    with col_a:
        st.subheader("Events by Hour of Day (UTC)")
        hourly = (
            df.dropna(subset=["event_timestamp"])
            .assign(hour=lambda d: d["event_timestamp"].dt.hour)
            .groupby("hour")
            .size()
            .reset_index(name="count")
        )
        hourly = (
            pd.DataFrame({"hour": range(24)})
            .merge(hourly, on="hour", how="left")
            .fillna({"count": 0})
            .astype({"count": int})
        )
        fig = px.bar(
            hourly, x="hour", y="count",
            labels={"hour": "Hour (UTC)", "count": "Event Count"},
            color="count", color_continuous_scale="Teal",
        )
        fig.update_traces(hovertemplate="<b>Hour %{x}</b><br>Events: %{y:,}<extra></extra>")
        fig.update_layout(coloraxis_showscale=False, margin=dict(t=0),
                          yaxis=dict(tickformat=","))
        st.plotly_chart(fig, use_container_width=True)

    # Events by day of week
    with col_b:
        st.subheader("Events by Day of Week")
        dow_map = {0: "Sun", 1: "Mon", 2: "Tue", 3: "Wed", 4: "Thu", 5: "Fri", 6: "Sat"}
        dow_order = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
        weekly = (
            df.dropna(subset=["event_timestamp"])
            .assign(dow=lambda d: d["event_timestamp"].dt.dayofweek.map(
                {0: "Mon", 1: "Tue", 2: "Wed", 3: "Thu", 4: "Fri", 5: "Sat", 6: "Sun"}
            ))
            .groupby("dow")
            .size()
            .reset_index(name="count")
        )
        weekly["dow"] = pd.Categorical(weekly["dow"], categories=dow_order, ordered=True)
        weekly = weekly.sort_values("dow")
        fig = px.bar(
            weekly, x="dow", y="count",
            labels={"dow": "Day", "count": "Event Count"},
            color="count", color_continuous_scale="Teal",
        )
        fig.update_traces(hovertemplate="<b>%{x}</b><br>Events: %{y:,}<extra></extra>")
        fig.update_layout(coloraxis_showscale=False, margin=dict(t=0),
                          yaxis=dict(tickformat=","))
        st.plotly_chart(fig, use_container_width=True)

    # Tool approval rate (full width)
    st.subheader("Tool Approval Rate")
    st.caption("Percentage of tool uses approved vs rejected by the user, and total decision count (top 15 tools)")
    if not tools.empty:
        tool_stats = (
            tools.groupby("tool_name")
            .agg(
                total=("tool_name", "count"),
                accepted=("decision", lambda s: (s == "accept").sum()),
            )
            .reset_index()
            .assign(accept_rate=lambda d: (d["accepted"] / d["total"] * 100).round(1))
            .sort_values("total", ascending=False)
            .head(15)
            .sort_values("total", ascending=True)
        )
        fig = go.Figure()
        fig.add_trace(go.Bar(
            y=tool_stats["tool_name"], x=tool_stats["total"],
            name="Total decisions", orientation="h",
            marker_color="#4f8ef7",
            hovertemplate="<b>%{y}</b><br>Decisions: %{x:,}<extra></extra>",
        ))
        fig.add_trace(go.Scatter(
            y=tool_stats["tool_name"], x=tool_stats["accept_rate"],
            name="Approval rate (%)", mode="markers",
            marker=dict(color="#f77f4f", size=8, symbol="diamond"),
            xaxis="x2",
        ))
        fig.update_layout(
            xaxis=dict(title="Decision count"),
            xaxis2=dict(title="Approval rate (%)", overlaying="x", side="top",
                        range=[90, 100]),
            legend=dict(orientation="h", yanchor="bottom", y=1.02),
            margin=dict(l=0, t=40),
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No tool decision events in selected range.")


# ════════════════════════════════════════════════════════════════════════════
# Tab 3 — Team & User Insights
# ════════════════════════════════════════════════════════════════════════════
with tab3:
    n_users = st.selectbox("Show top N users", options=[10, 25, 50, 100], index=0)
    st.subheader(f"Top {n_users} Users by Cost")
    top_users = (
        api.assign(practice=lambda d: d["practice"].fillna(d["resource_user_practice"]))
        .groupby(["user_email", "full_name", "practice", "level"])
        .agg(api_requests=("event_id", "count"), total_cost=("cost_usd", "sum"))
        .reset_index()
        .sort_values("total_cost", ascending=False)
        .head(n_users)
        .reset_index(drop=True)
    )
    top_users.index += 1
    top_users["total_cost"] = top_users["total_cost"].map("${:,.2f}".format)
    top_users["api_requests"] = top_users["api_requests"].map("{:,}".format)
    top_users.columns = ["Email", "Name", "Practice", "Level", "API Requests", "Total Cost"]
    st.dataframe(top_users, use_container_width=True)

    col_a, col_b = st.columns(2)

    # Session count by practice
    with col_a:
        st.subheader("Session Count by Practice")
        sess_practice = (
            df.assign(practice=lambda d: d["practice"].fillna(d["resource_user_practice"]))
            .dropna(subset=["session_id", "practice"])
            .groupby("practice")["session_id"]
            .nunique()
            .reset_index(name="sessions")
            .sort_values("sessions", ascending=True)
        )
        fig = px.bar(
            sess_practice, x="sessions", y="practice", orientation="h",
            labels={"sessions": "Sessions", "practice": ""},
            color="sessions", color_continuous_scale="Greens",
        )
        fig.update_traces(hovertemplate="<b>%{y}</b><br>Sessions: %{x:,}<extra></extra>")
        fig.update_layout(coloraxis_showscale=False, margin=dict(l=0, t=0),
                          xaxis=dict(tickformat=","),
                          height=max(220, len(sess_practice) * 52))
        st.plotly_chart(fig, use_container_width=True)

    # Avg session duration by practice
    with col_b:
        st.subheader("Avg Session Duration by Practice")
        per_session = (
            df.assign(practice=lambda d: d["practice"].fillna(d["resource_user_practice"]))
            .dropna(subset=["session_id", "practice", "event_timestamp_ms"])
            .groupby(["practice", "session_id"])["event_timestamp_ms"]
            .agg(duration=lambda s: (s.max() - s.min()) / 60000)
            .reset_index()
        )
        avg_duration = (
            per_session.groupby("practice")["duration"]
            .mean()
            .reset_index(name="avg_duration_min")
            .round(1)
            .sort_values("avg_duration_min", ascending=True)
        )
        fig = px.bar(
            avg_duration, x="avg_duration_min", y="practice", orientation="h",
            labels={"avg_duration_min": "Avg Duration (min)", "practice": ""},
            color="avg_duration_min", color_continuous_scale="Greens",
        )
        fig.update_layout(coloraxis_showscale=False, margin=dict(l=0, t=0),
                          height=max(220, len(avg_duration) * 52))
        st.plotly_chart(fig, use_container_width=True)


# ════════════════════════════════════════════════════════════════════════════
# Tab 4 — Token Analysis
# ════════════════════════════════════════════════════════════════════════════
with tab4:
    # Input vs output tokens by model
    st.subheader("Input vs Output Tokens by Model")
    token_model = (
        api.groupby("model")
        .agg(input_tokens=("input_tokens", "sum"), output_tokens=("output_tokens", "sum"))
        .reset_index()
        .sort_values("input_tokens", ascending=False)
    )
    token_model_melted = token_model.melt(
        id_vars="model", value_vars=["input_tokens", "output_tokens"],
        var_name="token_type", value_name="tokens"
    )
    token_model_melted["token_type"] = token_model_melted["token_type"].map(
        {"input_tokens": "Input", "output_tokens": "Output"}
    )
    fig = px.bar(
        token_model_melted, x="model", y="tokens", color="token_type",
        barmode="group",
        labels={"tokens": "Total Tokens", "model": "Model", "token_type": ""},
        color_discrete_map={"Input": "#4f8ef7", "Output": "#f7a44f"},
    )
    fig.update_traces(hovertemplate="<b>%{x}</b><br>%{fullData.name}: %{y:,}<extra></extra>")
    fig.update_layout(margin=dict(t=0), yaxis=dict(tickformat=","))
    st.plotly_chart(fig, use_container_width=True)

    col_a, col_b = st.columns(2)

    # Cost-per-token by model (new — not covered by any existing chart)
    with col_a:
        st.subheader("Cost per 1K Tokens by Model")
        if api.empty:
            st.info("No API request data in the selected range.")
        else:
            cpt = (
                api.groupby("model")
                .agg(cost=("cost_usd", "sum"),
                     tok_in=("input_tokens", "sum"),
                     tok_out=("output_tokens", "sum"))
                .reset_index()
                .assign(
                    total_tokens=lambda d: d["tok_in"] + d["tok_out"],
                    cost_per_1k=lambda d: (
                        d["cost"] / d["total_tokens"] * 1000
                    ).where(d["total_tokens"] > 0),
                )
                .dropna(subset=["cost_per_1k"])
                .sort_values("cost_per_1k", ascending=True)
            )
            if len(cpt) == 1:
                st.metric(
                    f"Cost per 1K tokens — {cpt.iloc[0]['model']}",
                    f"${cpt.iloc[0]['cost_per_1k']:.4f}",
                )
            else:
                fig = px.bar(
                    cpt, x="cost_per_1k", y="model", orientation="h",
                    labels={"cost_per_1k": "Cost per 1K tokens (USD)", "model": ""},
                    color="cost_per_1k", color_continuous_scale="Reds",
                    custom_data=["cost_per_1k"],
                )
                fig.update_traces(
                    hovertemplate="<b>%{y}</b><br>$%{customdata[0]:.4f} / 1K tokens<extra></extra>"
                )
                fig.update_layout(coloraxis_showscale=False, margin=dict(l=0, t=0),
                                  xaxis=dict(tickprefix="$", tickformat=".4f"),
                                  height=max(220, len(cpt) * 52))
                st.plotly_chart(fig, use_container_width=True)

    # Model usage by practice (new — existing token-by-practice chart doesn't break by model)
    with col_b:
        st.subheader("API Requests by Model and Practice")
        if api.empty:
            st.info("No API request data in the selected range.")
        else:
            mbp = (
                api.assign(practice=lambda d: d["practice"].fillna(d["resource_user_practice"]))
                .dropna(subset=["model", "practice"])
                .groupby(["practice", "model"])
                .size()
                .reset_index(name="requests")
            )
            n_practices = mbp["practice"].nunique()
            fig = px.bar(
                mbp, x="requests", y="practice", color="model",
                orientation="h", barmode="stack",
                labels={"requests": "API Requests", "practice": "", "model": "Model"},
                custom_data=["model", "requests"],
            )
            fig.update_traces(
                hovertemplate="<b>%{y}</b> — %{customdata[0]}<br>Requests: %{customdata[1]:,}<extra></extra>"
            )
            fig.update_layout(margin=dict(l=0, t=0),
                              xaxis=dict(tickformat=","),
                              height=max(220, n_practices * 52),
                              legend=dict(orientation="h", yanchor="bottom", y=1.02))
            st.plotly_chart(fig, use_container_width=True)

    col_a, col_b = st.columns(2)

    # Token usage by practice
    with col_a:
        st.subheader("Token Usage by Practice")
        token_practice = (
            api.assign(practice=lambda d: d["practice"].fillna(d["resource_user_practice"]))
            .groupby("practice")
            .agg(input_tokens=("input_tokens", "sum"), output_tokens=("output_tokens", "sum"))
            .reset_index()
            .sort_values("input_tokens", ascending=False)
        )
        tp_melted = token_practice.melt(
            id_vars="practice", value_vars=["input_tokens", "output_tokens"],
            var_name="token_type", value_name="tokens"
        )
        tp_melted["token_type"] = tp_melted["token_type"].map(
            {"input_tokens": "Input", "output_tokens": "Output"}
        )
        fig = px.bar(
            tp_melted, x="practice", y="tokens", color="token_type",
            barmode="group",
            labels={"tokens": "Total Tokens", "practice": "", "token_type": ""},
            color_discrete_map={"Input": "#4f8ef7", "Output": "#f7a44f"},
        )
        fig.update_traces(hovertemplate="<b>%{x}</b><br>%{fullData.name}: %{y:,}<extra></extra>")
        fig.update_layout(margin=dict(t=0, b=80), yaxis=dict(tickformat=","))
        st.plotly_chart(fig, use_container_width=True)

    # Cache hit ratio by model
    with col_b:
        st.subheader("Cache Hit Ratio by Model")
        cache = (
            api.groupby("model")
            .agg(
                cache_read=("cache_read_tokens", "sum"),
                cache_create=("cache_creation_tokens", "sum"),
            )
            .reset_index()
            .assign(
                total_cache=lambda d: d["cache_read"] + d["cache_create"],
                hit_ratio=lambda d: (d["cache_read"] / d["total_cache"].replace(0, float("nan")) * 100).round(1),
            )
            .sort_values("hit_ratio", ascending=True)
        )
        cache_melted = cache.melt(
            id_vars="model", value_vars=["cache_read", "cache_create"],
            var_name="cache_type", value_name="tokens"
        )
        cache_melted["cache_type"] = cache_melted["cache_type"].map(
            {"cache_read": "Cache Read (hit)", "cache_create": "Cache Write (miss)"}
        )
        fig = px.bar(
            cache_melted, x="tokens", y="model", color="cache_type",
            orientation="h", barmode="stack",
            labels={"tokens": "Tokens", "model": "", "cache_type": ""},
            color_discrete_map={
                "Cache Read (hit)": "#2ecc71",
                "Cache Write (miss)": "#e67e22",
            },
        )
        fig.update_traces(hovertemplate="<b>%{y}</b><br>%{fullData.name}: %{x:,}<extra></extra>")
        # Annotate hit ratios
        for _, row in cache.iterrows():
            if pd.notna(row["hit_ratio"]):
                fig.add_annotation(
                    x=row["total_cache"], y=row["model"],
                    text=f"  {row['hit_ratio']}% hit",
                    showarrow=False, xanchor="left", font=dict(size=11),
                )
        fig.update_layout(margin=dict(l=0, t=0, r=120))
        st.plotly_chart(fig, use_container_width=True)