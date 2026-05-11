"""
Model Diagnostics — per-feature validation for the 5 model improvements.
Each tab confirms one feature is firing correctly with observable evidence.
"""
import sys
import json
import sqlite3
import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import requests
import streamlit as st
from pathlib import Path

ROOT    = Path(__file__).resolve().parents[2]
DB_PATH = ROOT / "outputs" / "projections_history.db"

sys.path.insert(0, str(ROOT / "scripts"))
from utils.auth import require_auth, show_logout_button

# Mirror constants from player_model.py so we can reproduce calculations here
MIN_SEASON_MINS = 270
XG_SHRINK       = 0.75
RATE_SHRINK     = 0.60
FORM_WEIGHT     = 0.35
POS_AVG_G_PER90 = {"G": 0.01, "D": 0.04, "M": 0.12, "F": 0.30}
POS_AVG_A_PER90 = {"G": 0.01, "D": 0.05, "M": 0.20, "F": 0.12}
POS_MAP         = {1: "G", 2: "D", 3: "M", 4: "F"}
POS_LABEL       = {"G": "GK", "D": "DEF", "M": "MID", "F": "FWD"}

st.set_page_config(page_title="FPL – Model Diagnostics", layout="wide", page_icon="🔬")
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Barlow+Condensed:wght@400;600;700;800&family=Barlow:wght@400;500;600&display=swap');
html, body, [class*="css"], .stApp { font-family: 'Barlow', sans-serif !important; }
h1, h2, h3 { font-family: 'Barlow Condensed', sans-serif !important; font-weight: 800 !important; }
.block-container { padding-left: 1rem !important; padding-right: 1rem !important; }
</style>
""", unsafe_allow_html=True)

require_auth()
show_logout_button()

if st.button("← Back"):
    st.switch_page("fpl_app.py")

st.title("🔬 Model Diagnostics")
st.caption("Validate that each model improvement is firing correctly — observable evidence per feature.")


# ── Data loaders ──────────────────────────────────────────────────────────────

@st.cache_data(ttl=1800)
def fetch_bootstrap():
    r = requests.get(
        "https://fantasy.premierleague.com/api/bootstrap-static/",
        timeout=30, headers={"User-Agent": "FPL-Model/1.0"}
    )
    r.raise_for_status()
    return r.json()


@st.cache_data(ttl=300)
def load_latest_projections():
    """Load latest projections for upcoming (non-finished) GWs only."""
    with sqlite3.connect(str(DB_PATH)) as conn:
        # Find the most recent real poll timestamp
        row = conn.execute("""
            SELECT MAX(timestamp) FROM player_projection_model
            WHERE timestamp != '0000-backfill'
        """).fetchone()
        if not row or not row[0]:
            return pd.DataFrame()
        latest_ts = row[0]

        # GWs included in that poll (already had started GWs stripped out)
        return pd.read_sql("""
            SELECT p.*
            FROM player_projection_model p
            INNER JOIN (
                SELECT fpl_id, gw, MAX(timestamp) AS max_ts
                FROM player_projection_model
                WHERE timestamp != '0000-backfill'
                GROUP BY fpl_id, gw
            ) l ON p.fpl_id = l.fpl_id AND p.gw = l.gw AND p.timestamp = l.max_ts
            INNER JOIN fpl_gw_events e ON p.gw = e.gw
            WHERE (e.finished = 0 OR e.finished IS NULL)
            ORDER BY p.gw, p.xpts DESC
        """, conn)


@st.cache_data(ttl=300)
def load_recent_form(n_gws=5):
    with sqlite3.connect(str(DB_PATH)) as conn:
        recent_gws = [r[0] for r in conn.execute(
            "SELECT gw FROM fpl_gw_events WHERE finished=1 ORDER BY gw DESC LIMIT ?",
            (n_gws,)
        ).fetchall()]
        if not recent_gws:
            return pd.DataFrame(), []
        ph = ",".join("?" * len(recent_gws))
        df = pd.read_sql(f"""
            SELECT element_id,
                   SUM(minutes)                        AS recent_mins,
                   SUM(CAST(expected_goals   AS REAL)) AS recent_xg,
                   SUM(CAST(expected_assists AS REAL)) AS recent_xa
            FROM fpl_player_gw_stats
            WHERE gw IN ({ph})
            GROUP BY element_id
            HAVING SUM(minutes) >= 90
        """, conn, params=recent_gws)
    return df, recent_gws


@st.cache_data(ttl=300)
def load_latest_fixture_projections():
    """Load latest fixture projections for upcoming (non-finished) GWs only."""
    with sqlite3.connect(str(DB_PATH)) as conn:
        return pd.read_sql("""
            SELECT f.*
            FROM projections_fixtures f
            INNER JOIN (
                SELECT gw, home_team, away_team, MAX(timestamp) AS max_ts
                FROM projections_fixtures
                WHERE timestamp != '0000-backfill'
                GROUP BY gw, home_team, away_team
            ) l ON f.gw = l.gw AND f.home_team = l.home_team
               AND f.away_team = l.away_team AND f.timestamp = l.max_ts
            INNER JOIN fpl_gw_events e ON f.gw = e.gw
            WHERE (e.finished = 0 OR e.finished IS NULL)
            ORDER BY f.gw, f.home_team
        """, conn)


@st.cache_data(ttl=3600)
def load_set_pieces():
    data = json.loads((ROOT / "config" / "set_pieces.json").read_text())
    return data.get("penalty_takers", {}), data.get("corner_takers", {})


# ── Load everything ───────────────────────────────────────────────────────────

with st.spinner("Loading data…"):
    try:
        bootstrap    = fetch_bootstrap()
        projections  = load_latest_projections()
        recent_df, recent_gws = load_recent_form()
        fix_proj     = load_latest_fixture_projections()
        pen_takers, corner_takers = load_set_pieces()
    except Exception as e:
        st.error(f"Data load failed: {e}")
        st.stop()

# Build flat FPL player DataFrame from bootstrap
_team_map = {t["id"]: t.get("short_name", str(t["id"])) for t in bootstrap.get("teams", [])}
fpl_df = pd.DataFrame([{
    "fpl_id":          p["id"],
    "name":            p["web_name"],
    "pos":             POS_MAP.get(p["element_type"], "M"),
    "bv":              p["now_cost"] / 10.0,
    "team":            _team_map.get(p["team"], ""),
    "status":          p.get("status", "a"),
    "chance":          p.get("chance_of_playing_next_round"),
    "season_mins":     float(p.get("minutes") or 0),
    "season_goals":    float(p.get("goals_scored") or 0),
    "season_assists":  float(p.get("assists") or 0),
    "season_xg":       float(p.get("expected_goals")   or 0),
    "season_xa":       float(p.get("expected_assists")  or 0),
} for p in bootstrap.get("elements", [])])

fpl_df["avail_pct"] = fpl_df.apply(
    lambda r: float(r["chance"]) if r["chance"] is not None
              else (100.0 if r["status"] == "a" else 0.0),
    axis=1
)
fpl_df["pos_label"] = fpl_df["pos"].map(POS_LABEL)

# Next upcoming GW projections
next_gw = projections["gw"].min() if not projections.empty else None
proj_gw = projections[projections["gw"] == next_gw].copy() if next_gw else pd.DataFrame()
merged  = proj_gw.merge(fpl_df, on="fpl_id", how="left", suffixes=("", "_fpl"))


# ── Tabs ──────────────────────────────────────────────────────────────────────

tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "1 · Availability",
    "2 · xG vs Actuals",
    "3 · Set Pieces",
    "4 · Recent Form",
    "5 · Odds & Team Projections",
])


# ─────────────────────────────────────────────────────────────────────────────
# TAB 1 — Availability
# Expected: xMins drops roughly proportionally as chance_of_playing decreases.
# ─────────────────────────────────────────────────────────────────────────────
with tab1:
    st.subheader("Does xMins scale with chance_of_playing?")

    if merged.empty:
        st.info("No projection data.")
        st.stop()

    # Show poll timestamp so it's clear when the model ran vs now
    poll_ts = projections["timestamp"].max() if not projections.empty else "unknown"
    st.info(
        f"**Model last polled:** {poll_ts}  |  **FPL status:** live (fetched now).  "
        f"Players whose status changed *after* the poll will show a mismatch — that's expected. "
        f"Players who were 0% at poll time are absent from the DB (correctly filtered before insert)."
    )

    # Coverage metrics — confirm 0% players were excluded at poll time
    n_fpl_total   = len(fpl_df)                         # all non-withdrawn FPL players
    n_projected   = len(proj_gw)                        # players with xMins > 0 in DB
    n_excluded    = max(n_fpl_total - n_projected, 0)   # those with xMins=0 at poll time

    m1, m2, m3 = st.columns(3)
    m1.metric("FPL players (active)", n_fpl_total)
    m2.metric(f"Projected in GW{next_gw}", n_projected,
              help="Players with xMins > 0 at poll time — stored in DB")
    m3.metric("Excluded (0% at poll)", n_excluded,
              help="Had xMins=0 at poll time — not inserted into DB. Confirms 0% multiplier works.")

    gws_elapsed = max(int(next_gw) - 1, 1) if next_gw else 35

    # Build fixture counts for next_gw from projections data
    fix_counts_gw = {}
    if not fix_proj.empty and next_gw:
        for _, row in fix_proj[fix_proj["gw"] == next_gw].iterrows():
            for tc in ["home_team", "away_team"]:
                k = row[tc]
                fix_counts_gw[k] = fix_counts_gw.get(k, 0) + 1
    merged["fixture_count"] = merged["team"].map(fix_counts_gw).fillna(1).astype(int)

    def avail_band(pct):
        if pct == 0:    return "0% now*"
        if pct <= 25:   return "1–25%"
        if pct <= 50:   return "26–50%"
        if pct <= 75:   return "51–75%"
        if pct < 100:   return "76–99%"
        return "100% — fit"

    # Include 0% band — these players were projected at poll time but status
    # changed afterwards. Labelled separately so the count adds up to 553.
    merged["avail_band"] = merged["avail_pct"].apply(avail_band)
    active = merged.copy()
    band_order = ["0% now*", "1–25%", "26–50%", "51–75%", "76–99%", "100% — fit"]

    band_stats = (
        active.groupby("avail_band", sort=False)
              .agg(avg_xmins=("xmins", "mean"), n=("fpl_id", "count"))
              .reindex(band_order).dropna(how="all").reset_index()
    )

    col_a, col_b = st.columns([1, 1])
    with col_a:
        st.markdown("**Avg xMins by current availability band**")
        bar_colors = band_stats["avail_band"].map(
            lambda b: "#94A3B8" if b == "0% now*" else "#16A34A"
        )
        fig = go.Figure(go.Bar(
            x=band_stats["avail_band"],
            y=band_stats["avg_xmins"],
            text=band_stats.apply(lambda r: f"n={int(r['n'])}", axis=1),
            textposition="outside",
            marker_color=bar_colors,
        ))
        fig.update_layout(xaxis_title="", yaxis_title="Avg projected minutes",
                          height=370, margin=dict(t=30))
        st.plotly_chart(fig, use_container_width=True)
        st.caption("\\* 0% now — these players had xMins > 0 at poll time but their "
                   "FPL status changed to 0% afterwards. Their projected xMins is still "
                   "valid for when the model ran.")

    with col_b:
        st.markdown(f"**Doubts & part-fit players — GW{next_gw}**")
        st.caption("Players with 1–99% chance of playing. xMins should be reduced proportionally.")
        doubts = merged[(merged["avail_pct"] > 0) & (merged["avail_pct"] < 100)].copy()
        doubts["full_fit_xmins"] = (
            doubts["season_mins"] / gws_elapsed
        ).clip(upper=90).mul(doubts["fixture_count"]).round(1)
        doubts["avail_discount"] = (doubts["full_fit_xmins"] - doubts["xmins"]).round(1)

        disp = doubts.sort_values("avail_pct")[
            ["name", "team_fpl", "pos_label", "avail_pct", "full_fit_xmins", "xmins", "avail_discount"]
        ].rename(columns={
            "name": "Player", "team_fpl": "Team", "pos_label": "Pos",
            "avail_pct": "Avail %", "full_fit_xmins": "If 100% fit",
            "xmins": "xMins", "avail_discount": "Mins lost to doubt",
        })
        if disp.empty:
            st.success("No doubts for this GW.")
        else:
            st.dataframe(disp, hide_index=True, use_container_width=True,
                         height=38 + 35 * len(disp))

    # Scatter: expected xMins (avail × season_avg) vs model xMins — exclude 0% avail
    st.markdown("**xMins vs avail-adjusted season average** *(rate-based players, avail > 0%)*")
    st.caption("Points near the diagonal = model tracks availability correctly. "
               "Below diagonal = model is more conservative (normal for position/rotation). "
               "Outliers well above diagonal may need investigation.")
    fit_only = merged[(merged["season_mins"] >= MIN_SEASON_MINS) & (merged["avail_pct"] > 0)].copy()
    fit_only["season_avg_capped"] = (fit_only["season_mins"] / gws_elapsed).clip(upper=90)
    fit_only["expected_xmins"]    = (
        fit_only["avail_pct"] / 100.0 * fit_only["season_avg_capped"] * fit_only["fixture_count"]
    ).round(1)

    fig2 = px.scatter(
        fit_only, x="expected_xmins", y="xmins",
        color="pos_label", hover_data=["name", "team_fpl", "avail_pct"],
        labels={"expected_xmins": "Expected xMins (avail × season avg)", "xmins": "Model xMins"},
        color_discrete_map={"GK": "#7C3AED", "DEF": "#2563EB", "MID": "#16A34A", "FWD": "#DC2626"},
    )
    max_v = max(fit_only["expected_xmins"].max(), fit_only["xmins"].max()) + 5
    fig2.add_trace(go.Scatter(x=[0, max_v], y=[0, max_v], mode="lines",
                              line=dict(dash="dash", color="#999"), name="Perfect match",
                              showlegend=True))
    fig2.update_layout(height=420, margin=dict(t=20))
    st.plotly_chart(fig2, use_container_width=True)


# ─────────────────────────────────────────────────────────────────────────────
# TAB 2 — xG vs Actuals
# Expected: scatter sits on or near the diagonal for average converters.
# Over-converters (lucky goals) sit above diagonal; xG correctly discounts them.
# ─────────────────────────────────────────────────────────────────────────────
with tab2:
    st.subheader("xG vs actual goals rate — who benefits from using xG?")
    st.caption(
        "Points above the diagonal = over-converters (scored more than xG suggests). "
        "The model uses xG, so it rates these players more conservatively. "
        "Points below = under-converters who the model rates higher than actuals would."
    )

    rate_df = fpl_df[fpl_df["season_mins"] >= MIN_SEASON_MINS].copy()
    rate_df["xg_p90"]      = (rate_df["season_xg"]      / rate_df["season_mins"]) * 90
    rate_df["goals_p90"]   = (rate_df["season_goals"]    / rate_df["season_mins"]) * 90
    rate_df["xa_p90"]      = (rate_df["season_xa"]       / rate_df["season_mins"]) * 90
    rate_df["assists_p90"] = (rate_df["season_assists"]  / rate_df["season_mins"]) * 90

    rate_df["pos_avg_g"]  = rate_df["pos"].map(POS_AVG_G_PER90).fillna(0.12)
    rate_df["g_rate_xg"]  = XG_SHRINK   * rate_df["xg_p90"]  + (1-XG_SHRINK)   * rate_df["pos_avg_g"]
    rate_df["g_rate_act"] = RATE_SHRINK * rate_df["goals_p90"] + (1-RATE_SHRINK) * rate_df["pos_avg_g"]
    rate_df["g_delta"]    = (rate_df["g_rate_xg"] - rate_df["g_rate_act"]).round(4)
    rate_df["conversion"] = (rate_df["goals_p90"] - rate_df["xg_p90"]).round(3)

    col_a, col_b = st.columns([3, 2])
    with col_a:
        fig = px.scatter(
            rate_df, x="xg_p90", y="goals_p90",
            color="pos_label", hover_data=["name", "team", "season_mins"],
            labels={"xg_p90": "xG per 90", "goals_p90": "Actual goals per 90"},
            color_discrete_map={"GK": "#7C3AED", "DEF": "#2563EB", "MID": "#16A34A", "FWD": "#DC2626"},
        )
        mx = max(rate_df["xg_p90"].max(), rate_df["goals_p90"].max()) * 1.05
        fig.add_trace(go.Scatter(x=[0, mx], y=[0, mx], mode="lines",
                                 line=dict(dash="dash", color="#999"),
                                 name="xG = goals", showlegend=True))
        fig.update_layout(height=420, margin=dict(t=20))
        st.plotly_chart(fig, use_container_width=True)

    with col_b:
        st.markdown("**Biggest xG → model impact** (g_rate with xG minus g_rate with actuals)")
        st.caption("Positive = model rates player higher using xG. Negative = model is more cautious.")
        impact = rate_df.nlargest(25, "g_delta")[
            ["name", "team", "pos_label", "xg_p90", "goals_p90", "g_rate_xg", "g_rate_act", "g_delta"]
        ].rename(columns={
            "name": "Player", "team": "Team", "pos_label": "Pos",
            "xg_p90": "xG/90", "goals_p90": "Goals/90",
            "g_rate_xg": "Rate (xG)", "g_rate_act": "Rate (act)", "g_delta": "Δ rate",
        }).round(4)
        st.dataframe(impact, hide_index=True, use_container_width=True, height=420)

    st.markdown("**Biggest over-converters** (actual goals much higher than xG)")
    st.caption("These players look better on actuals — xG correctly reins them in.")
    over = rate_df[rate_df["pos"] != "G"].nlargest(15, "conversion")[
        ["name", "team", "pos_label", "season_mins", "season_goals", "season_xg", "conversion"]
    ].rename(columns={
        "name": "Player", "team": "Team", "pos_label": "Pos",
        "season_mins": "Mins", "season_goals": "Goals", "season_xg": "xG",
        "conversion": "Goals − xG/90",
    })
    st.dataframe(over, hide_index=True, use_container_width=True)


# ─────────────────────────────────────────────────────────────────────────────
# TAB 3 — Set Pieces
# Expected: penalty takers have noticeably higher goal_prob than same-pos peers.
# Corner takers have higher assist_prob. Set piece takers should stand out.
# ─────────────────────────────────────────────────────────────────────────────
with tab3:
    st.subheader("Set piece roles — goal/assist_prob vs position peers")
    st.caption(
        "Penalty takers' goal_prob should be above the position average. "
        "Corner takers' assist_prob should be above the position average. "
        "Note: rate-based players (≥270 mins) have set pieces embedded in their xG/xA — "
        "the explicit bonus only applies to BV-fallback players with fewer season minutes."
    )

    if proj_gw.empty:
        st.info("No projection data.")
    else:
        sp_df = proj_gw.merge(
            fpl_df[["fpl_id", "pos", "pos_label", "bv", "season_mins"]],
            on="fpl_id", how="left", suffixes=("", "_fpl")
        )
        # Use team/name from projection (model short names match set_pieces.json keys)
        sp_df["is_pen"]    = sp_df.apply(lambda r: pen_takers.get(r["team"], "") == r["name"], axis=1)
        sp_df["is_corner"] = sp_df.apply(lambda r: corner_takers.get(r["team"], "") == r["name"], axis=1)
        sp_df["use_rates"] = sp_df["season_mins"] >= MIN_SEASON_MINS

        col_a, col_b = st.columns(2)

        with col_a:
            st.markdown("**Penalty takers vs position avg goal_prob**")
            pen_rows = sp_df[sp_df["is_pen"]].copy()
            pos_avg_goal = sp_df.groupby("pos")["goal_prob"].mean().rename("pos_avg_goal")
            pen_rows = pen_rows.merge(pos_avg_goal, left_on="pos", right_index=True, how="left")
            pen_rows["Δ vs pos"] = (pen_rows["goal_prob"] - pen_rows["pos_avg_goal"]).round(4)

            disp_pen = pen_rows[["name", "team", "pos_label", "bv", "xmins",
                                  "goal_prob", "pos_avg_goal", "Δ vs pos", "use_rates"]].rename(columns={
                "name": "Player", "team": "Team", "pos_label": "Pos",
                "bv": "BV", "xmins": "xMins", "goal_prob": "Goal prob",
                "pos_avg_goal": "Pos avg", "use_rates": "Rate path",
            }).sort_values("Goal prob", ascending=False)
            st.dataframe(disp_pen, hide_index=True, use_container_width=True)

        with col_b:
            st.markdown("**Corner takers vs position avg assist_prob**")
            cor_rows = sp_df[sp_df["is_corner"]].copy()
            pos_avg_ast = sp_df.groupby("pos")["assist_prob"].mean().rename("pos_avg_ast")
            cor_rows = cor_rows.merge(pos_avg_ast, left_on="pos", right_index=True, how="left")
            cor_rows["Δ vs pos"] = (cor_rows["assist_prob"] - cor_rows["pos_avg_ast"]).round(4)

            disp_cor = cor_rows[["name", "team", "pos_label", "bv", "xmins",
                                  "assist_prob", "pos_avg_ast", "Δ vs pos", "use_rates"]].rename(columns={
                "name": "Player", "team": "Team", "pos_label": "Pos",
                "bv": "BV", "xmins": "xMins", "assist_prob": "Assist prob",
                "pos_avg_ast": "Pos avg", "use_rates": "Rate path",
            }).sort_values("Assist prob", ascending=False)
            st.dataframe(disp_cor, hide_index=True, use_container_width=True)

        st.markdown("---")
        st.markdown("**Goal prob distribution — takers vs rest (by position)**")
        for pos_code, pos_name in [("F", "FWD"), ("M", "MID"), ("D", "DEF")]:
            pos_data = sp_df[sp_df["pos"] == pos_code]
            if pos_data.empty:
                continue
            takers = pos_data[pos_data["is_pen"]]["goal_prob"]
            others = pos_data[~pos_data["is_pen"]]["goal_prob"]
            if takers.empty:
                continue
            st.markdown(
                f"**{pos_name}** — penalty takers avg: `{takers.mean():.4f}` "
                f"vs rest: `{others.mean():.4f}` "
                f"(+{(takers.mean()-others.mean()):.4f})"
            )


# ─────────────────────────────────────────────────────────────────────────────
# TAB 4 — Recent Form
# Expected: players in hot/cold streaks show clear recent vs season divergence.
# The 35% form blend should shift projections in the right direction.
# ─────────────────────────────────────────────────────────────────────────────
with tab4:
    st.subheader("Recent form weighting — last-5-GW xG vs full-season rate")
    st.caption(
        f"Model blends season xG/90 ({int((1-FORM_WEIGHT)*100)}%) with last-5-GW xG/90 ({int(FORM_WEIGHT*100)}%). "
        f"In-form players get a boost; out-of-form players are penalised. "
        f"Recent GWs used: {recent_gws if recent_gws else 'none'}."
    )

    if recent_df.empty:
        st.info("No recent form data in DB.")
    else:
        recent_df["recent_xg_p90"] = (recent_df["recent_xg"] / recent_df["recent_mins"].clip(lower=1)) * 90
        recent_df["recent_xa_p90"] = (recent_df["recent_xa"] / recent_df["recent_mins"].clip(lower=1)) * 90

        form_merged = recent_df.merge(
            fpl_df[["fpl_id", "name", "team", "pos", "pos_label", "season_mins", "season_xg", "season_xa"]],
            left_on="element_id", right_on="fpl_id", how="inner"
        )
        form_merged = form_merged[form_merged["season_mins"] >= MIN_SEASON_MINS].copy()
        form_merged["season_xg_p90"] = (form_merged["season_xg"] / form_merged["season_mins"]) * 90
        form_merged["blended_xg_p90"] = (
            (1 - FORM_WEIGHT) * form_merged["season_xg_p90"]
            + FORM_WEIGHT     * form_merged["recent_xg_p90"]
        )
        form_merged["form_delta"] = (form_merged["recent_xg_p90"] - form_merged["season_xg_p90"]).round(4)
        form_merged["blend_delta"] = (form_merged["blended_xg_p90"] - form_merged["season_xg_p90"]).round(4)

        # Scatter: season vs recent
        fig = px.scatter(
            form_merged, x="season_xg_p90", y="recent_xg_p90",
            color="pos_label", hover_data=["name", "team", "recent_mins"],
            labels={"season_xg_p90": "Season xG/90", "recent_xg_p90": "Recent 5-GW xG/90"},
            color_discrete_map={"GK": "#7C3AED", "DEF": "#2563EB", "MID": "#16A34A", "FWD": "#DC2626"},
        )
        mx = max(form_merged["season_xg_p90"].max(), form_merged["recent_xg_p90"].max()) * 1.05
        fig.add_trace(go.Scatter(x=[0, mx], y=[0, mx], mode="lines",
                                 line=dict(dash="dash", color="#999"),
                                 name="No change", showlegend=True))
        fig.update_layout(height=380, margin=dict(t=20))
        st.plotly_chart(fig, use_container_width=True)
        st.caption("Above diagonal = in form (recent > season). Below = out of form.")

        col_a, col_b = st.columns(2)
        show_cols = ["name", "team", "pos_label", "season_xg_p90", "recent_xg_p90",
                     "blended_xg_p90", "form_delta", "blend_delta"]
        rename = {
            "name": "Player", "team": "Team", "pos_label": "Pos",
            "season_xg_p90": "Season xG/90", "recent_xg_p90": "Recent xG/90",
            "blended_xg_p90": "Blended xG/90", "form_delta": "Recent − Season",
            "blend_delta": "Blend boost",
        }

        with col_a:
            st.markdown("**In form — top 20 (recent > season)**")
            in_form = form_merged.nlargest(20, "form_delta")[show_cols].rename(columns=rename).round(4)
            st.dataframe(in_form, hide_index=True, use_container_width=True)

        with col_b:
            st.markdown("**Out of form — top 20 (recent < season)**")
            out_form = form_merged.nsmallest(20, "form_delta")[show_cols].rename(columns=rename).round(4)
            st.dataframe(out_form, hide_index=True, use_container_width=True)


# ─────────────────────────────────────────────────────────────────────────────
# TAB 5 — Odds & Team Projections
# Expected: projected total goals per fixture tracks the bookmaker's over/under line.
# Blend method should dominate; Ratings-only means no odds were found.
# ─────────────────────────────────────────────────────────────────────────────
with tab5:
    st.subheader("Team projections — odds blend quality")
    st.caption(
        "Our projected total (home_g + away_g) should sit near the bookmaker's over/under line. "
        "'Blend' = odds used; 'Ratings' = DC model only (no odds found for that fixture)."
    )

    if fix_proj.empty:
        st.info("No fixture projection data.")
    else:
        # Focus on upcoming GWs only
        upcoming_fix = fix_proj[fix_proj["gw"] >= (next_gw or 0)].copy()
        upcoming_fix["total_g"] = (upcoming_fix["home_g"] + upcoming_fix["away_g"]).round(3)
        upcoming_fix["home_cs_pct"] = (upcoming_fix["home_cs"] * 100).round(1)
        upcoming_fix["away_cs_pct"] = (upcoming_fix["away_cs"] * 100).round(1)

        gw_options = sorted(upcoming_fix["gw"].unique().astype(int))
        sel_gw = st.selectbox("Gameweek", gw_options, index=0)
        gw_data = upcoming_fix[upcoming_fix["gw"] == sel_gw].copy()

        st.markdown(f"**GW{sel_gw} fixture projections**")

        disp = gw_data[[
            "home_team", "away_team", "home_g", "away_g", "total_g",
            "home_cs_pct", "away_cs_pct", "method"
        ]].rename(columns={
            "home_team": "Home", "away_team": "Away",
            "home_g": "Home xG", "away_g": "Away xG", "total_g": "Total xG",
            "home_cs_pct": "Home CS %", "away_cs_pct": "Away CS %", "method": "Method",
        }).sort_values("Total xG", ascending=False).reset_index(drop=True)

        def style_method(val):
            return "color: #16A34A; font-weight:600" if val == "Blend" else "color: #DC2626;"

        n_blend   = int((disp["Method"] == "Blend").sum())
        n_ratings = int((disp["Method"] == "Ratings").sum())

        m1, m2, m3 = st.columns(3)
        m1.metric("Fixtures with odds", n_blend)
        m2.metric("Ratings only", n_ratings)
        m3.metric("Avg projected total", f"{disp['Total xG'].mean():.2f}")

        try:
            styled = disp.style.applymap(style_method, subset=["Method"])
            st.dataframe(styled, hide_index=True, use_container_width=True)
        except Exception:
            st.dataframe(disp, hide_index=True, use_container_width=True)

        # Bar chart: projected goals per fixture
        fig = go.Figure()
        fig.add_bar(x=disp["Home"] + " v " + disp["Away"], y=disp["Home xG"],
                    name="Home xG", marker_color="#16A34A")
        fig.add_bar(x=disp["Home"] + " v " + disp["Away"], y=disp["Away xG"],
                    name="Away xG", marker_color="#2563EB")
        fig.update_layout(barmode="stack", height=350,
                          yaxis_title="Expected goals", xaxis_tickangle=-30,
                          margin=dict(t=20))
        st.plotly_chart(fig, use_container_width=True)

        st.markdown("---")
        st.subheader("Home vs Away goal projection calibration (all upcoming GWs)")
        fig2 = px.scatter(
            upcoming_fix, x="home_g", y="away_g",
            color="method", text="home_team",
            labels={"home_g": "Home xG", "away_g": "Away xG"},
            color_discrete_map={"Blend": "#16A34A", "Ratings": "#DC2626"},
        )
        fig2.update_traces(textposition="top center", textfont_size=9)
        fig2.update_layout(height=400, margin=dict(t=20))
        st.plotly_chart(fig2, use_container_width=True)
