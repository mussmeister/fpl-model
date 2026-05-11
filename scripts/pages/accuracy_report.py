"""
Model Accuracy Report — Our Model vs Solio vs Actuals.
Component-level breakdown: xPts, xMins, Goals, Assists, CS, Bonus.
Tab 5: Team Model — DC model vs Solio team predictions vs actual match results.
"""
import sys
import sqlite3
import numpy as np
import pandas as pd
import plotly.graph_objects as go
from pathlib import Path
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from utils.auth import require_auth, show_logout_button
from utils.team_mappings import to_short, FDCO_TO_SHORT

DB_PATH      = Path(__file__).resolve().parents[2] / "outputs" / "projections_history.db"
FIXTURE_PATH = Path(__file__).resolve().parents[2] / "fixtures" / "fixtures_all.csv"

MODEL_COL = "#16A34A"
SOLIO_COL = "#2563EB"
DC_COL    = "#7C3AED"
POS_ORDER = ["GK", "DEF", "MID", "FWD"]

st.set_page_config(page_title="FPL – Accuracy Report", layout="wide", page_icon="📐")
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Barlow+Condensed:wght@400;600;700;800&family=Barlow:wght@400;500;600&display=swap');
html, body, [class*="css"], .stApp { font-family: 'Barlow', sans-serif !important; }
h1, h2, h3 { font-family: 'Barlow Condensed', sans-serif !important; font-weight: 800 !important; }
.block-container { padding-left: 1rem !important; padding-right: 1rem !important; }
.metric-card { background:#f8fafc; border:1px solid #e2e8f0; border-radius:10px; padding:14px 18px; text-align:center; }
.mc-label { font-size:11px; color:#888; text-transform:uppercase; letter-spacing:0.8px; font-weight:600; }
.mc-value { font-family:'Barlow Condensed',sans-serif; font-size:28px; font-weight:800; }
.mc-sub   { font-size:11px; color:#666; margin-top:2px; }
</style>
""", unsafe_allow_html=True)

require_auth()
show_logout_button()

if st.button("← Back to Fixtures"):
    st.switch_page("fpl_app.py")

st.title("📐 Model Accuracy Report")
st.caption("Our model vs Solio vs actuals — component level breakdown.")

# ── Player data loaders ────────────────────────────────────────────────────────

@st.cache_data(ttl=300)
def get_available_gws() -> list[int]:
    try:
        with sqlite3.connect(str(DB_PATH)) as conn:
            rows = conn.execute("""
                SELECT DISTINCT m.gw
                FROM player_projection_model m
                JOIN fpl_gw_events e ON m.gw = e.gw
                WHERE e.finished = 1
                ORDER BY m.gw
            """).fetchall()
            return [r[0] for r in rows]
    except Exception:
        return []


@st.cache_data(ttl=300)
def load_all_gws_data() -> pd.DataFrame:
    try:
        with sqlite3.connect(str(DB_PATH)) as conn:
            return pd.read_sql("""
                WITH model_snap AS (
                    SELECT gw, name,
                           xmins     AS mdl_xmins,
                           xpts      AS mdl_xpts,
                           goal_prob, assist_prob, cs_prob
                    FROM player_projection_model m1
                    WHERE timestamp = (
                        SELECT MAX(timestamp)
                        FROM player_projection_model m2
                        WHERE m2.gw = m1.gw
                    )
                ),
                bench_latest AS (
                    SELECT name, gw, MAX(ingested_at) AS max_ia
                    FROM player_projection_snapshots
                    WHERE source = 'solio'
                    GROUP BY name, gw
                ),
                bench AS (
                    SELECT p.name, p.gw,
                           p.xmins   AS sol_xmins,
                           p.pts     AS sol_xpts,
                           p.goals   AS sol_goals,
                           p.assists AS sol_assists,
                           p.cs      AS sol_cs,
                           p.bonus   AS sol_bonus
                    FROM player_projection_snapshots p
                    JOIN bench_latest l ON p.name = l.name
                                       AND p.gw = l.gw
                                       AND p.ingested_at = l.max_ia
                    WHERE p.source = 'solio'
                )
                SELECT
                    a.gw, a.name, a.pos, a.team,
                    a.minutes, a.total_points, a.goals_scored, a.assists,
                    a.clean_sheets, a.bonus, a.bps,
                    a.expected_goals, a.expected_assists,
                    m.mdl_xmins, m.mdl_xpts, m.goal_prob, m.assist_prob, m.cs_prob,
                    b.sol_xmins, b.sol_xpts, b.sol_goals, b.sol_assists, b.sol_cs, b.sol_bonus
                FROM (
                    SELECT s.gw,
                           p.web_name AS name,
                           CASE p.element_type
                               WHEN 1 THEN 'GK' WHEN 2 THEN 'DEF'
                               WHEN 3 THEN 'MID' WHEN 4 THEN 'FWD'
                           END AS pos,
                           t.short_name AS team,
                           s.minutes, s.total_points, s.goals_scored, s.assists,
                           s.clean_sheets, s.bonus, s.bps,
                           s.expected_goals, s.expected_assists
                    FROM fpl_player_gw_stats s
                    JOIN fpl_players p ON s.element_id = p.element_id
                    LEFT JOIN fpl_teams t ON p.team_id = t.team_id
                    JOIN fpl_gw_events e ON s.gw = e.gw AND e.finished = 1
                    WHERE s.minutes > 0
                ) a
                LEFT JOIN model_snap m ON m.gw = a.gw AND LOWER(m.name) = LOWER(a.name)
                LEFT JOIN bench      b ON b.gw = a.gw AND LOWER(b.name) = LOWER(a.name)
                ORDER BY a.gw, a.total_points DESC
            """, conn)
    except Exception as e:
        st.error(f"Failed to load data: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_gw_data(gw: int) -> pd.DataFrame:
    try:
        with sqlite3.connect(str(DB_PATH)) as conn:
            return pd.read_sql("""
                WITH model_snap AS (
                    SELECT name,
                           xmins     AS mdl_xmins,
                           xpts      AS mdl_xpts,
                           goal_prob, assist_prob, cs_prob
                    FROM player_projection_model
                    WHERE gw = ?
                      AND timestamp = (
                          SELECT MAX(timestamp)
                          FROM player_projection_model WHERE gw = ?
                      )
                ),
                bench_latest AS (
                    SELECT name, MAX(ingested_at) AS max_ia
                    FROM player_projection_snapshots
                    WHERE gw = ? AND source = 'solio'
                    GROUP BY name
                ),
                bench AS (
                    SELECT p.name,
                           p.xmins   AS sol_xmins,
                           p.pts     AS sol_xpts,
                           p.goals   AS sol_goals,
                           p.assists AS sol_assists,
                           p.cs      AS sol_cs,
                           p.bonus   AS sol_bonus
                    FROM player_projection_snapshots p
                    JOIN bench_latest l ON p.name = l.name
                                       AND p.ingested_at = l.max_ia
                    WHERE p.gw = ? AND p.source = 'solio'
                )
                SELECT
                    a.name, a.pos, a.team,
                    a.minutes, a.total_points, a.goals_scored, a.assists,
                    a.clean_sheets, a.bonus, a.bps,
                    a.expected_goals, a.expected_assists,
                    m.mdl_xmins, m.mdl_xpts, m.goal_prob, m.assist_prob, m.cs_prob,
                    b.sol_xmins, b.sol_xpts, b.sol_goals, b.sol_assists, b.sol_cs, b.sol_bonus
                FROM (
                    SELECT p.web_name AS name,
                           CASE p.element_type
                               WHEN 1 THEN 'GK' WHEN 2 THEN 'DEF'
                               WHEN 3 THEN 'MID' WHEN 4 THEN 'FWD'
                           END AS pos,
                           t.short_name AS team,
                           s.minutes, s.total_points, s.goals_scored, s.assists,
                           s.clean_sheets, s.bonus, s.bps,
                           s.expected_goals, s.expected_assists
                    FROM fpl_player_gw_stats s
                    JOIN fpl_players p ON s.element_id = p.element_id
                    LEFT JOIN fpl_teams t ON p.team_id = t.team_id
                    WHERE s.gw = ? AND s.minutes > 0
                ) a
                LEFT JOIN model_snap m ON LOWER(m.name) = LOWER(a.name)
                LEFT JOIN bench      b ON LOWER(b.name) = LOWER(a.name)
                ORDER BY a.total_points DESC
            """, conn, params=(gw, gw, gw, gw, gw))
    except Exception as e:
        st.error(f"Failed to load data: {e}")
        return pd.DataFrame()


# ── Team model data loaders ────────────────────────────────────────────────────

@st.cache_data(ttl=3600)
def load_team_actuals() -> pd.DataFrame:
    try:
        df_r = pd.read_csv("https://www.football-data.co.uk/mmz4281/2526/E0.csv")
        df_r["FTHG"] = pd.to_numeric(df_r["FTHG"], errors="coerce")
        df_r["FTAG"] = pd.to_numeric(df_r["FTAG"], errors="coerce")
        df_r = df_r.dropna(subset=["FTHG", "FTAG"]).copy()
        df_r["home_short"] = df_r["HomeTeam"].apply(lambda x: to_short(x, FDCO_TO_SHORT))
        df_r["away_short"] = df_r["AwayTeam"].apply(lambda x: to_short(x, FDCO_TO_SHORT))
    except Exception:
        return pd.DataFrame()
    try:
        df_f = pd.read_csv(str(FIXTURE_PATH))
        df_f["Home"] = df_f["Home"].str.strip()
        df_f["Away"] = df_f["Away"].str.strip()
        df_f["GW"]   = pd.to_numeric(df_f["GW"], errors="coerce")
    except Exception:
        return pd.DataFrame()

    df_m = df_r.merge(df_f[["Home","Away","GW"]],
                      left_on=["home_short","away_short"],
                      right_on=["Home","Away"], how="inner")
    rows = []
    for _, r in df_m.iterrows():
        gw = int(r["GW"])
        rows.extend([
            {"team": r["home_short"], "gw": gw,
             "actual_g": float(r["FTHG"]), "actual_gc": float(r["FTAG"]),
             "actual_cs": 1.0 if r["FTAG"] == 0 else 0.0},
            {"team": r["away_short"], "gw": gw,
             "actual_g": float(r["FTAG"]), "actual_gc": float(r["FTHG"]),
             "actual_cs": 1.0 if r["FTHG"] == 0 else 0.0},
        ])
    if not rows:
        return pd.DataFrame()
    df_per = pd.DataFrame(rows)
    return (df_per.groupby(["team","gw"], as_index=False)
            .agg(actual_g=("actual_g","sum"), actual_gc=("actual_gc","sum"),
                 actual_cs=("actual_cs","sum"), n_fix=("actual_g","count")))


@st.cache_data(ttl=600)
def load_team_predictions() -> tuple[pd.DataFrame, pd.DataFrame]:
    try:
        with sqlite3.connect(str(DB_PATH)) as conn:
            solio = pd.read_sql("""
                WITH latest AS (
                    SELECT team, gw, MAX(ingested_at) AS max_ia
                    FROM solio_fixture_snapshots GROUP BY team, gw
                )
                SELECT s.team, s.gw, s.g AS solio_g, s.gc AS solio_gc, s.cs AS solio_cs
                FROM solio_fixture_snapshots s
                JOIN latest l ON s.team=l.team AND s.gw=l.gw AND s.ingested_at=l.max_ia
                WHERE s.g IS NOT NULL ORDER BY s.gw, s.team
            """, conn)
            dc = pd.read_sql("""
                WITH latest AS (SELECT gw, MAX(timestamp) AS max_ts FROM projections GROUP BY gw)
                SELECT p.team, p.gw, p.g AS dc_g, p.gc AS dc_gc, p.cs AS dc_cs
                FROM projections p
                JOIN latest l ON p.gw=l.gw AND p.timestamp=l.max_ts
                ORDER BY p.gw, p.team
            """, conn)
    except Exception:
        return pd.DataFrame(), pd.DataFrame()
    return solio, dc


# ── Accuracy helpers ──────────────────────────────────────────────────────────

def mae(pred, actual):
    mask = pred.notna() & actual.notna()
    if mask.sum() == 0:
        return None, 0
    return round((pred[mask] - actual[mask]).abs().mean(), 3), int(mask.sum())


def bias(pred, actual):
    mask = pred.notna() & actual.notna()
    if mask.sum() == 0:
        return None
    return round((pred[mask] - actual[mask]).mean(), 3)


def fmt_delta(v):
    if v is None or pd.isna(v):
        return "—"
    return f"+{v:.3f}" if v > 0 else f"{v:.3f}"


def metric_card(label, value, sub=""):
    sub_html = f'<div class="mc-sub">{sub}</div>' if sub else ""
    return (f'<div class="metric-card">'
            f'<div class="mc-label">{label}</div>'
            f'<div class="mc-value">{value}</div>'
            f'{sub_html}</div>')


# ── GW selector ───────────────────────────────────────────────────────────────

avail_gws = get_available_gws()
has_model = bool(avail_gws)

_NUM_COLS = [
    "minutes","total_points","goals_scored","assists","clean_sheets",
    "bonus","bps","expected_goals","expected_assists",
    "mdl_xmins","mdl_xpts","goal_prob","assist_prob","cs_prob",
    "sol_xmins","sol_xpts","sol_goals","sol_assists","sol_cs","sol_bonus",
]

if has_model:
    gw_options = [0] + avail_gws
    selected_gw = st.selectbox(
        "Gameweek", gw_options, index=len(gw_options) - 1,
        format_func=lambda g: "All GWs" if g == 0 else f"GW{g}",
    )
    all_gws_mode = selected_gw == 0
    df = load_all_gws_data() if all_gws_mode else load_gw_data(selected_gw)
    if not df.empty:
        for c in _NUM_COLS:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce")
    n_played = len(df)
    n_model  = int(df["mdl_xpts"].notna().sum()) if not df.empty else 0
    n_solio  = int(df["sol_xpts"].notna().sum()) if not df.empty else 0
else:
    df = pd.DataFrame()
    all_gws_mode = False
    n_played = n_model = n_solio = 0

# ── Tabs ──────────────────────────────────────────────────────────────────────

tab_overview, tab_players, tab_positions, tab_misses, tab_team = st.tabs([
    "📊 Overview", "👤 By Player", "🗂️ By Position", "🎯 Biggest Misses", "⚽ Team Model"
])

_NO_MODEL = (
    "No completed gameweeks with model projections yet. "
    "Once GW36 finishes you'll have your first comparison."
)

# ── Tab 1: Overview ───────────────────────────────────────────────────────────

with tab_overview:
    if not has_model or df.empty:
        st.info(_NO_MODEL)
    else:
        gw_label = "All GWs" if all_gws_mode else f"GW{selected_gw}"
        played_label = "player appearances" if all_gws_mode else "players played"
        st.markdown(
            f"**{gw_label}** — {n_played} {played_label} &nbsp;|&nbsp; "
            f"Model coverage: **{n_model}/{n_played}** &nbsp;|&nbsp; "
            f"Solio coverage: **{n_solio}/{n_played}**"
        )
        st.markdown("---")

        # ── Primary comparison: Our Model vs Solio ────────────────────────────
        st.markdown("#### Our Model vs Solio — head to head")

        comp_rows = []
        for metric, mdl_col, sol_col, actual_col, fmt in [
            ("xPts",  "mdl_xpts",  "sol_xpts",  "total_points", ".3f"),
            ("xMins", "mdl_xmins", "sol_xmins",  "minutes",      ".1f"),
        ]:
            mdl_mae,  n = mae(df[mdl_col],  df[actual_col])
            sol_mae,  _ = mae(df[sol_col],  df[actual_col])
            mdl_bias    = bias(df[mdl_col], df[actual_col])
            sol_bias    = bias(df[sol_col], df[actual_col])
            comp_rows.append({
                "Metric":      metric,
                "Model MAE":   f"{mdl_mae:{fmt}}" if mdl_mae is not None else "—",
                "Model Bias":  fmt_delta(mdl_bias),
                "Solio MAE":   f"{sol_mae:{fmt}}" if sol_mae is not None else "—",
                "Solio Bias":  fmt_delta(sol_bias),
                "n":           n,
                "_mdl_raw":    mdl_mae,
                "_sol_raw":    sol_mae,
            })

        df_comp = pd.DataFrame(comp_rows)

        def _highlight_winner(row):
            style = [""] * len(row)
            try:
                m, s = float(row["_mdl_raw"]), float(row["_sol_raw"])
                idx_m = list(row.index).index("Model MAE")
                idx_s = list(row.index).index("Solio MAE")
                if m < s:
                    style[idx_m] = "background-color:#dcfce7;font-weight:600"
                else:
                    style[idx_s] = "background-color:#dcfce7;font-weight:600"
            except Exception:
                pass
            return style

        st.dataframe(
            df_comp.drop(columns=["_mdl_raw","_sol_raw"])
                   .style.apply(_highlight_winner, axis=1),
            hide_index=True, use_container_width=True,
        )
        st.caption(
            "MAE = Mean Absolute Error (lower = better). "
            "Bias = avg(predicted − actual): positive = overestimates, negative = underestimates. "
            "Green = better of the two."
        )

        # ── Per-GW trend (all-GWs mode only) ──────────────────────────────────
        if all_gws_mode and "gw" in df.columns:
            st.markdown("---")
            st.markdown("#### xPts MAE per Gameweek")
            st.caption("Trend over the season — see if accuracy is improving or degrading week by week.")

            gw_acc = []
            for gw_n, sub in df.groupby("gw"):
                mdl_m, n_g = mae(sub["mdl_xpts"], sub["total_points"])
                sol_m, _   = mae(sub["sol_xpts"],  sub["total_points"])
                gw_acc.append({"GW": gw_n, "Model MAE": mdl_m, "Solio MAE": sol_m, "n": n_g})
            df_trend = pd.DataFrame(gw_acc)

            fig_trend = go.Figure()
            fig_trend.add_trace(go.Scatter(
                x=df_trend["GW"], y=df_trend["Model MAE"],
                mode="lines+markers", name="Our Model",
                line=dict(color=MODEL_COL, width=2), marker=dict(size=6),
                hovertemplate="GW%{x}<br>Model MAE: %{y:.3f} (n=%{customdata})<extra></extra>",
                customdata=df_trend["n"],
            ))
            fig_trend.add_trace(go.Scatter(
                x=df_trend["GW"], y=df_trend["Solio MAE"],
                mode="lines+markers", name="Solio",
                line=dict(color=SOLIO_COL, width=2), marker=dict(size=6),
                hovertemplate="GW%{x}<br>Solio MAE: %{y:.3f}<extra></extra>",
            ))
            mdl_overall, _ = mae(df["mdl_xpts"], df["total_points"])
            sol_overall, _ = mae(df["sol_xpts"],  df["total_points"])
            if mdl_overall:
                fig_trend.add_hline(y=mdl_overall, line_dash="dot",
                                    line_color=MODEL_COL, opacity=0.35,
                                    annotation_text=f"Model avg {mdl_overall:.3f}",
                                    annotation_position="top left")
            if sol_overall:
                fig_trend.add_hline(y=sol_overall, line_dash="dot",
                                    line_color=SOLIO_COL, opacity=0.35,
                                    annotation_text=f"Solio avg {sol_overall:.3f}",
                                    annotation_position="bottom right")
            fig_trend.update_layout(
                xaxis_title="Gameweek", yaxis_title="xPts MAE",
                height=360, legend=dict(orientation="h", yanchor="bottom", y=1.02),
                margin=dict(l=40, r=20, t=20, b=40), plot_bgcolor="#fafafa",
            )
            st.plotly_chart(fig_trend, use_container_width=True)

            st.markdown("#### xMins MAE per Gameweek")
            mins_acc = []
            for gw_n, sub in df.groupby("gw"):
                mdl_m, _ = mae(sub["mdl_xmins"], sub["minutes"])
                sol_m, _ = mae(sub["sol_xmins"], sub["minutes"])
                mins_acc.append({"GW": gw_n, "Model MAE": mdl_m, "Solio MAE": sol_m})
            df_mins_trend = pd.DataFrame(mins_acc)

            fig_mins = go.Figure()
            fig_mins.add_trace(go.Scatter(
                x=df_mins_trend["GW"], y=df_mins_trend["Model MAE"],
                mode="lines+markers", name="Our Model",
                line=dict(color=MODEL_COL, width=2), marker=dict(size=6),
                hovertemplate="GW%{x}<br>Model xMins MAE: %{y:.1f}<extra></extra>",
            ))
            fig_mins.add_trace(go.Scatter(
                x=df_mins_trend["GW"], y=df_mins_trend["Solio MAE"],
                mode="lines+markers", name="Solio",
                line=dict(color=SOLIO_COL, width=2), marker=dict(size=6),
                hovertemplate="GW%{x}<br>Solio xMins MAE: %{y:.1f}<extra></extra>",
            ))
            fig_mins.update_layout(
                xaxis_title="Gameweek", yaxis_title="xMins MAE",
                height=320, legend=dict(orientation="h", yanchor="bottom", y=1.02),
                margin=dict(l=40, r=20, t=20, b=40), plot_bgcolor="#fafafa",
            )
            st.plotly_chart(fig_mins, use_container_width=True)

        st.markdown("---")

        # ── Component breakdown: Our Model vs Solio ──────────────────────────
        st.markdown("#### Component accuracy — Our Model vs Solio")
        st.caption(
            "Goals/Assists/CS: probability-based expected values vs 0/1 actuals. "
            "Bonus: Solio only — our proxy uses a different scale. Green = lower MAE."
        )

        comp2_rows = []
        for metric, mdl_col, sol_col, actual_col, fmt in [
            ("Goals",   "goal_prob",   "sol_goals",   "goals_scored", ".3f"),
            ("Assists", "assist_prob", "sol_assists",  "assists",      ".3f"),
            ("CS",      "cs_prob",     "sol_cs",      "clean_sheets", ".3f"),
            ("Bonus",   None,          "sol_bonus",   "bonus",        ".3f"),
        ]:
            if actual_col not in df.columns:
                continue
            sol_m, n = mae(df[sol_col], df[actual_col]) if sol_col in df.columns else (None, 0)
            sol_b    = bias(df[sol_col], df[actual_col]) if sol_col in df.columns else None
            if mdl_col is not None and mdl_col in df.columns:
                mdl_m, _ = mae(df[mdl_col], df[actual_col])
                mdl_b    = bias(df[mdl_col], df[actual_col])
            else:
                mdl_m, mdl_b = None, None
            comp2_rows.append({
                "Component":  metric,
                "Model MAE":  f"{mdl_m:{fmt}}" if mdl_m is not None else "—",
                "Model Bias": fmt_delta(mdl_b) if mdl_b is not None else "—",
                "Solio MAE":  f"{sol_m:{fmt}}"  if sol_m  is not None else "—",
                "Solio Bias": fmt_delta(sol_b),
                "n": n,
                "_mdl_raw": mdl_m,
                "_sol_raw": sol_m,
            })

        if comp2_rows:
            df_comp2 = pd.DataFrame(comp2_rows)

            def _comp_winner(row):
                style = [""] * len(row)
                try:
                    m, s = row["_mdl_raw"], row["_sol_raw"]
                    if m is not None and s is not None:
                        cols_l = list(row.index)
                        idx = cols_l.index("Model MAE" if m < s else "Solio MAE")
                        style[idx] = "background-color:#dcfce7;font-weight:600"
                except Exception:
                    pass
                return style

            st.dataframe(
                df_comp2.drop(columns=["_mdl_raw", "_sol_raw"])
                        .style.apply(_comp_winner, axis=1),
                hide_index=True, use_container_width=True,
            )

        st.markdown("---")

        # ── Scatter: xPts predicted vs actual ─────────────────────────────────
        st.markdown("#### Predicted vs Actual Points")

        fig = go.Figure()
        max_val = df["total_points"].max() + 2
        fig.add_trace(go.Scatter(
            x=[0, max_val], y=[0, max_val], mode="lines",
            line=dict(color="#cbd5e1", dash="dot", width=1),
            showlegend=False, hoverinfo="skip",
        ))
        for label, col, colour in [
            ("Our Model", "mdl_xpts", MODEL_COL),
            ("Solio",     "sol_xpts", SOLIO_COL),
        ]:
            m = df[col].notna()
            fig.add_trace(go.Scatter(
                x=df.loc[m, col], y=df.loc[m, "total_points"],
                mode="markers", name=label,
                marker=dict(color=colour, size=6, opacity=0.65),
                text=df.loc[m, "name"] + " (" + df.loc[m, "pos"] + ")",
                hovertemplate="<b>%{text}</b><br>Predicted: %{x:.2f}<br>Actual: %{y}<extra></extra>",
            ))
        fig.update_layout(
            xaxis_title="Predicted xPts", yaxis_title="Actual Points",
            height=400, legend=dict(orientation="h", yanchor="bottom", y=1.02),
            margin=dict(l=40, r=20, t=30, b=40), plot_bgcolor="#fafafa",
        )
        st.plotly_chart(fig, use_container_width=True)

        # ── Scatter: xMins ────────────────────────────────────────────────────
        st.markdown("#### Predicted vs Actual Minutes")
        st.caption("Minutes drive most of the xPts error — getting rotation and injury right matters most.")

        fig2 = go.Figure()
        fig2.add_trace(go.Scatter(
            x=[0, 200], y=[0, 200], mode="lines",
            line=dict(color="#cbd5e1", dash="dot", width=1),
            showlegend=False, hoverinfo="skip",
        ))
        for label, col, colour in [
            ("Our Model", "mdl_xmins", MODEL_COL),
            ("Solio",     "sol_xmins", SOLIO_COL),
        ]:
            m = df[col].notna()
            fig2.add_trace(go.Scatter(
                x=df.loc[m, col], y=df.loc[m, "minutes"],
                mode="markers", name=label,
                marker=dict(color=colour, size=6, opacity=0.65),
                text=df.loc[m, "name"] + " (" + df.loc[m, "pos"] + ")",
                hovertemplate="<b>%{text}</b><br>xMins: %{x:.0f}<br>Actual: %{y}<extra></extra>",
            ))
        fig2.update_layout(
            xaxis_title="Predicted xMins", yaxis_title="Actual Minutes",
            height=380, legend=dict(orientation="h", yanchor="bottom", y=1.02),
            margin=dict(l=40, r=20, t=30, b=40), plot_bgcolor="#fafafa",
        )
        st.plotly_chart(fig2, use_container_width=True)

        # ── Goal prob calibration ─────────────────────────────────────────────
        st.markdown("#### Our Model — Goal Probability Calibration")
        st.caption("Model goal_prob vs actual Opta xG. Dots coloured by goals scored.")

        m = df["goal_prob"].notna() & df["expected_goals"].notna()
        if m.sum() > 0:
            fig3 = go.Figure()
            fig3.add_trace(go.Scatter(
                x=df.loc[m, "goal_prob"], y=df.loc[m, "expected_goals"],
                mode="markers",
                marker=dict(
                    color=df.loc[m, "goals_scored"].fillna(0),
                    colorscale=[[0, "#e2e8f0"], [1, MODEL_COL]],
                    size=7, opacity=0.8,
                    colorbar=dict(title="Goals scored", thickness=12),
                ),
                text=df.loc[m, "name"] + " (" + df.loc[m, "pos"] + ")",
                hovertemplate=(
                    "<b>%{text}</b><br>Model goal prob: %{x:.3f}"
                    "<br>Actual xG: %{y:.3f}<extra></extra>"
                ),
            ))
            fig3.update_layout(
                xaxis_title="Model goal_prob", yaxis_title="Actual xG (Opta)",
                height=320, margin=dict(l=40, r=20, t=20, b=40), plot_bgcolor="#fafafa",
            )
            st.plotly_chart(fig3, use_container_width=True)


# ── Tab 2: By Player ──────────────────────────────────────────────────────────

with tab_players:
    if not has_model or df.empty:
        st.info(_NO_MODEL)
    else:
        c1, c2 = st.columns(2)
        with c1:
            pos_filter = st.multiselect(
                "Position", POS_ORDER, default=POS_ORDER, key="pos_fp"
            )
        with c2:
            show_only_model = st.checkbox("Only players with model projections", value=False)

        dv = df[df["pos"].isin(pos_filter)].copy()
        if show_only_model:
            dv = dv[dv["mdl_xpts"].notna()]

        dv["Mdl Δ Pts"]  = (dv["mdl_xpts"]  - dv["total_points"]).round(2)
        dv["Sol Δ Pts"]  = (dv["sol_xpts"]   - dv["total_points"]).round(2)
        dv["Mdl Δ Mins"] = (dv["mdl_xmins"]  - dv["minutes"]).round(1)
        dv["Sol Δ Mins"] = (dv["sol_xmins"]  - dv["minutes"]).round(1)

        _rename = {
            "name": "Player", "pos": "Pos", "team": "Team",
        }
        if all_gws_mode and "gw" in dv.columns:
            _rename["gw"] = "GW"

        display = dv.rename(columns={
            **_rename,
            "total_points": "Pts", "minutes": "Mins",
            "goals_scored": "Goals", "assists": "Ast", "clean_sheets": "CS", "bonus": "Bonus",
            "expected_goals": "xG", "expected_assists": "xA",
            "mdl_xpts": "Mdl xPts", "mdl_xmins": "Mdl xMins",
            "goal_prob": "Mdl Goal%", "assist_prob": "Mdl Ast%", "cs_prob": "Mdl CS%",
            "sol_xpts": "Sol xPts", "sol_xmins": "Sol xMins",
            "sol_goals": "Sol Goals", "sol_assists": "Sol Ast", "sol_cs": "Sol CS",
        })

        _gw_prefix = ["GW"] if all_gws_mode else []
        cols = _gw_prefix + [
            "Player", "Pos", "Team",
            "Pts", "Mins", "Goals", "Ast", "CS", "Bonus", "xG", "xA",
            "Mdl xPts", "Mdl xMins", "Mdl Goal%", "Mdl Ast%", "Mdl CS%",
            "Mdl Δ Pts", "Mdl Δ Mins",
            "Sol xPts", "Sol xMins", "Sol Goals", "Sol Ast", "Sol CS",
            "Sol Δ Pts", "Sol Δ Mins",
        ]
        cols = [c for c in cols if c in display.columns]

        def _delta_colour(val):
            try:
                v = float(val)
                if v >  2: return "color:#DC2626;font-weight:600"
                if v < -2: return "color:#2563EB;font-weight:600"
            except Exception:
                pass
            return ""

        delta_cols = [c for c in ["Mdl Δ Pts","Mdl Δ Mins","Sol Δ Pts","Sol Δ Mins"] if c in display.columns]
        _non_numeric = {"Player","Pos","Team","GW","Pts","Mins","Goals","Ast","CS","Bonus"}
        num_fmt    = {c: "{:.2f}" for c in cols if c not in _non_numeric}

        _cell_count = len(display) * len(cols)
        if _cell_count <= 262144:
            st.dataframe(
                display[cols].style
                    .format(num_fmt, na_rep="—")
                    .map(_delta_colour, subset=delta_cols),
                hide_index=True, use_container_width=True, height=620,
            )
        else:
            st.dataframe(
                display[cols],
                hide_index=True, use_container_width=True, height=620,
            )
        st.caption(
            "Δ = Predicted − Actual. Red >+2 (overestimate), Blue <-2 (underestimate). "
            "Sol Goals/Ast/CS = Solio's component predictions — compare against actual Goals/Ast/CS columns."
        )


# ── Tab 3: By Position ────────────────────────────────────────────────────────

with tab_positions:
    if not has_model or df.empty:
        st.info(_NO_MODEL)
    else:
        st.markdown("#### xPts MAE by Position — Our Model vs Solio")
        st.caption("Lower MAE = more accurate. Green = better of the two for each position.")

        pos_rows = []
        for pos in POS_ORDER:
            sub = df[df["pos"] == pos]
            if sub.empty:
                continue
            mdl_m, _ = mae(sub["mdl_xpts"], sub["total_points"])
            sol_m, _ = mae(sub["sol_xpts"], sub["total_points"])
            mdl_b    = bias(sub["mdl_xpts"], sub["total_points"])
            sol_b    = bias(sub["sol_xpts"], sub["total_points"])
            pos_rows.append({
                "Pos": pos, "n": len(sub),
                "Model MAE": mdl_m, "Model Bias": mdl_b,
                "Solio MAE": sol_m, "Solio Bias": sol_b,
            })

        df_pos = pd.DataFrame(pos_rows)

        def _pos_winner(row):
            style = [""] * len(row)
            try:
                cols_l = list(row.index)
                m, s = row["Model MAE"], row["Solio MAE"]
                if pd.notna(m) and pd.notna(s):
                    idx = cols_l.index("Model MAE" if m < s else "Solio MAE")
                    style[idx] = "background-color:#dcfce7;font-weight:600"
            except Exception:
                pass
            return style

        st.dataframe(
            df_pos.style
                .format({"Model MAE":"{:.3f}","Model Bias":"{:.3f}",
                         "Solio MAE":"{:.3f}","Solio Bias":"{:.3f}"}, na_rep="—")
                .apply(_pos_winner, axis=1),
            hide_index=True, use_container_width=True,
        )

        st.markdown("---")
        st.markdown("#### xMins MAE by Position")

        mins_rows = []
        for pos in POS_ORDER:
            sub = df[df["pos"] == pos]
            if sub.empty:
                continue
            mdl_m, _ = mae(sub["mdl_xmins"], sub["minutes"])
            sol_m, _ = mae(sub["sol_xmins"], sub["minutes"])
            mdl_b    = bias(sub["mdl_xmins"], sub["minutes"])
            sol_b    = bias(sub["sol_xmins"], sub["minutes"])
            mins_rows.append({
                "Pos": pos, "n": len(sub),
                "Model MAE": mdl_m, "Model Bias": mdl_b,
                "Solio MAE": sol_m, "Solio Bias": sol_b,
            })

        df_mins = pd.DataFrame(mins_rows)
        st.dataframe(
            df_mins.style
                .format({"Model MAE":"{:.1f}","Model Bias":"{:.1f}",
                         "Solio MAE":"{:.1f}","Solio Bias":"{:.1f}"}, na_rep="—")
                .apply(_pos_winner, axis=1),
            hide_index=True, use_container_width=True,
        )

        st.markdown("---")
        st.markdown("#### Component MAE by Position — Our Model vs Solio")
        st.caption("Goals/Assists/CS accuracy by position. Green = best per column.")

        comp_pos_rows = []
        for pos in POS_ORDER:
            sub = df[df["pos"] == pos]
            if sub.empty:
                continue
            row = {"Pos": pos, "n": len(sub)}
            for lbl, mdl_col, sol_col, ac in [
                ("Goals",   "goal_prob",   "sol_goals",  "goals_scored"),
                ("Assists", "assist_prob", "sol_assists", "assists"),
                ("CS",      "cs_prob",     "sol_cs",     "clean_sheets"),
            ]:
                mdl_m, _ = mae(sub[mdl_col], sub[ac]) if mdl_col in sub.columns else (None, 0)
                sol_m, _ = mae(sub[sol_col], sub[ac]) if sol_col in sub.columns else (None, 0)
                row[f"Mdl {lbl}"] = round(mdl_m, 3) if mdl_m is not None else None
                row[f"Sol {lbl}"] = round(sol_m, 3) if sol_m is not None else None
            comp_pos_rows.append(row)

        df_cp = pd.DataFrame(comp_pos_rows)
        mae_cp_cols = [c for c in df_cp.columns if c not in ("Pos", "n")]
        st.dataframe(
            df_cp.style
                .format({c: "{:.3f}" for c in mae_cp_cols}, na_rep="—")
                .highlight_min(subset=mae_cp_cols, color="#dcfce7", axis=0),
            hide_index=True, use_container_width=True,
        )

        st.markdown("---")
        with st.expander("Why might we differ from Solio?"):
            st.markdown("""
**Minutes (xMins)**
- Our model uses season-average minutes (`season_mins / gws_elapsed`) capped at 90 per fixture
- Solio uses a proprietary rotation model with injury news and team context
- **When we'll differ**: rotation-heavy teams, players returning from injury, late squad news

**Goal Probability**
- Our model: `g_per_90_shrunk × (xMins/90) × team_strength` with `RATE_SHRINK = 0.60`
- Shrinkage blends individual rate toward positional mean — dampens hot/cold streaks
- **When we'll differ**: players on streaks vs their season average

**Clean Sheet Probability**
- Our model: `CS_team × cs_share × mins_ratio` — CS_team from Dixon-Coles DC ratings
- Grounded in team defensive strength, not recent form
- **When we'll differ**: teams on form runs, key defender absences

**Bonus**
- Our model uses a simple `G_team/3 × 0.55` proxy — this is our weakest component
""")


# ── Tab 4: Biggest Misses ─────────────────────────────────────────────────────

with tab_misses:
    if not has_model or df.empty:
        st.info(_NO_MODEL)
    else:
        df_m = df[df["mdl_xpts"].notna()].copy()
        df_m["mdl_delta"] = (df_m["mdl_xpts"] - df_m["total_points"]).round(2)
        df_m["sol_delta"] = (df_m["sol_xpts"]  - df_m["total_points"]).round(2)

        col_over, col_under = st.columns(2)
        with col_over:
            st.markdown("#### 🔴 Model biggest overestimates")
            top = df_m.nlargest(10, "mdl_delta")[
                ["name","pos","team","total_points","mdl_xpts","sol_xpts","mdl_delta"]
            ]
            top.columns = ["Player","Pos","Team","Actual","Mdl xPts","Sol xPts","Mdl over by"]
            st.dataframe(
                top.style.format({"Mdl xPts":"{:.2f}","Sol xPts":"{:.2f}","Mdl over by":"{:.2f}"}),
                hide_index=True, use_container_width=True,
            )
        with col_under:
            st.markdown("#### 🔵 Model biggest underestimates")
            bot = df_m.nsmallest(10, "mdl_delta")[
                ["name","pos","team","total_points","mdl_xpts","sol_xpts","mdl_delta"]
            ]
            bot.columns = ["Player","Pos","Team","Actual","Mdl xPts","Sol xPts","Mdl under by"]
            st.dataframe(
                bot.style.format({"Mdl xPts":"{:.2f}","Sol xPts":"{:.2f}","Mdl under by":"{:.2f}"}),
                hide_index=True, use_container_width=True,
            )

        st.markdown("---")
        st.markdown("#### Where do our model and Solio disagree most?")
        st.caption(
            "Players where we diverge from Solio — cross-reference with actuals to see who was right."
        )

        df_m["vs_solio"] = (df_m["mdl_xpts"] - df_m["sol_xpts"]).round(2)
        df_div = df_m[df_m["sol_xpts"].notna()].copy()
        df_div["abs_div"] = df_div["vs_solio"].abs()
        top_div = df_div.nlargest(15, "abs_div")[
            ["name","pos","team","total_points","mdl_xpts","sol_xpts","vs_solio"]
        ]
        top_div.columns = ["Player","Pos","Team","Actual Pts","Our Model","Solio","Mdl vs Sol"]

        def _div_colour(val):
            try:
                v = float(val)
                if v >  1: return "color:#DC2626;font-weight:600"
                if v < -1: return "color:#2563EB;font-weight:600"
            except Exception:
                pass
            return ""

        st.dataframe(
            top_div.style
                .format({"Our Model":"{:.2f}","Solio":"{:.2f}","Mdl vs Sol":"{:.2f}"})
                .map(_div_colour, subset=["Mdl vs Sol"]),
            hide_index=True, use_container_width=True,
        )
        st.caption("Red = we predicted higher than Solio; Blue = we predicted lower.")


# ── Tab 5: Team Model ─────────────────────────────────────────────────────────

with tab_team:
    st.markdown("### ⚽ Team Model — Solio vs DC vs Actuals")
    st.caption(
        "Solio team G/GC/CS predictions for GW1–35 vs actual Premier League results. "
        "DC model shown where projections exist. DGW values summed across fixtures."
    )

    with st.spinner("Loading team results from football-data.co.uk…"):
        actuals       = load_team_actuals()
        solio_t, dc_t = load_team_predictions()

    if actuals.empty:
        st.warning("Could not load actual match results — check internet connection.")
    elif solio_t.empty:
        st.info("No Solio team predictions in the database yet.")
    else:
        df_t = actuals.merge(solio_t, on=["team","gw"], how="inner")
        if not dc_t.empty:
            df_t = df_t.merge(dc_t, on=["team","gw"], how="left")
        else:
            df_t["dc_g"] = df_t["dc_gc"] = df_t["dc_cs"] = np.nan
        df_t = df_t.sort_values(["gw","team"]).reset_index(drop=True)

        n_obs    = len(df_t)
        n_dc     = int(df_t["dc_g"].notna().sum())
        gw_range = f"GW{df_t['gw'].min()}–GW{df_t['gw'].max()}"

        sol_g_mae,  _ = mae(df_t["solio_g"],  df_t["actual_g"])
        sol_gc_mae, _ = mae(df_t["solio_gc"], df_t["actual_gc"])
        sol_cs_mae, _ = mae(df_t["solio_cs"], df_t["actual_cs"])
        sol_g_bias    = bias(df_t["solio_g"],  df_t["actual_g"])
        dc_g_mae,   _ = mae(df_t["dc_g"],     df_t["actual_g"])
        dc_g_bias     = bias(df_t["dc_g"],     df_t["actual_g"])

        c1, c2, c3, c4 = st.columns(4)
        for col, lbl, val, sub in [
            (c1, "Solio Goals MAE",
             f"{sol_g_mae:.3f}" if sol_g_mae else "—",
             f"bias {fmt_delta(sol_g_bias)} | {n_obs} team-GWs"),
            (c2, "Solio Goals Conceded MAE",
             f"{sol_gc_mae:.3f}" if sol_gc_mae else "—",
             gw_range),
            (c3, "Solio CS MAE",
             f"{sol_cs_mae:.3f}" if sol_cs_mae else "—",
             "prob vs 0/1 outcome"),
            (c4, "DC Goals MAE",
             f"{dc_g_mae:.3f}" if dc_g_mae else "—",
             f"bias {fmt_delta(dc_g_bias)} | {n_dc} obs" if dc_g_mae else "no DC data yet"),
        ]:
            with col:
                st.markdown(metric_card(lbl, val, sub), unsafe_allow_html=True)

        st.markdown("---")

        st.markdown("#### Goals MAE per Gameweek")
        gw_m = (
            df_t.groupby("gw")
            .apply(lambda g: pd.Series({
                "solio_mae": (g["solio_g"] - g["actual_g"]).abs().mean(),
                "dc_mae":    (g["dc_g"] - g["actual_g"]).abs().mean()
                              if g["dc_g"].notna().any() else np.nan,
            }), include_groups=False)
            .reset_index()
        )
        fig_gw = go.Figure()
        fig_gw.add_trace(go.Scatter(
            x=gw_m["gw"], y=gw_m["solio_mae"], mode="lines+markers",
            name="Solio", line=dict(color=SOLIO_COL, width=2), marker=dict(size=5),
            hovertemplate="GW%{x}<br>Solio MAE: %{y:.3f}<extra></extra>",
        ))
        dc_mask = gw_m["dc_mae"].notna()
        if dc_mask.any():
            fig_gw.add_trace(go.Scatter(
                x=gw_m.loc[dc_mask,"gw"], y=gw_m.loc[dc_mask,"dc_mae"],
                mode="lines+markers", name="DC Model",
                line=dict(color=DC_COL, width=2), marker=dict(size=7, symbol="diamond"),
                hovertemplate="GW%{x}<br>DC MAE: %{y:.3f}<extra></extra>",
            ))
        if sol_g_mae:
            fig_gw.add_hline(y=sol_g_mae, line_dash="dot", line_color=SOLIO_COL, opacity=0.4,
                             annotation_text=f"Solio avg {sol_g_mae:.3f}",
                             annotation_position="bottom right")
        fig_gw.update_layout(
            xaxis_title="Gameweek", yaxis_title="Goals MAE", height=360,
            legend=dict(orientation="h", yanchor="bottom", y=1.02),
            margin=dict(l=40,r=20,t=20,b=40), plot_bgcolor="#fafafa",
        )
        st.plotly_chart(fig_gw, use_container_width=True)

        st.markdown("#### Solio Predicted Goals vs Actual Goals")
        err = df_t["solio_g"] - df_t["actual_g"]
        fig_sc = go.Figure()
        max_g = max(df_t["actual_g"].max(), df_t["solio_g"].max()) + 0.5
        fig_sc.add_trace(go.Scatter(
            x=[0,max_g], y=[0,max_g], mode="lines",
            line=dict(color="#cbd5e1", dash="dot", width=1),
            showlegend=False, hoverinfo="skip",
        ))
        fig_sc.add_trace(go.Scatter(
            x=df_t["solio_g"], y=df_t["actual_g"], mode="markers",
            marker=dict(
                color=err, colorscale=[[0,SOLIO_COL],[0.5,"#e2e8f0"],[1,"#DC2626"]],
                cmid=0, size=6, opacity=0.7,
                colorbar=dict(title="Over/under", thickness=12),
            ),
            text=df_t["team"] + " GW" + df_t["gw"].astype(str),
            hovertemplate="<b>%{text}</b><br>Solio G: %{x:.2f}<br>Actual G: %{y:.2f}<extra></extra>",
        ))
        fig_sc.update_layout(
            xaxis_title="Solio Predicted Goals", yaxis_title="Actual Goals",
            height=400, margin=dict(l=40,r=20,t=20,b=40), plot_bgcolor="#fafafa",
        )
        st.plotly_chart(fig_sc, use_container_width=True)

        st.markdown("#### Accuracy by Team — full season")
        team_rows = []
        for team, grp in df_t.groupby("team"):
            sg_m,  _ = mae(grp["solio_g"],  grp["actual_g"])
            sg_b     = bias(grp["solio_g"],  grp["actual_g"])
            sgc_m, _ = mae(grp["solio_gc"], grp["actual_gc"])
            scs_m, _ = mae(grp["solio_cs"], grp["actual_cs"])
            dcg_m, _ = mae(grp["dc_g"],     grp["actual_g"])
            dcg_b    = bias(grp["dc_g"],     grp["actual_g"])
            team_rows.append({
                "Team":       team,
                "GWs":        len(grp),
                "Sol G MAE":  round(sg_m,  3) if sg_m  is not None else None,
                "Sol G Bias": round(sg_b,  3) if sg_b  is not None else None,
                "Sol GC MAE": round(sgc_m, 3) if sgc_m is not None else None,
                "Sol CS MAE": round(scs_m, 3) if scs_m is not None else None,
                "DC G MAE":   round(dcg_m, 3) if dcg_m is not None else None,
                "DC G Bias":  round(dcg_b, 3) if dcg_b is not None else None,
            })

        df_teams = pd.DataFrame(team_rows).sort_values("Sol G MAE")
        mae_cols  = [c for c in df_teams.columns if "MAE"  in c]
        bias_cols = [c for c in df_teams.columns if "Bias" in c]

        def _bias_col(val):
            try:
                v = float(val)
                if v >  0.3: return "color:#DC2626"
                if v < -0.3: return "color:#2563EB"
            except Exception:
                pass
            return ""

        st.dataframe(
            df_teams.style
                .format({c:"{:.3f}" for c in mae_cols+bias_cols}, na_rep="—")
                .highlight_min(subset=mae_cols, color="#dcfce7", axis=0)
                .map(_bias_col, subset=bias_cols),
            hide_index=True, use_container_width=True, height=560,
        )
        st.caption(
            "Sorted by Solio Goals MAE (best first). Green = best per column. "
            "Red bias = over-predicted; Blue = under-predicted. "
            "DC G MAE only shown for GWs where the DC model has run."
        )
