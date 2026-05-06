"""
Player Projections — visualise Solio and FPLReview xMins / xPts snapshots.
"""
import sys
import sqlite3
import pandas as pd
import plotly.graph_objects as go
from pathlib import Path
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from utils.auth import require_auth, show_logout_button

DB_PATH = Path(__file__).resolve().parents[2] / 'outputs' / 'projections_history.db'

SOLIO_COL   = '#2563EB'
FPLREV_COL  = '#DC2626'
SOLIO_FAINT = 'rgba(37,99,235,0.25)'
FPLREV_FAINT= 'rgba(220,38,38,0.25)'

st.set_page_config(page_title="FPL – Player Projections", layout="wide", page_icon="🎯")

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Barlow+Condensed:wght@400;600;700;800&family=Barlow:wght@400;500;600&display=swap');
html, body, [class*="css"], .stApp, .stMarkdown, .stButton > button {
    font-family: 'Barlow', sans-serif !important;
}
h1, h2, h3 { font-family: 'Barlow Condensed', sans-serif !important; font-weight: 800 !important; }
.block-container { max-width: 1000px !important; }
.stat-card {
    background:#f8f9fa; border:1px solid #e0e0e0; border-radius:10px;
    padding:12px 16px; text-align:center;
}
.stat-label { font-size:11px; color:#888; text-transform:uppercase; letter-spacing:0.8px; font-weight:600; }
.stat-value { font-family:'Barlow Condensed',sans-serif; font-size:26px; font-weight:800; color:#1a1a1a; }
.stat-sub   { font-size:11px; color:#666; margin-top:2px; }
</style>
""", unsafe_allow_html=True)

require_auth()
show_logout_button()

st.title("🎯 Player Projections")

if st.button("← Back to Fixtures"):
    st.switch_page("fpl_app.py")

# ── Data loaders ──────────────────────────────────────────────────────────────

@st.cache_data(ttl=300)
def get_player_list():
    try:
        with sqlite3.connect(str(DB_PATH), check_same_thread=False) as conn:
            return pd.read_sql("""
                SELECT name, team, pos,
                       AVG(bv) AS bv,
                       MIN(gw) AS min_gw,
                       MAX(gw) AS max_gw,
                       COUNT(DISTINCT source) AS sources
                FROM player_projection_snapshots
                GROUP BY name, team, pos
                ORDER BY name
            """, conn)
    except Exception:
        return pd.DataFrame()

@st.cache_data(ttl=300)
def get_latest_snapshot(name, team, source):
    """Latest ingested file rows for this player+source."""
    try:
        with sqlite3.connect(str(DB_PATH), check_same_thread=False) as conn:
            return pd.read_sql("""
                SELECT gw, xmins, pts, goals, assists, cs, bonus, cbit, eo, elite_pct,
                       ingested_at
                FROM player_projection_snapshots
                WHERE name = ? AND team = ? AND source = ?
                  AND ingested_at = (
                      SELECT MAX(ingested_at)
                      FROM player_projection_snapshots
                      WHERE name = ? AND team = ? AND source = ?
                  )
                ORDER BY gw
            """, conn, params=(name, team, source, name, team, source))
    except Exception:
        return pd.DataFrame()

@st.cache_data(ttl=300)
def get_trend_for_gw(name, team, gw):
    """One value per snapshot per source for a specific GW."""
    try:
        with sqlite3.connect(str(DB_PATH), check_same_thread=False) as conn:
            return pd.read_sql("""
                SELECT source, ingested_at, xmins, pts, goals, assists, cs
                FROM player_projection_snapshots
                WHERE name = ? AND team = ? AND gw = ?
                ORDER BY source, ingested_at
            """, conn, params=(name, team, int(gw)))
    except Exception:
        return pd.DataFrame()

@st.cache_data(ttl=300)
def get_all_snapshots(name, team):
    """All rows for all sources, used for the full projection table."""
    try:
        with sqlite3.connect(str(DB_PATH), check_same_thread=False) as conn:
            return pd.read_sql("""
                SELECT source, gw, xmins, pts, goals, assists, cs,
                       bonus, cbit, eo, elite_pct, ingested_at
                FROM player_projection_snapshots
                WHERE name = ? AND team = ?
                  AND ingested_at IN (
                      SELECT MAX(ingested_at)
                      FROM player_projection_snapshots
                      WHERE name = ? AND team = ?
                      GROUP BY source
                  )
                ORDER BY source, gw
            """, conn, params=(name, team, name, team))
    except Exception:
        return pd.DataFrame()

def stat_card(label, value, sub=""):
    sub_html = f'<div class="stat-sub">{sub}</div>' if sub else ''
    return (f'<div class="stat-card">'
            f'<div class="stat-label">{label}</div>'
            f'<div class="stat-value">{value}</div>'
            f'{sub_html}</div>')

# ── Player selection ──────────────────────────────────────────────────────────

players = get_player_list()

if players.empty:
    st.warning("No player projection data in the database yet. Upload Solio or FPLReview files first.")
    st.stop()

st.markdown("---")

fc1, fc2, fc3 = st.columns(3)
with fc1:
    pos_opts = ['All'] + sorted(players['pos'].dropna().unique().tolist())
    pos_filt = st.selectbox("Position", pos_opts, key="pos_filt")
with fc2:
    team_opts = ['All'] + sorted(players['team'].dropna().unique().tolist())
    team_filt = st.selectbox("Team", team_opts, key="team_filt")
with fc3:
    src_opts = ['Both', 'solio', 'fplreview']
    src_filt = st.selectbox("Source", src_opts, key="src_filt")

filtered = players.copy()
if pos_filt  != 'All': filtered = filtered[filtered['pos']  == pos_filt]
if team_filt != 'All': filtered = filtered[filtered['team'] == team_filt]
if src_filt  != 'Both':
    filtered = filtered[filtered['sources'] > 0]  # both always > 0

if filtered.empty:
    st.info("No players match the selected filters.")
    st.stop()

filtered['label'] = (filtered['name'] + '  ·  '
                     + filtered['team'] + '  ·  '
                     + filtered['pos'].fillna('')
                     + filtered['bv'].apply(lambda v: f'  ·  £{v:.1f}m' if pd.notna(v) else ''))

sel_label = st.selectbox("Player", filtered['label'].tolist(), key="player_sel")
sel_row   = filtered[filtered['label'] == sel_label].iloc[0]
name      = sel_row['name']
team      = sel_row['team']
pos       = sel_row['pos']
bv        = sel_row['bv']

st.markdown("---")

# ── Player header ─────────────────────────────────────────────────────────────

col_hdr, col_meta = st.columns([3, 1])
with col_hdr:
    st.markdown(f"### {name}")
    st.caption(f"{team}  ·  {pos}  ·  GWs {int(sel_row['min_gw'])}–{int(sel_row['max_gw'])}")
with col_meta:
    if pd.notna(bv):
        st.markdown(stat_card("Buy Value", f"£{bv:.1f}m"), unsafe_allow_html=True)

# Load latest snapshots per source
show_solio  = src_filt in ('Both', 'solio')
show_fplrev = src_filt in ('Both', 'fplreview')

df_solio  = get_latest_snapshot(name, team, 'solio')   if show_solio  else pd.DataFrame()
df_fplrev = get_latest_snapshot(name, team, 'fplreview') if show_fplrev else pd.DataFrame()

if df_solio.empty and df_fplrev.empty:
    st.info("No projection data found for this player.")
    st.stop()

# Summary metric strip — latest GW average xMins and Pts across sources
all_latest = get_all_snapshots(name, team)
if not all_latest.empty:
    avg = all_latest.groupby('source')[['xmins', 'pts']].mean().round(2)
    cols = st.columns(len(avg) * 2)
    i = 0
    for src, row in avg.iterrows():
        cols[i].markdown(stat_card(f"{src.upper()} avg xMins", f"{row['xmins']:.1f}",
                                   f"GW {int(all_latest[all_latest['source']==src]['gw'].min())}–"
                                   f"{int(all_latest[all_latest['source']==src]['gw'].max())}"),
                         unsafe_allow_html=True)
        cols[i+1].markdown(stat_card(f"{src.upper()} avg xPts", f"{row['pts']:.2f}", ""),
                           unsafe_allow_html=True)
        i += 2

st.markdown("<br>", unsafe_allow_html=True)

# ── Tabs ───────────────────────────────────────────────────────────────────────

tab_snap, tab_trend, tab_table = st.tabs(["📊 GW Snapshot", "📈 Projection Trend", "📋 Data Table"])

# ── Tab 1: GW Snapshot ────────────────────────────────────────────────────────

with tab_snap:
    metric_snap = st.radio("Metric", ["xMins", "xPts", "Goals", "Assists", "CS"],
                           horizontal=True, key="snap_metric")
    col_map = {"xMins": "xmins", "xPts": "pts", "Goals": "goals",
               "Assists": "assists", "CS": "cs"}
    snap_col = col_map[metric_snap]

    fig_snap = go.Figure()

    if not df_solio.empty and snap_col in df_solio.columns:
        ts = df_solio['ingested_at'].iloc[0]
        fig_snap.add_trace(go.Bar(
            x=df_solio['gw'],
            y=df_solio[snap_col].fillna(0),
            name=f'Solio ({ts[:10]})',
            marker_color=SOLIO_COL,
            hovertemplate='GW%{x} — Solio<br>' + metric_snap + ': <b>%{y:.2f}</b><extra></extra>',
        ))

    if not df_fplrev.empty and snap_col in df_fplrev.columns:
        ts = df_fplrev['ingested_at'].iloc[0]
        fig_snap.add_trace(go.Bar(
            x=df_fplrev['gw'],
            y=df_fplrev[snap_col].fillna(0),
            name=f'FPLReview ({ts[:10]})',
            marker_color=FPLREV_COL,
            hovertemplate='GW%{x} — FPLReview<br>' + metric_snap + ': <b>%{y:.2f}</b><extra></extra>',
        ))

    fig_snap.update_layout(
        barmode='group',
        title=f"{name} — {metric_snap} by Gameweek (latest snapshot)",
        xaxis_title="Gameweek",
        yaxis_title=metric_snap + (" (mins)" if metric_snap == "xMins" else ""),
        xaxis=dict(tickmode='linear', dtick=1),
        height=420,
        template='plotly_white',
        legend=dict(x=0.01, y=0.99),
        margin=dict(t=50),
    )
    st.plotly_chart(fig_snap, use_container_width=True)

    # Difference line (Solio − FPLReview) when both present
    if not df_solio.empty and not df_fplrev.empty and snap_col in df_solio.columns:
        merged = df_solio[['gw', snap_col]].merge(
            df_fplrev[['gw', snap_col]], on='gw', suffixes=('_solio', '_fplrev'), how='inner'
        )
        if not merged.empty:
            merged['diff'] = merged[f'{snap_col}_solio'] - merged[f'{snap_col}_fplrev']
            fig_diff = go.Figure()
            colors = [SOLIO_COL if v >= 0 else FPLREV_COL for v in merged['diff']]
            fig_diff.add_trace(go.Bar(
                x=merged['gw'], y=merged['diff'],
                marker_color=colors,
                hovertemplate='GW%{x}<br>Solio − FPLReview: <b>%{y:.2f}</b><extra></extra>',
                name='Solio − FPLReview',
            ))
            fig_diff.add_hline(y=0, line_color='#888', line_width=1)
            fig_diff.update_layout(
                title=f"Difference: Solio − FPLReview  ({metric_snap})",
                xaxis_title="Gameweek",
                yaxis_title="Δ " + metric_snap,
                xaxis=dict(tickmode='linear', dtick=1),
                height=250,
                template='plotly_white',
                showlegend=False,
                margin=dict(t=40),
            )
            st.plotly_chart(fig_diff, use_container_width=True)

# ── Tab 2: Projection Trend ────────────────────────────────────────────────────

with tab_trend:
    # Available GWs across both sources
    all_gws = set()
    if not df_solio.empty:  all_gws.update(df_solio['gw'].tolist())
    if not df_fplrev.empty: all_gws.update(df_fplrev['gw'].tolist())
    gw_opts = sorted(all_gws)

    if not gw_opts:
        st.info("No GW data available.")
    else:
        col_tgw, col_tmet = st.columns(2)
        with col_tgw:
            sel_gw = st.selectbox("Gameweek", gw_opts,
                                  format_func=lambda g: f"GW {g}", key="trend_gw")
        with col_tmet:
            metric_trend = st.radio("Metric", ["xMins", "xPts", "Goals", "Assists", "CS"],
                                    horizontal=True, key="trend_metric")

        trend_col  = col_map[metric_trend]
        df_trend   = get_trend_for_gw(name, team, sel_gw)

        if df_trend.empty:
            st.info(f"No trend data for GW {sel_gw}.")
        else:
            fig_trend = go.Figure()
            for src, color, faint in [
                ('solio',     SOLIO_COL,  SOLIO_FAINT),
                ('fplreview', FPLREV_COL, FPLREV_FAINT),
            ]:
                d = df_trend[df_trend['source'] == src].copy()
                if d.empty or trend_col not in d.columns:
                    continue
                d['ingested_at'] = pd.to_datetime(d['ingested_at'])
                d = d.dropna(subset=[trend_col]).sort_values('ingested_at')
                if d.empty:
                    continue
                label = src.upper()
                fig_trend.add_trace(go.Scatter(
                    x=d['ingested_at'],
                    y=d[trend_col],
                    name=label,
                    mode='lines+markers',
                    line=dict(color=color, width=3, shape='spline', smoothing=1.3),
                    marker=dict(size=8),
                    hovertemplate=(f'<b>{label}</b><br>%{{x|%Y-%m-%d %H:%M}}<br>'
                                   f'{metric_trend}: <b>%{{y:.2f}}</b><extra></extra>'),
                ))

            fig_trend.update_layout(
                title=f"{name} — GW{sel_gw} {metric_trend} over time",
                xaxis_title="Snapshot date",
                yaxis_title=metric_trend,
                height=400,
                template='plotly_white',
                hovermode='x unified',
                legend=dict(x=0.01, y=0.99),
                margin=dict(t=50),
            )
            st.plotly_chart(fig_trend, use_container_width=True)
            st.caption(f"Each point = one uploaded snapshot. {len(df_trend)} total data points for GW {sel_gw}.")

# ── Tab 3: Data Table ─────────────────────────────────────────────────────────

with tab_table:
    if not all_latest.empty:
        # Solio — full metrics
        solio_df = all_latest[all_latest['source'] == 'solio']
        fplrev_df = all_latest[all_latest['source'] == 'fplreview']

        if not solio_df.empty:
            st.markdown(f"**Solio** — latest snapshot: `{solio_df['ingested_at'].iloc[0][:10]}`")
            show_s = solio_df[['gw', 'xmins', 'pts', 'goals', 'assists', 'cs', 'bonus', 'cbit', 'eo']].copy()
            show_s = show_s.rename(columns={
                'gw': 'GW', 'xmins': 'xMins', 'pts': 'xPts', 'goals': 'Goals',
                'assists': 'Assists', 'cs': 'CS', 'bonus': 'Bonus', 'cbit': 'CBit', 'eo': 'EO'
            })
            for c in ['xMins', 'xPts', 'Goals', 'Assists', 'CS', 'Bonus', 'CBit', 'EO']:
                if c in show_s.columns:
                    show_s[c] = show_s[c].round(3)
            st.dataframe(show_s, use_container_width=True, hide_index=True)

        if not fplrev_df.empty:
            st.markdown(f"**FPLReview** — latest snapshot: `{fplrev_df['ingested_at'].iloc[0][:10]}`")
            show_f = fplrev_df[['gw', 'xmins', 'pts', 'elite_pct']].copy()
            show_f = show_f.rename(columns={
                'gw': 'GW', 'xmins': 'xMins', 'pts': 'xPts', 'elite_pct': 'Elite%'
            })
            for c in ['xMins', 'xPts', 'Elite%']:
                if c in show_f.columns:
                    show_f[c] = show_f[c].round(3)
            st.dataframe(show_f, use_container_width=True, hide_index=True)
    else:
        st.info("No data available.")

st.markdown("---")
st.caption("Source: Solio projection_all_metrics + FPLReview fplreview_*.csv — uploaded via Player Projections Upload page.")
