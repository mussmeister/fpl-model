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

SOLIO_COL    = '#2563EB'
FPLREV_COL   = '#DC2626'
MODEL_COL    = '#16A34A'
SOLIO_FAINT  = 'rgba(37,99,235,0.25)'
FPLREV_FAINT = 'rgba(220,38,38,0.25)'
MODEL_FAINT  = 'rgba(22,163,74,0.25)'

st.set_page_config(page_title="FPL – Player Projections", layout="wide", page_icon="🎯")

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Barlow+Condensed:wght@400;600;700;800&family=Barlow:wght@400;500;600&display=swap');
html, body, [class*="css"], .stApp, .stMarkdown, .stButton > button {
    font-family: 'Barlow', sans-serif !important;
}
h1, h2, h3 { font-family: 'Barlow Condensed', sans-serif !important; font-weight: 800 !important; }
.block-container { padding-left: 1rem !important; padding-right: 1rem !important; }
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

@st.cache_data(ttl=300)
def get_model_snapshot(name, team):
    """Latest model projection snapshot for this player — one row per GW."""
    try:
        with sqlite3.connect(str(DB_PATH), check_same_thread=False) as conn:
            return pd.read_sql("""
                SELECT gw, xmins, xpts AS pts, goal_prob, assist_prob,
                       cs_prob, bonus_prob, appearance_pts, save_pts, timestamp
                FROM player_projection_model
                WHERE name = ? AND team = ?
                  AND timestamp = (
                      SELECT MAX(timestamp) FROM player_projection_model
                      WHERE name = ? AND team = ?
                  )
                ORDER BY gw
            """, conn, params=(name, team, name, team))
    except Exception:
        return pd.DataFrame()

@st.cache_data(ttl=300)
def get_model_trend_for_gw(name, team, gw):
    """Model xMins/xPts for a specific GW across all stored timestamps."""
    try:
        with sqlite3.connect(str(DB_PATH), check_same_thread=False) as conn:
            return pd.read_sql("""
                SELECT timestamp, xmins, xpts AS pts, goal_prob, assist_prob, cs_prob
                FROM player_projection_model
                WHERE name = ? AND team = ? AND gw = ?
                ORDER BY timestamp
            """, conn, params=(name, team, int(gw)))
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
df_model  = get_model_snapshot(name, team)

if df_solio.empty and df_fplrev.empty and df_model.empty:
    st.info("No projection data found for this player.")
    st.stop()

# Summary metric strip — latest GW average xMins and Pts across sources
all_latest = get_all_snapshots(name, team)
stat_entries = []
if not all_latest.empty:
    avg = all_latest.groupby('source')[['xmins', 'pts']].mean().round(2)
    for src, row in avg.iterrows():
        label_gw = (f"GW {int(all_latest[all_latest['source']==src]['gw'].min())}–"
                    f"{int(all_latest[all_latest['source']==src]['gw'].max())}")
        stat_entries.append((f"{src.upper()} avg xMins", f"{row['xmins']:.1f}", label_gw))
        stat_entries.append((f"{src.upper()} avg xPts",  f"{row['pts']:.2f}",  ""))
if not df_model.empty:
    mdl_gw_min = int(df_model['gw'].min())
    mdl_gw_max = int(df_model['gw'].max())
    stat_entries.append(("Model avg xMins", f"{df_model['xmins'].mean():.1f}",
                          f"GW {mdl_gw_min}–{mdl_gw_max}"))
    stat_entries.append(("Model avg xPts",  f"{df_model['pts'].mean():.2f}",
                          df_model['timestamp'].iloc[-1][:16] if not df_model.empty else ""))

if stat_entries:
    cols = st.columns(len(stat_entries))
    for i, (lbl, val, sub) in enumerate(stat_entries):
        cols[i].markdown(stat_card(lbl, val, sub), unsafe_allow_html=True)

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

    # Model — xMins and xPts only (Goals/Assists/CS are probabilities in the model)
    model_snap_col = snap_col if snap_col in ('xmins', 'pts') else None
    if not df_model.empty and model_snap_col and model_snap_col in df_model.columns:
        ts = df_model['timestamp'].iloc[-1]
        fig_snap.add_trace(go.Bar(
            x=df_model['gw'],
            y=df_model[model_snap_col].fillna(0),
            name=f'Model ({ts[:10]})',
            marker_color=MODEL_COL,
            hovertemplate='GW%{x} — Model<br>' + metric_snap + ': <b>%{y:.2f}</b><extra></extra>',
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
    # Available GWs across all sources including model
    all_gws = set()
    if not df_solio.empty:  all_gws.update(df_solio['gw'].tolist())
    if not df_fplrev.empty: all_gws.update(df_fplrev['gw'].tolist())
    if not df_model.empty:  all_gws.update(df_model['gw'].tolist())
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
        df_mdl_trend = get_model_trend_for_gw(name, team, sel_gw) if trend_col in ('xmins', 'pts') else pd.DataFrame()

        if df_trend.empty and df_mdl_trend.empty:
            st.info(f"No trend data for GW {sel_gw}.")
        else:
            fig_trend = go.Figure()

            for src, color, faint in [
                ('solio',     SOLIO_COL,  SOLIO_FAINT),
                ('fplreview', FPLREV_COL, FPLREV_FAINT),
            ]:
                if df_trend.empty:
                    continue
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
                    line=dict(color=color, width=3, shape='spline', smoothing=1.3, dash='dot'),
                    marker=dict(size=8, symbol='circle-open'),
                    hovertemplate=(f'<b>{label}</b><br>%{{x|%Y-%m-%d %H:%M}}<br>'
                                   f'{metric_trend}: <b>%{{y:.2f}}</b><extra></extra>'),
                ))

            # Model trend line (xMins / xPts only) — solid green, latest day only
            if not df_mdl_trend.empty and trend_col in df_mdl_trend.columns:
                dm = df_mdl_trend.copy()
                dm['timestamp'] = pd.to_datetime(dm['timestamp'])
                dm = dm.dropna(subset=[trend_col]).sort_values('timestamp')
                if not dm.empty:
                    # Only show snapshots from the most recent calendar day
                    latest_day = dm['timestamp'].dt.normalize().max()
                    dm = dm[dm['timestamp'].dt.normalize() == latest_day]
                if not dm.empty:
                    fig_trend.add_trace(go.Scatter(
                        x=dm['timestamp'],
                        y=dm[trend_col],
                        name='Model',
                        mode='lines+markers',
                        line=dict(color=MODEL_COL, width=3, shape='spline', smoothing=1.3),
                        marker=dict(size=8),
                        hovertemplate=(f'<b>Model</b><br>%{{x|%Y-%m-%d %H:%M}}<br>'
                                       f'{metric_trend}: <b>%{{y:.2f}}</b><extra></extra>'),
                    ))

            n_bench = len(df_trend) if not df_trend.empty else 0
            n_model = len(df_mdl_trend) if not df_mdl_trend.empty else 0
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
            st.caption(f"Benchmarks: {n_bench} data points. Model: {n_model} snapshots (every 2h).")

# ── Tab 3: Data Table ─────────────────────────────────────────────────────────

with tab_table:
    _solio_tbl  = all_latest[all_latest['source'] == 'solio'].copy()   if not all_latest.empty else pd.DataFrame()
    _fplrev_tbl = all_latest[all_latest['source'] == 'fplreview'].copy() if not all_latest.empty else pd.DataFrame()

    # Unified column order — every table uses exactly these headings
    _UCOLS = ['GW','xMins','xPts','Goals','Assists','CS','Bonus',
              'App Pts','Save Pts','CBit','EO','Elite%']

    def _unify_model(df):
        r = df[['gw','xmins','pts','goal_prob','assist_prob','cs_prob',
                'bonus_prob','appearance_pts','save_pts']].copy().reset_index(drop=True)
        r.columns = ['GW','xMins','xPts','Goals','Assists','CS','Bonus','App Pts','Save Pts']
        for c in ('CBit','EO','Elite%'): r[c] = float('nan')
        return r[_UCOLS]

    def _unify_solio(df):
        r = df[['gw','xmins','pts','goals','assists','cs','bonus','cbit','eo']].copy().reset_index(drop=True)
        r.columns = ['GW','xMins','xPts','Goals','Assists','CS','Bonus','CBit','EO']
        for c in ('App Pts','Save Pts','Elite%'): r[c] = float('nan')
        return r[_UCOLS]

    def _unify_fplrev(df):
        r = df[['gw','xmins','pts','elite_pct']].copy().reset_index(drop=True)
        r.columns = ['GW','xMins','xPts','Elite%']
        for c in ('Goals','Assists','CS','Bonus','App Pts','Save Pts','CBit','EO'): r[c] = float('nan')
        return r[_UCOLS]

    # ── GW selector — highlights same row in all tables ───────────────────
    _gw_pool: set = set()
    for _d in [_solio_tbl, _fplrev_tbl, df_model]:
        if not _d.empty:
            _gw_pool.update(_d['gw' if 'gw' in _d.columns else 'GW'].tolist())
    _highlight_gw = None
    if _gw_pool:
        _highlight_gw = st.selectbox(
            "Highlight gameweek",
            [None] + sorted(int(g) for g in _gw_pool),
            format_func=lambda g: "— all rows —" if g is None else f"GW {g}",
            key="table_highlight_gw",
        )

    # Formatting: 2 dp for all numeric cols, "—" for NaN, int for GW
    _nan2 = lambda v: '—' if pd.isna(v) else f'{v:.2f}'
    _FMT = {c: _nan2 for c in _UCOLS if c != 'GW'}
    _FMT['GW'] = lambda v: '—' if pd.isna(v) else str(int(v))

    _sm = st.column_config.NumberColumn(width="small")
    _cfg_u = {c: st.column_config.TextColumn(c, width="small") for c in _UCOLS}

    def _styled(df, delta_cols=()):
        fmt = {c: v for c, v in _FMT.items() if c in df.columns}
        for c in df.columns:
            if c not in fmt:
                fmt[c] = _nan2
        def _row(row):
            is_hl = _highlight_gw is not None and 'GW' in row.index and row['GW'] == _highlight_gw
            bg = 'background-color:#fef9c3; font-weight:600; ' if is_hl else ''
            out = []
            for col in row.index:
                s = bg
                if col in delta_cols:
                    val = row[col]
                    if pd.notna(val) and val != 0:
                        s += ('color:#16a34a; font-weight:600' if val > 0
                              else 'color:#dc2626; font-weight:600')
                out.append(s)
            return out
        return df.style.format(fmt).apply(_row, axis=1)

    # ── Own Model ─────────────────────────────────────────────────────────
    if not df_model.empty:
        st.markdown(f"**Own Model** — latest snapshot: `{df_model['timestamp'].iloc[-1][:16]}`")
        st.dataframe(_styled(_unify_model(df_model)), hide_index=True,
                     use_container_width=True, column_config=_cfg_u)

    # ── Solio ─────────────────────────────────────────────────────────────
    if not _solio_tbl.empty:
        st.markdown(f"**Solio** — latest snapshot: `{_solio_tbl['ingested_at'].iloc[0][:10]}`")
        st.dataframe(_styled(_unify_solio(_solio_tbl)), hide_index=True,
                     use_container_width=True, column_config=_cfg_u)

    # ── FPLReview ─────────────────────────────────────────────────────────
    if not _fplrev_tbl.empty:
        st.markdown(f"**FPLReview** — latest snapshot: `{_fplrev_tbl['ingested_at'].iloc[0][:10]}`")
        st.dataframe(_styled(_unify_fplrev(_fplrev_tbl)), hide_index=True,
                     use_container_width=True, column_config=_cfg_u)

    if all_latest.empty:
        st.info("No benchmark data available.")

    # ── Model vs Benchmark difference ─────────────────────────────────────
    _has_bench = not _solio_tbl.empty or not _fplrev_tbl.empty
    if not df_model.empty and _has_bench:
        st.markdown("**Model vs Benchmark** — model minus benchmark average")
        _parts = [_d[['gw','xmins','pts']] for _d in [_solio_tbl, _fplrev_tbl] if not _d.empty]
        _bench = pd.concat(_parts).groupby('gw')[['xmins','pts']].mean().reset_index()
        _bench.columns = ['gw','bench_xmins','bench_xpts']
        _diff = _bench.merge(df_model[['gw','xmins','pts']], on='gw', how='inner')
        _diff['Δ xMins'] = (_diff['xmins'] - _diff['bench_xmins']).round(2)
        _diff['Δ xPts']  = (_diff['pts']   - _diff['bench_xpts']).round(2)
        _diff = _diff.rename(columns={
            'gw':'GW', 'bench_xmins':'Bench xMins', 'bench_xpts':'Bench xPts',
            'xmins':'Mdl xMins', 'pts':'Mdl xPts',
        })
        for c in ('Bench xMins','Mdl xMins','Bench xPts','Mdl xPts'):
            _diff[c] = _diff[c].round(2)
        _diff = _diff[['GW','Bench xMins','Mdl xMins','Δ xMins','Bench xPts','Mdl xPts','Δ xPts']]
        _diff_cfg = {c: st.column_config.TextColumn(c, width="small")
                     for c in _diff.columns}
        st.dataframe(
            _styled(_diff, delta_cols=('Δ xMins','Δ xPts')),
            hide_index=True, use_container_width=True, column_config=_diff_cfg,
        )

st.markdown("---")
st.caption("Benchmarks: Solio + FPLReview uploads. Own model: DC team ratings + FPL player universe, refreshed every 2h.")
