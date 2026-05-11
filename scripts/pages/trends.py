"""
FPL Projection Trends – multipage page
Navigated to from fpl_app.py; also works standalone via:
  streamlit run scripts/view_projections_trends.py
"""
import math
import sys
import sqlite3
import pandas as pd
import plotly.graph_objects as go
from pathlib import Path
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from utils.auth import require_auth, is_admin, show_logout_button

DB_PATH      = Path(__file__).resolve().parents[2] / 'outputs' / 'projections_history.db'
FIXTURE_PATH = Path(__file__).resolve().parents[2] / 'fixtures' / 'fixtures_all.csv'

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Barlow+Condensed:wght@400;600;700;800&family=Barlow:wght@400;500;600&display=swap');
html, body, [class*="css"], .stApp, .stMarkdown, .stButton > button,
.stSelectbox label, .stSelectbox div[data-baseweb="select"] {
    font-family: 'Barlow', sans-serif !important;
}
h1, h2, h3 { font-family: 'Barlow Condensed', sans-serif !important; font-weight: 800 !important; }
</style>
""", unsafe_allow_html=True)

require_auth()
show_logout_button()
_admin = is_admin()

st.title("📈 Projection Trends")

if st.button("← Back to Fixtures"):
    st.switch_page("fpl_app.py")

# ── Data loaders ──────────────────────────────────────────────────────────────

@st.cache_resource
def get_connection():
    return sqlite3.connect(DB_PATH, check_same_thread=False)

conn = get_connection()

@st.cache_data(ttl=600)
def get_matches():
    df = pd.read_csv(FIXTURE_PATH)
    df['GW']           = pd.to_numeric(df['GW'], errors='coerce')
    df['Home']         = df['Home'].str.strip()
    df['Away']         = df['Away'].str.strip()
    df['Kickoff_Date'] = pd.to_datetime(df['Kickoff_Date'], errors='coerce')
    today    = pd.Timestamp.now().normalize()
    upcoming = df[(df['Status'] == 'Upcoming') & (df['Kickoff_Date'] >= today)].copy()
    avail_gws  = sorted(upcoming['GW'].dropna().unique().astype(int))
    target_gws = avail_gws[:4]
    df_target  = upcoming[upcoming['GW'].isin(target_gws)]
    rows = []
    for _, row in df_target.iterrows():
        rows.append({'gw': int(row['GW']), 'team': row['Home'], 'opponent': row['Away']})
    return pd.DataFrame(rows)

def get_history(gw, team):
    df = pd.read_sql("""
        SELECT timestamp, g, gc, cs, method
        FROM projections
        WHERE gw = ? AND team = ?
        ORDER BY timestamp
    """, conn, params=(gw, team))
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    return df

@st.cache_data(ttl=600)
def get_solio_history(gw, team, metric_col):
    col = 'g' if metric_col == 'g' else 'cs'
    try:
        with sqlite3.connect(str(DB_PATH), check_same_thread=False) as c:
            df = pd.read_sql(
                f"SELECT ingested_at AS timestamp, {col} AS value "
                "FROM solio_fixture_snapshots WHERE gw = ? AND team = ? ORDER BY ingested_at",
                c, params=(gw, team)
            )
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        return df.dropna(subset=['value']).reset_index(drop=True)
    except Exception:
        return pd.DataFrame(columns=['timestamp', 'value'])

# ── UI ────────────────────────────────────────────────────────────────────────

col1, col2 = st.columns(2)

df_matches    = get_matches()
match_strings = list(df_matches.apply(
    lambda r: f"GW{int(r['gw'])}: {r['team']} v {r['opponent']}", axis=1
).unique())

# Pre-select match if navigating from fixtures page
pre_gw   = st.session_state.pop('pre_gw',   None)
pre_team = st.session_state.pop('pre_team', None)
default_idx = 0
if pre_gw and pre_team:
    for i, ms in enumerate(match_strings):
        if f"GW{pre_gw}:" in ms and pre_team in ms:
            default_idx = i
            break

with col1:
    st.markdown("**Step 1: Select Match**")
    selected_match = st.selectbox("Match", match_strings, index=default_idx, key="match_select")

match_parts = selected_match.split(": ")
gw          = int(match_parts[0].replace("GW", ""))
team_part   = match_parts[1].split(" v ")
team        = team_part[0]
opponent    = team_part[1]

with col2:
    st.markdown("**Step 2: Select Metric**")
    metric = st.selectbox("Metric", ["Goals (G)", "Clean Sheet %"], key="metric_select")

# ── Chart ─────────────────────────────────────────────────────────────────────

st.markdown("---")

def to_bst(df):
    """Convert UTC timestamps to Europe/London local time."""
    if df.empty: return df
    d = df.copy()
    d['timestamp'] = (d['timestamp'].dt.tz_localize('UTC')
                      .dt.tz_convert('Europe/London')
                      .dt.tz_localize(None))
    return d

try:
    hist1 = get_history(gw, team)
    hist2 = get_history(gw, opponent)

    if len(hist1) == 0 and len(hist2) == 0:
        st.warning(f"No projection history yet for GW{gw}. Check back after polling data.")
    else:
        metric_col = {"Goals (G)": "g", "Clean Sheet %": "cs"}[metric]
        is_pct     = metric_col == 'cs'
        scale      = 100 if is_pct else 1
        hover_fmt  = '.1f}%' if is_pct else '.2f}'

        solio1 = get_solio_history(gw, team, metric_col)     if _admin else pd.DataFrame(columns=['timestamp', 'value'])
        solio2 = get_solio_history(gw, opponent, metric_col) if _admin else pd.DataFrame(columns=['timestamp', 'value'])

        # Convert all timestamps to local time (BST/GMT) for display
        h1 = to_bst(hist1)
        h2 = to_bst(hist2)
        s1 = to_bst(solio1)
        s2 = to_bst(solio2)

        TEAM_COLOR     = '#2563EB'
        OPP_COLOR      = '#DC2626'
        TEAM_SOLIO     = 'rgba(37, 99, 235, 0.35)'
        OPP_SOLIO      = 'rgba(220, 38, 38, 0.35)'

        fig = go.Figure()

        if len(h1) > 0:
            fig.add_trace(go.Scatter(
                x=h1['timestamp'], y=h1[metric_col] * scale,
                name=team, mode='lines',
                line=dict(color=TEAM_COLOR, width=3, shape='spline', smoothing=1.3),
                hovertemplate='<b>' + team + '</b><br>%{x|%d-%b-%y %H:%M:%S}<br>' + metric + ': %{y:' + hover_fmt + '<extra></extra>'
            ))

        if len(h2) > 0:
            fig.add_trace(go.Scatter(
                x=h2['timestamp'], y=h2[metric_col] * scale,
                name=opponent, mode='lines',
                line=dict(color=OPP_COLOR, width=3, shape='spline', smoothing=1.3),
                hovertemplate='<b>' + opponent + '</b><br>%{x|%d-%b-%y %H:%M:%S}<br>' + metric + ': %{y:' + hover_fmt + '<extra></extra>'
            ))

        if _admin and len(s1) > 0:
            fig.add_trace(go.Scatter(
                x=s1['timestamp'], y=s1['value'] * scale,
                name=f'{team} (Solio)', mode='lines',
                line=dict(color=TEAM_SOLIO, dash='dot', width=2, shape='spline', smoothing=1.3),
                hovertemplate='<b>' + team + ' (Solio)</b><br>%{x|%d-%b-%y %H:%M:%S}<br>' + metric + ': %{y:' + hover_fmt + '<extra></extra>'
            ))

        if _admin and len(s2) > 0:
            fig.add_trace(go.Scatter(
                x=s2['timestamp'], y=s2['value'] * scale,
                name=f'{opponent} (Solio)', mode='lines',
                line=dict(color=OPP_SOLIO, dash='dot', width=2, shape='spline', smoothing=1.3),
                hovertemplate='<b>' + opponent + ' (Solio)</b><br>%{x|%d-%b-%y %H:%M:%S}<br>' + metric + ': %{y:' + hover_fmt + '<extra></extra>'
            ))

        all_y_vals = []
        for df_src, col in [(h1, metric_col), (h2, metric_col)]:
            if len(df_src) > 0:
                all_y_vals.extend((df_src[col] * scale).dropna().tolist())
        if _admin:
            for df_src in [s1, s2]:
                if len(df_src) > 0:
                    all_y_vals.extend((df_src['value'] * scale).dropna().tolist())
        y_max = math.ceil(max(all_y_vals)) if all_y_vals else (100 if is_pct else 1)

        fig.update_layout(
            title=f"GW{gw}: {team} v {opponent} – {metric}",
            xaxis_title="Time (local)",
            yaxis_title=metric,
            yaxis=dict(range=[0, y_max], ticksuffix='%' if is_pct else ''),
            hovermode='x unified',
            height=500,
            template="plotly_white",
            legend=dict(x=0.01, y=0.99)
        )

        st.plotly_chart(fig, use_container_width=True)

        st.markdown("---")
        col_a, col_b = st.columns(2)

        def fmt(val, pct): return f"{val * 100:.1f}%" if pct else f"{val:.2f}"

        with col_a:
            if len(h1) > 0:
                current_val = h1.iloc[-1][metric_col]
                last_time   = h1.iloc[-1]['timestamp']
                solio_val   = s1.iloc[-1]['value'] if (_admin and len(s1) > 0) else None
                sub_cols    = st.columns(2) if _admin else [st.container()]
                with sub_cols[0]:
                    st.metric(f"{team} (Model)", fmt(current_val, is_pct),
                              help=f"Last update: {last_time.strftime('%d-%b-%y %H:%M:%S')}")
                if _admin:
                    with sub_cols[1]:
                        st.metric(f"{team} (Solio)",
                                  fmt(solio_val, is_pct) if solio_val is not None else "—")

        with col_b:
            if len(h2) > 0:
                current_val = h2.iloc[-1][metric_col]
                last_time   = h2.iloc[-1]['timestamp']
                solio_val   = s2.iloc[-1]['value'] if (_admin and len(s2) > 0) else None
                sub_cols    = st.columns(2) if _admin else [st.container()]
                with sub_cols[0]:
                    st.metric(f"{opponent} (Model)", fmt(current_val, is_pct),
                              help=f"Last update: {last_time.strftime('%d-%b-%y %H:%M:%S')}")
                if _admin:
                    with sub_cols[1]:
                        st.metric(f"{opponent} (Solio)",
                                  fmt(solio_val, is_pct) if solio_val is not None else "—")

        st.markdown("---")
        st.subheader("Data Table")
        tab1, tab2 = st.tabs([team, opponent])

        def fmt_table(df_bst):
            d = df_bst[['timestamp', 'g', 'gc', 'cs', 'method']].copy()
            d['timestamp'] = d['timestamp'].dt.strftime('%d-%b-%y %H:%M:%S')
            d['g']  = d['g'].round(2)
            d['gc'] = d['gc'].round(2)
            d['cs'] = d['cs'].apply(lambda v: f'{v * 100:.1f}%')
            return d

        with tab1:
            if len(h1) > 0:
                st.dataframe(fmt_table(h1.sort_values('timestamp', ascending=False)),
                             use_container_width=True)
            else:
                st.info(f"No data yet for {team}")

        with tab2:
            if len(h2) > 0:
                st.dataframe(fmt_table(h2.sort_values('timestamp', ascending=False)),
                             use_container_width=True)
            else:
                st.info(f"No data yet for {opponent}")

except Exception as e:
    st.error(f"Error loading data: {e}")

st.markdown("---")
st.caption("Data collected every 2 hours via scheduled task. Refresh to see latest.")
