"""
Streamlit app to visualize projection trends over time.
Run: streamlit run view_projections_trends.py
"""
import math
import sys
import streamlit as st
import sqlite3
import pandas as pd
import plotly.graph_objects as go
from pathlib import Path
from datetime import datetime, timedelta

sys.path.insert(0, str(Path(__file__).resolve().parent))
from utils.team_mappings import to_short, SOLIO_TO_SHORT

DB_PATH      = Path(__file__).resolve().parents[1] / 'outputs' / 'projections_history.db'
FIXTURE_PATH = Path(__file__).resolve().parents[1] / 'fixtures' / 'fixtures_all.csv'
SOLIO_DIR    = Path(__file__).resolve().parents[1] / 'solio'

st.set_page_config(page_title="FPL Projections Tracker", layout="wide")
st.title("📊 FPL Projections Tracker")
st.markdown("Track team projections over time. See how market odds shift expectations.")

# === LOAD DATA ===
@st.cache_resource
def get_connection():
    return sqlite3.connect(DB_PATH, check_same_thread=False)

conn = get_connection()

# Get available matches from fixtures CSV so DGW fixtures always appear
@st.cache_data(ttl=600)
def get_matches():
    """Return one row per fixture (home perspective) for upcoming GWs."""
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
    """Get all history for a team in a specific GW."""
    df = pd.read_sql("""
        SELECT timestamp, g, gc, cs, method
        FROM projections
        WHERE gw = ? AND team = ?
        ORDER BY timestamp
    """, conn, params=(gw, team))
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    return df

def get_solio_history(gw, team, metric_col):
    """Load Solio snapshots across all fixture_difficulty CSVs, using file mtime as timestamp."""
    col_name = f'{gw}_G' if metric_col == 'g' else f'{gw}_CS'
    rows = []
    for path in sorted(SOLIO_DIR.glob('fixture_difficulty_all_metrics*.csv')):
        try:
            df = pd.read_csv(path, encoding='utf-8-sig')
            df['Team'] = df['Team'].apply(lambda x: to_short(x, SOLIO_TO_SHORT))
            team_row = df[df['Team'] == team]
            if len(team_row) == 0 or col_name not in df.columns:
                continue
            val = pd.to_numeric(team_row.iloc[0][col_name], errors='coerce')
            if pd.isna(val):
                continue
            mtime = datetime.fromtimestamp(path.stat().st_mtime)
            rows.append({'timestamp': mtime, 'value': float(val)})
        except Exception:
            continue
    if not rows:
        return pd.DataFrame(columns=['timestamp', 'value'])
    return pd.DataFrame(rows).sort_values('timestamp').reset_index(drop=True)

# === UI ===
col1, col2 = st.columns(2)

df_matches = get_matches()

# Dropdown 1: Select Match
with col1:
    st.markdown("**Step 1: Select Match**")
    match_strings = df_matches.apply(
        lambda r: f"GW{int(r['gw'])}: {r['team']} v {r['opponent']}",
        axis=1
    ).unique()
    selected_match = st.selectbox("Match", match_strings, key="match_select")

# Extract GW, team, opponent from selection
match_parts = selected_match.split(": ")
gw_part = match_parts[0].replace("GW", "")
gw = int(gw_part)
team_part = match_parts[1].split(" v ")
team     = team_part[0]
opponent = team_part[1]

# Dropdown 2: Select Metric
with col2:
    st.markdown("**Step 2: Select Metric**")
    metric = st.selectbox("Metric", ["Goals (G)", "Clean Sheet %"], key="metric_select")

# === FETCH DATA ===
st.markdown("---")

try:
    hist1 = get_history(gw, team)
    hist2 = get_history(gw, opponent)
    
    if len(hist1) == 0 and len(hist2) == 0:
        st.warning(f"No projection history yet for GW{gw}. Check back after polling data.")
    else:
        # Map metric to column
        metric_col = {"Goals (G)": "g", "Clean Sheet %": "cs"}[metric]
        
        # Create chart
        fig = go.Figure()
        
        is_pct    = metric_col == 'cs'
        scale     = 100 if is_pct else 1
        hover_fmt = '.1f}%' if is_pct else '.3f}'

        solio1 = get_solio_history(gw, team, metric_col)
        solio2 = get_solio_history(gw, opponent, metric_col)

        if len(hist1) > 0:
            fig.add_trace(go.Scatter(
                x=hist1['timestamp'],
                y=hist1[metric_col] * scale,
                name=team,
                mode='lines+markers',
                line=dict(width=2),
                hovertemplate='<b>' + team + '</b><br>%{x|%Y-%m-%d %H:%M}<br>' + metric + ': %{y:' + hover_fmt + '<extra></extra>'
            ))

        if len(hist2) > 0:
            fig.add_trace(go.Scatter(
                x=hist2['timestamp'],
                y=hist2[metric_col] * scale,
                name=opponent,
                mode='lines+markers',
                line=dict(width=2),
                hovertemplate='<b>' + opponent + '</b><br>%{x|%Y-%m-%d %H:%M}<br>' + metric + ': %{y:' + hover_fmt + '<extra></extra>'
            ))

        if len(solio1) > 0:
            fig.add_trace(go.Scatter(
                x=solio1['timestamp'],
                y=solio1['value'] * scale,
                name=f'{team} (Solio)',
                mode='lines+markers',
                line=dict(color='red', dash='dot', width=2),
                hovertemplate='<b>' + team + ' (Solio)</b><br>%{x|%Y-%m-%d %H:%M}<br>' + metric + ': %{y:' + hover_fmt + '<extra></extra>'
            ))

        if len(solio2) > 0:
            fig.add_trace(go.Scatter(
                x=solio2['timestamp'],
                y=solio2['value'] * scale,
                name=f'{opponent} (Solio)',
                mode='lines+markers',
                line=dict(color='blue', dash='dot', width=2),
                hovertemplate='<b>' + opponent + ' (Solio)</b><br>%{x|%Y-%m-%d %H:%M}<br>' + metric + ': %{y:' + hover_fmt + '<extra></extra>'
            ))

        all_y_vals = []
        if len(hist1) > 0:
            all_y_vals.extend((hist1[metric_col] * scale).dropna().tolist())
        if len(hist2) > 0:
            all_y_vals.extend((hist2[metric_col] * scale).dropna().tolist())
        if len(solio1) > 0:
            all_y_vals.extend((solio1['value'] * scale).dropna().tolist())
        if len(solio2) > 0:
            all_y_vals.extend((solio2['value'] * scale).dropna().tolist())
        y_max = math.ceil(max(all_y_vals)) if all_y_vals else (100 if is_pct else 1)

        fig.update_layout(
            title=f"GW{gw}: {team} v {opponent} – {metric}",
            xaxis_title="Time",
            yaxis_title=metric,
            yaxis=dict(range=[0, y_max], ticksuffix='%' if is_pct else ''),
            hovermode='x unified',
            height=500,
            template="plotly_white",
            legend=dict(x=0.01, y=0.99)
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
        # Show current values
        st.markdown("---")
        col_a, col_b = st.columns(2)
        
        with col_a:
            if len(hist1) > 0:
                current_val = hist1.iloc[-1][metric_col]
                last_time   = hist1.iloc[-1]['timestamp']
                display_val = f"{current_val * 100:.1f}%" if is_pct else f"{current_val:.3f}"
                st.metric(team, display_val, help=f"Last update: {last_time.strftime('%Y-%m-%d %H:%M')}")

        with col_b:
            if len(hist2) > 0:
                current_val = hist2.iloc[-1][metric_col]
                last_time   = hist2.iloc[-1]['timestamp']
                display_val = f"{current_val * 100:.1f}%" if is_pct else f"{current_val:.3f}"
                st.metric(opponent, display_val, help=f"Last update: {last_time.strftime('%Y-%m-%d %H:%M')}")
        
        # Show data table
        st.markdown("---")
        st.subheader("Data Table")
        
        tab1, tab2 = st.tabs([team, opponent])
        
        with tab1:
            if len(hist1) > 0:
                st.dataframe(hist1.sort_values('timestamp', ascending=False), use_container_width=True)
            else:
                st.info(f"No data yet for {team}")
        
        with tab2:
            if len(hist2) > 0:
                st.dataframe(hist2.sort_values('timestamp', ascending=False), use_container_width=True)
            else:
                st.info(f"No data yet for {opponent}")

except Exception as e:
    st.error(f"Error loading data: {e}")

st.markdown("---")
st.markdown("**Note:** Data is collected every 2 hours. Refresh this page to see the latest updates.")
