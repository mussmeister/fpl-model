"""
FPL Dashboard – Fixtures Overview
Run:  streamlit run scripts/fpl_app.py
"""
import math
import sys
import sqlite3
import urllib.parse
import pandas as pd
from pathlib import Path
from datetime import datetime
import streamlit as st

ROOT         = Path(__file__).resolve().parent.parent
DB_PATH      = ROOT / 'outputs' / 'projections_history.db'
FIXTURE_PATH = ROOT / 'fixtures' / 'fixtures_all.csv'

BLUE_SOLID   = "#4472C4"
ORANGE_SOLID = "#C4632A"
ORANGE_LIGHT = "#FDDDB9"
WHITE        = "#FFFFFF"

st.set_page_config(page_title="FPL Dashboard", layout="wide", page_icon="⚽")

# Handle card-click navigation before rendering anything
_nav = st.query_params.get('navigate', '')
if _nav:
    try:
        _home, _away, _gw = urllib.parse.unquote(_nav).split('|')
        st.session_state['pre_gw']   = int(_gw)
        st.session_state['pre_team'] = _home
    except Exception:
        pass
    st.query_params.clear()
    st.switch_page("pages/trends.py")

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Barlow+Condensed:wght@400;600;700;800&family=Barlow:wght@400;500;600&display=swap');

html, body, [class*="css"], .stApp, .stMarkdown, .stButton > button,
.stSelectbox label, .stSelectbox div[data-baseweb="select"] {
    font-family: 'Barlow', sans-serif !important;
}

h1, h2, h3 {
    font-family: 'Barlow Condensed', sans-serif !important;
    font-weight: 800 !important;
    letter-spacing: 0.5px;
}

.block-container {
    max-width: 960px !important;
    padding-left: 2rem !important;
    padding-right: 2rem !important;
}
</style>
""", unsafe_allow_html=True)

# ── Data loaders ──────────────────────────────────────────────────────────────

@st.cache_data(ttl=600)
def load_fixtures_df():
    df = pd.read_csv(FIXTURE_PATH)
    df['GW']           = pd.to_numeric(df['GW'], errors='coerce')
    df['Home']         = df['Home'].str.strip()
    df['Away']         = df['Away'].str.strip()
    df['Kickoff_Date'] = pd.to_datetime(df['Kickoff_Date'], errors='coerce')
    return df

@st.cache_data(ttl=3600)
def load_abbreviations():
    try:
        with sqlite3.connect(str(DB_PATH), check_same_thread=False) as conn:
            cur = conn.execute(
                "SELECT team, abbr FROM solio_fixture_snapshots "
                "WHERE ingested_at = (SELECT MAX(ingested_at) FROM solio_fixture_snapshots) "
                "AND abbr IS NOT NULL AND abbr != ''"
            )
            return {row[0]: row[1] for row in cur.fetchall()}
    except Exception:
        return {}

def get_abbr(team, abbr_map):
    return abbr_map.get(team, team[:3].upper())

def get_latest_projections(gw):
    with sqlite3.connect(str(DB_PATH), check_same_thread=False) as conn:
        cur = conn.execute(
            "SELECT team, g, cs, timestamp FROM projections WHERE gw = ? ORDER BY timestamp DESC",
            (int(gw),)
        )
        rows = cur.fetchall()
    if not rows:
        return {}
    df = pd.DataFrame(rows, columns=['team', 'g', 'cs', 'timestamp'])
    latest_ts = df['timestamp'].max()
    df = df[df['timestamp'] == latest_ts]
    return {r['team']: {'g': float(r['g']), 'cs': float(r['cs'])} for _, r in df.iterrows()}

# ── HTML cell helpers ─────────────────────────────────────────────────────────

NUM_FONT = "font-family:'Barlow Condensed',sans-serif;font-weight:700;"

def goals_td(val, higher):
    if val is None:
        return f'<td style="padding:5px 12px;text-align:center;min-width:72px;{NUM_FONT}">—</td>'
    s = f"{val:.2f}"
    if higher:
        return (f'<td style="background:{BLUE_SOLID};color:{WHITE};padding:5px 12px;'
                f'border-radius:5px;text-align:center;min-width:72px;{NUM_FONT}">{s}</td>')
    return f'<td style="padding:5px 12px;text-align:center;color:#333;min-width:72px;{NUM_FONT}">{s}</td>'

def cs_td(val, higher):
    if val is None:
        return f'<td style="padding:5px 10px;text-align:center;min-width:58px;{NUM_FONT}">—</td>'
    pct = val * 100
    s   = f"{pct:.0f}%"
    if pct >= 40:
        return (f'<td style="background:{ORANGE_SOLID};color:{WHITE};padding:5px 10px;'
                f'border-radius:5px;text-align:center;min-width:58px;{NUM_FONT}">{s}</td>')
    if pct >= 25:
        return (f'<td style="background:{ORANGE_LIGHT};padding:5px 10px;'
                f'border-radius:5px;text-align:center;min-width:58px;{NUM_FONT}">{s}</td>')
    return f'<td style="padding:5px 10px;text-align:center;color:#333;min-width:58px;{NUM_FONT}">{s}</td>'

def fixture_html(home, away, kickoff, hp, ap, abbr_map, show_header, nav_key=""):
    day_str  = kickoff.strftime('%a')
    date_str = kickoff.strftime('%d/%m')
    hg  = hp.get('g');  ag  = ap.get('g')
    hcs = hp.get('cs'); acs = ap.get('cs')
    hg_hi  = hg  is not None and ag  is not None and hg  > ag
    ag_hi  = hg  is not None and ag  is not None and ag  > hg
    hcs_hi = hcs is not None and acs is not None and hcs > acs
    acs_hi = hcs is not None and acs is not None and acs > hcs
    ha = get_abbr(home, abbr_map)
    aa = get_abbr(away, abbr_map)

    CARD_FONT  = "font-family:'Barlow Condensed',sans-serif;"
    DATE_STYLE = ("background:#444;color:#fff;font-size:13px;font-weight:700;"
                  "border-radius:6px;padding:6px 8px;text-align:center;"
                  "vertical-align:middle;width:48px;line-height:1.4;"
                  "letter-spacing:0.3px;" + CARD_FONT)
    HDR_CELL   = ("background:#444;color:#fff;text-align:center;padding:4px 10px;"
                  "border-radius:4px;font-size:10px;font-weight:700;"
                  "text-transform:uppercase;letter-spacing:0.8px;" + CARD_FONT)

    hdr = ""
    if show_header:
        hdr = (
            '<tr>'
            '<td style="width:48px;"></td><td></td>'
            f'<td style="{HDR_CELL}">Goals</td>'
            f'<td style="{HDR_CELL}">CS%</td>'
            '</tr>'
        )

    href = f'?navigate={urllib.parse.quote(nav_key)}' if nav_key else '#'
    return f"""
<a href="{href}" style="text-decoration:none;color:inherit;display:block;margin:4px 0 0 0;">
<div style="border:1px solid #ddd;border-radius:10px;padding:10px 14px;
            background:#fff;{CARD_FONT}cursor:pointer;transition:border-color 0.15s,box-shadow 0.15s;"
     onmouseover="this.style.borderColor='#999';this.style.boxShadow='0 3px 10px rgba(0,0,0,0.1)'"
     onmouseout="this.style.borderColor='#ddd';this.style.boxShadow='none'">
  <table style="width:100%;border-collapse:collapse;">{hdr}
    <tr>
      <td rowspan="2" style="{DATE_STYLE}">{day_str}<br/>{date_str}</td>
      <td style="font-size:20px;font-weight:800;padding:3px 10px;letter-spacing:0.5px;">{ha}</td>
      {goals_td(hg, hg_hi)}{cs_td(hcs, hcs_hi)}
    </tr>
    <tr>
      <td style="font-size:20px;font-weight:800;padding:3px 10px;letter-spacing:0.5px;">{aa}</td>
      {goals_td(ag, ag_hi)}{cs_td(acs, acs_hi)}
    </tr>
  </table>
</div>
</a>"""

# ── Main UI ───────────────────────────────────────────────────────────────────

df_fix   = load_fixtures_df()
abbr_map = load_abbreviations()

today    = pd.Timestamp.now().normalize()
upcoming = df_fix[(df_fix['Status'] == 'Upcoming') & (df_fix['Kickoff_Date'] >= today)].copy()
avail_gws = sorted(upcoming['GW'].dropna().unique().astype(int))

if not avail_gws:
    st.warning("No upcoming fixtures found.")
    st.stop()

col_title, col_live, col_analytics, col_upload = st.columns([4, 1, 1, 1])
with col_title:
    st.title("⚽ FPL Fixtures")
with col_live:
    st.markdown("<br>", unsafe_allow_html=True)
    if st.button("🔴 Live GW", use_container_width=True, type="primary"):
        st.switch_page("pages/live_gw.py")
with col_analytics:
    st.markdown("<br>", unsafe_allow_html=True)
    if st.button("📊 Analytics", use_container_width=True):
        st.switch_page("pages/analytics.py")
with col_upload:
    st.markdown("<br>", unsafe_allow_html=True)
    if st.button("📤 Upload", use_container_width=True):
        st.switch_page("pages/upload_solio.py")

selected_gw = st.selectbox(
    "Gameweek", avail_gws, index=0, format_func=lambda g: f"Gameweek {g}"
)

gw_fix = upcoming[upcoming['GW'] == selected_gw].sort_values('Kickoff_Date').reset_index(drop=True)
projs  = get_latest_projections(selected_gw)

if not projs:
    st.info(f"No projection data in DB yet for GW{selected_gw}. Run the polling task to populate.")

# Split fixtures into two columns
n   = len(gw_fix)
mid = math.ceil(n / 2)

col_l, col_r = st.columns(2)

for col, grp in [(col_l, gw_fix.iloc[:mid]), (col_r, gw_fix.iloc[mid:])]:
    with col:
        for i, (_, row) in enumerate(grp.iterrows()):
            home, away = row['Home'], row['Away']
            html = fixture_html(
                home, away, row['Kickoff_Date'],
                projs.get(home, {}), projs.get(away, {}),
                abbr_map, show_header=(i == 0),
                nav_key=f"{home}|{away}|{selected_gw}"
            )
            st.markdown(html, unsafe_allow_html=True)
            st.markdown('<div style="margin-bottom:8px;"></div>', unsafe_allow_html=True)
