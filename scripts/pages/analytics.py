"""
FPL Analytics — player and team stats explorer, multi-season
"""
import sqlite3
import pandas as pd
import plotly.graph_objects as go
from pathlib import Path
import streamlit as st

DB_PATH = Path(__file__).resolve().parents[2] / 'outputs' / 'projections_history.db'

POS_MAP    = {1: 'GK', 2: 'DEF', 3: 'MID', 4: 'FWD'}
STATUS_MAP = {'a': '', 'd': '⚠️ Doubtful', 'i': '🔴 Injured',
              's': '🟡 Suspended', 'u': '❌ Unavailable', 'n': '—'}
CURRENT    = 'Current Season (Live)'
ALL        = 'All Seasons'

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Barlow+Condensed:wght@400;600;700;800&family=Barlow:wght@400;500;600&display=swap');
html, body, [class*="css"], .stApp, .stMarkdown, .stButton > button,
.stSelectbox label, .stSelectbox div[data-baseweb="select"] {
    font-family: 'Barlow', sans-serif !important;
}
h1, h2, h3 { font-family: 'Barlow Condensed', sans-serif !important; font-weight: 800 !important; }
.stat-card {
    background: #f8f9fa; border: 1px solid #e0e0e0; border-radius: 10px;
    padding: 14px 18px; text-align: center;
}
.stat-label { font-size: 11px; color: #888; text-transform: uppercase; letter-spacing: 0.8px; font-weight: 600; }
.stat-value { font-family: 'Barlow Condensed', sans-serif; font-size: 28px; font-weight: 800; color: #1a1a1a; }
.stat-sub   { font-size: 12px; color: #666; margin-top: 2px; }
</style>
""", unsafe_allow_html=True)

st.title("📊 Analytics")

if st.button("← Back to Fixtures"):
    st.switch_page("fpl_app.py")

# ── Data loaders ──────────────────────────────────────────────────────────────

@st.cache_data(ttl=300)
def get_available_seasons():
    try:
        with sqlite3.connect(str(DB_PATH), check_same_thread=False) as conn:
            cur = conn.execute(
                "SELECT DISTINCT season FROM vaastav_gw_stats ORDER BY season DESC"
            )
            return [row[0] for row in cur.fetchall()]
    except Exception:
        return []

@st.cache_data(ttl=300)
def load_current_players():
    try:
        with sqlite3.connect(str(DB_PATH), check_same_thread=False) as conn:
            return pd.read_sql("""
                SELECT p.element_id, p.web_name, p.first_name, p.second_name,
                       p.element_type, p.now_cost, p.status, p.news,
                       p.total_points, p.form, p.selected_by_percent, p.ep_next,
                       p.minutes, p.goals_scored, p.assists, p.clean_sheets,
                       t.name AS team_name, t.short_name AS team_short
                FROM fpl_players p
                LEFT JOIN fpl_teams t ON p.team_id = t.team_id
                ORDER BY p.total_points DESC
            """, conn)
    except Exception:
        return pd.DataFrame()

@st.cache_data(ttl=300)
def load_vaastav_players(season):
    try:
        with sqlite3.connect(str(DB_PATH), check_same_thread=False) as conn:
            return pd.read_sql("""
                SELECT name, position, team,
                       SUM(total_points) AS total_points,
                       SUM(minutes)      AS minutes,
                       SUM(goals_scored) AS goals_scored,
                       SUM(assists)      AS assists,
                       SUM(clean_sheets) AS clean_sheets,
                       SUM(bonus)        AS bonus,
                       AVG(value)        AS avg_value
                FROM vaastav_gw_stats
                WHERE season = ?
                GROUP BY name, position, team
                ORDER BY total_points DESC
            """, conn, params=(season,))
    except Exception:
        return pd.DataFrame()

@st.cache_data(ttl=300)
def load_current_player_gw(element_id):
    try:
        with sqlite3.connect(str(DB_PATH), check_same_thread=False) as conn:
            return pd.read_sql("""
                SELECT s.gw, s.minutes, s.goals_scored, s.assists, s.clean_sheets,
                       s.goals_conceded, s.bonus, s.bps, s.total_points,
                       s.was_home, s.team_h_score, s.team_a_score,
                       s.expected_goals, s.expected_assists,
                       s.expected_goal_involvements, s.expected_goals_conceded,
                       s.value
                FROM fpl_player_gw_stats s
                WHERE s.element_id = ?
                ORDER BY s.gw
            """, conn, params=(int(element_id),))
    except Exception:
        return pd.DataFrame()

@st.cache_data(ttl=300)
def load_vaastav_player_gw(name, team, season):
    try:
        with sqlite3.connect(str(DB_PATH), check_same_thread=False) as conn:
            return pd.read_sql("""
                SELECT gw, minutes, goals_scored, assists, clean_sheets,
                       goals_conceded, bonus, bps, total_points,
                       was_home, team_h_score, team_a_score,
                       expected_goals, expected_assists,
                       expected_goal_involvements, expected_goals_conceded,
                       value
                FROM vaastav_gw_stats
                WHERE season = ? AND name = ? AND team = ?
                ORDER BY gw
            """, conn, params=(season, name, team))
    except Exception:
        return pd.DataFrame()

@st.cache_data(ttl=300)
def load_player_career(web_name):
    """Season-by-season totals, combining vaastav history + current season."""
    CURRENT_SEASON = '2024-25'
    rows = []
    try:
        with sqlite3.connect(str(DB_PATH), check_same_thread=False) as conn:
            # Historical seasons from vaastav — exclude current season (covered by fpl API).
            # Use LIKE for flexible name matching (vaastav name format can differ by season).
            df_hist = pd.read_sql("""
                SELECT season, name, team, position,
                       COUNT(DISTINCT gw)  AS gws,
                       SUM(minutes)        AS minutes,
                       SUM(total_points)   AS total_points,
                       SUM(goals_scored)   AS goals_scored,
                       SUM(assists)        AS assists,
                       SUM(clean_sheets)   AS clean_sheets,
                       SUM(bonus)          AS bonus,
                       SUM(COALESCE(expected_goals, 0))  AS xg,
                       SUM(COALESCE(expected_assists, 0)) AS xa
                FROM vaastav_gw_stats
                WHERE season != :season
                  AND (
                      LOWER(name) = LOWER(:name)
                      OR LOWER(name) LIKE '%' || LOWER(:name) || '%'
                      OR LOWER(:name) LIKE '%' || LOWER(name) || '%'
                  )
                GROUP BY season, name, team, position
                ORDER BY season DESC
            """, conn, params={'season': CURRENT_SEASON, 'name': web_name})
            rows.append(df_hist)

            # Current season always from fpl API (most up to date)
            df_cur = pd.read_sql("""
                SELECT :season AS season,
                       p.web_name AS name, t.name AS team,
                       CASE p.element_type
                           WHEN 1 THEN 'GK' WHEN 2 THEN 'DEF'
                           WHEN 3 THEN 'MID' WHEN 4 THEN 'FWD'
                       END AS position,
                       COUNT(DISTINCT s.gw)  AS gws,
                       SUM(s.minutes)        AS minutes,
                       SUM(s.total_points)   AS total_points,
                       SUM(s.goals_scored)   AS goals_scored,
                       SUM(s.assists)        AS assists,
                       SUM(s.clean_sheets)   AS clean_sheets,
                       SUM(s.bonus)          AS bonus,
                       SUM(COALESCE(s.expected_goals, 0))   AS xg,
                       SUM(COALESCE(s.expected_assists, 0)) AS xa
                FROM fpl_player_gw_stats s
                JOIN fpl_players p ON s.element_id = p.element_id
                LEFT JOIN fpl_teams t ON p.team_id = t.team_id
                WHERE LOWER(p.web_name) = LOWER(:name)
                GROUP BY season, name, team, position
            """, conn, params={'season': CURRENT_SEASON, 'name': web_name})
            rows.append(df_cur)

    except Exception:
        pass

    if not rows:
        return pd.DataFrame()
    combined = pd.concat(rows, ignore_index=True)
    combined = combined[combined['total_points'] > 0]
    return combined.sort_values('season', ascending=False).reset_index(drop=True)

@st.cache_data(ttl=300)
def load_current_teams():
    try:
        with sqlite3.connect(str(DB_PATH), check_same_thread=False) as conn:
            return pd.read_sql("SELECT * FROM fpl_teams ORDER BY name", conn)
    except Exception:
        return pd.DataFrame()

@st.cache_data(ttl=300)
def load_vaastav_teams(season):
    try:
        with sqlite3.connect(str(DB_PATH), check_same_thread=False) as conn:
            cur = conn.execute(
                "SELECT DISTINCT team FROM vaastav_gw_stats WHERE season = ? ORDER BY team",
                (season,)
            )
            return [row[0] for row in cur.fetchall()]
    except Exception:
        return []

@st.cache_data(ttl=300)
def load_current_team_players(team_id):
    try:
        with sqlite3.connect(str(DB_PATH), check_same_thread=False) as conn:
            return pd.read_sql("""
                SELECT element_id, web_name, element_type, now_cost,
                       total_points, minutes, goals_scored, assists,
                       clean_sheets, form, selected_by_percent, status, news
                FROM fpl_players
                WHERE team_id = ?
                ORDER BY element_type, total_points DESC
            """, conn, params=(int(team_id),))
    except Exception:
        return pd.DataFrame()

@st.cache_data(ttl=300)
def load_vaastav_team_players(team, season):
    try:
        with sqlite3.connect(str(DB_PATH), check_same_thread=False) as conn:
            return pd.read_sql("""
                SELECT name, position, team,
                       SUM(total_points)   AS total_points,
                       SUM(minutes)        AS minutes,
                       SUM(goals_scored)   AS goals_scored,
                       SUM(assists)        AS assists,
                       SUM(clean_sheets)   AS clean_sheets,
                       SUM(bonus)          AS bonus,
                       SUM(COALESCE(expected_goals, 0))   AS xg,
                       SUM(COALESCE(expected_assists, 0)) AS xa
                FROM vaastav_gw_stats
                WHERE season = ? AND team = ?
                GROUP BY name, position
                ORDER BY position, total_points DESC
            """, conn, params=(season, team))
    except Exception:
        return pd.DataFrame()

@st.cache_data(ttl=300)
def load_vaastav_team_gw(team, season):
    try:
        with sqlite3.connect(str(DB_PATH), check_same_thread=False) as conn:
            return pd.read_sql("""
                SELECT gw, was_home, team_h_score, team_a_score
                FROM vaastav_gw_stats
                WHERE season = ? AND team = ? AND minutes > 0
                GROUP BY gw, was_home, team_h_score, team_a_score
                ORDER BY gw
            """, conn, params=(season, team))
    except Exception:
        return pd.DataFrame()

@st.cache_data(ttl=300)
def load_current_team_gw(team_id):
    try:
        with sqlite3.connect(str(DB_PATH), check_same_thread=False) as conn:
            df = pd.read_sql("""
                SELECT s.gw, s.was_home, s.team_h_score, s.team_a_score
                FROM fpl_player_gw_stats s
                JOIN fpl_players p ON s.element_id = p.element_id
                WHERE p.team_id = ? AND s.minutes > 0
                GROUP BY s.gw, s.was_home, s.team_h_score, s.team_a_score
                ORDER BY s.gw
            """, conn, params=(int(team_id),))
        return df.drop_duplicates(subset='gw')
    except Exception:
        return pd.DataFrame()

# ── UI helpers ────────────────────────────────────────────────────────────────

def stat_card(label, value, sub=""):
    sub_html = f'<div class="stat-sub">{sub}</div>' if sub else ''
    return (f'<div class="stat-card">'
            f'<div class="stat-label">{label}</div>'
            f'<div class="stat-value">{value}</div>'
            f'{sub_html}</div>')

def fmt_price(cost):
    return f"£{cost / 10:.1f}m"

def per90(val, mins):
    try:
        return f"{float(val) / float(mins) * 90:.2f}" if mins and float(mins) > 0 else "—"
    except Exception:
        return "—"

def gw_points_chart(gw_df, title="Points per Gameweek"):
    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=gw_df['gw'], y=gw_df['total_points'],
        name='GW Points', marker_color='#4472C4',
        hovertemplate='GW%{x}: <b>%{y} pts</b><extra></extra>'
    ))
    fig.add_trace(go.Scatter(
        x=gw_df['gw'], y=gw_df['total_points'].cumsum(),
        name='Cumulative', mode='lines',
        line=dict(color='#C4632A', width=2),
        yaxis='y2',
        hovertemplate='GW%{x}: <b>%{y} cumulative</b><extra></extra>'
    ))
    fig.update_layout(
        title=title,
        xaxis_title="Gameweek",
        yaxis=dict(title="GW Points"),
        yaxis2=dict(title="Cumulative", overlaying='y', side='right', showgrid=False),
        hovermode='x unified', height=350,
        template='plotly_white',
        legend=dict(x=0.01, y=0.99),
        margin=dict(t=40)
    )
    return fig

def goals_chart(team_gw_df):
    df = team_gw_df.copy()
    df['scored']   = df.apply(lambda r: r['team_h_score'] if r['was_home'] else r['team_a_score'], axis=1)
    df['conceded'] = df.apply(lambda r: r['team_a_score'] if r['was_home'] else r['team_h_score'], axis=1)
    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=df['gw'], y=df['scored'], name='Scored',
        marker_color='#16A34A',
        hovertemplate='GW%{x}: <b>%{y} scored</b><extra></extra>'
    ))
    fig.add_trace(go.Bar(
        x=df['gw'], y=-df['conceded'], name='Conceded',
        marker_color='#DC2626',
        customdata=df['conceded'],
        hovertemplate='GW%{x}: <b>%{customdata} conceded</b><extra></extra>'
    ))
    fig.update_layout(
        barmode='relative', xaxis_title='Gameweek',
        yaxis=dict(title='Goals', tickvals=list(range(-8, 9)),
                   ticktext=[str(abs(v)) for v in range(-8, 9)]),
        height=300, template='plotly_white',
        legend=dict(x=0.01, y=0.99), margin=dict(t=20)
    )
    return fig

def player_stat_cards(gw_df, season_label):
    mins  = int(gw_df['minutes'].sum())
    goals = int(gw_df['goals_scored'].sum())
    ast   = int(gw_df['assists'].sum())
    cs    = int(gw_df['clean_sheets'].sum())
    pts   = int(gw_df['total_points'].sum())
    bon   = int(gw_df['bonus'].sum())
    xg    = pd.to_numeric(gw_df['expected_goals'],  errors='coerce').sum() if 'expected_goals'  in gw_df.columns else None
    xa    = pd.to_numeric(gw_df['expected_assists'], errors='coerce').sum() if 'expected_assists' in gw_df.columns else None
    # treat as unavailable if the whole column was None (pre-xG seasons)
    if xg == 0 and 'expected_goals' in gw_df.columns and gw_df['expected_goals'].isna().all():
        xg = None

    c1, c2, c3, c4, c5, c6 = st.columns(6)
    c1.markdown(stat_card("Total Pts",    pts,                 f"{season_label}"), unsafe_allow_html=True)
    c2.markdown(stat_card("Minutes",      f"{mins:,}",         f"per 90: —"), unsafe_allow_html=True)
    c3.markdown(stat_card("Goals",        goals,               f"per 90: {per90(goals, mins)}"), unsafe_allow_html=True)
    c4.markdown(stat_card("Assists",      ast,                 f"per 90: {per90(ast, mins)}"), unsafe_allow_html=True)
    c5.markdown(stat_card("Clean Sheets", cs,                  ""), unsafe_allow_html=True)
    c6.markdown(stat_card("Bonus",        bon,                 ""), unsafe_allow_html=True)

    if xg is not None and (xg > 0 or (xa is not None and xa > 0)):
        st.markdown("<br>", unsafe_allow_html=True)
        cx1, cx2, cx3, _, _, _ = st.columns(6)
        xgi = gw_df['expected_goal_involvements'].sum() if 'expected_goal_involvements' in gw_df.columns else None
        cx1.markdown(stat_card("xG",  f"{xg:.2f}", f"Actual: {goals}"), unsafe_allow_html=True)
        cx2.markdown(stat_card("xA",  f"{xa:.2f}" if xa is not None else "—", f"Actual: {ast}"), unsafe_allow_html=True)
        cx3.markdown(stat_card("xGI", f"{xgi:.2f}" if xgi is not None else "—", ""), unsafe_allow_html=True)

def gw_tables(gw_df):
    cols = ['gw', 'minutes', 'total_points', 'goals_scored', 'assists',
            'clean_sheets', 'bonus', 'bps', 'expected_goals', 'expected_assists',
            'team_h_score', 'team_a_score']
    present = [c for c in cols if c in gw_df.columns]
    rename  = {
        'gw': 'GW', 'minutes': 'Mins', 'total_points': 'Pts',
        'goals_scored': 'G', 'assists': 'A', 'clean_sheets': 'CS',
        'bonus': 'Bonus', 'bps': 'BPS',
        'expected_goals': 'xG', 'expected_assists': 'xA',
        'team_h_score': 'H', 'team_a_score': 'A Score'
    }
    show = gw_df[present].rename(columns=rename)
    for col in ['xG', 'xA']:
        if col in show.columns:
            show[col] = pd.to_numeric(show[col], errors='coerce').round(2)

    col_l5, col_full = st.columns(2)
    with col_l5:
        st.markdown("**Last 5 Gameweeks**")
        st.dataframe(show.tail(5).sort_values('GW', ascending=False),
                     use_container_width=True, hide_index=True)
    with col_full:
        st.markdown("**Full Season**")
        st.dataframe(show.sort_values('GW', ascending=False),
                     use_container_width=True, hide_index=True)

# ── Season selector (shared) ──────────────────────────────────────────────────

hist_seasons   = get_available_seasons()
season_options = [CURRENT] + hist_seasons + ([ALL] if hist_seasons else [])

selected_season = st.selectbox("Season", season_options, index=0, key="season_top")
st.markdown("---")

# ══ Tabs ══════════════════════════════════════════════════════════════════════

tab_player, tab_team = st.tabs(["👤 Player Search", "🏟️ Team View"])

# ══ Player Search ═════════════════════════════════════════════════════════════

with tab_player:

    # ── All Seasons career view ────────────────────────────────────────────
    if selected_season == ALL:
        st.subheader("Career View")
        if not hist_seasons:
            st.info("No historical data yet. Run `python scripts/ingest_vaastav.py` on the server.")
            st.stop()

        current_df = load_current_players()
        if not current_df.empty:
            current_df['label'] = (current_df['web_name'] + '  ·  '
                                   + current_df['team_short'].fillna('') + '  ·  '
                                   + current_df['element_type'].map(POS_MAP))
        else:
            current_df['label'] = current_df['web_name']

        selected_label = st.selectbox("Search player", current_df['label'].tolist(),
                                      key="career_player_select",
                                      help="Career view matches by web name — accuracy improves when name hasn't changed across seasons")
        p = current_df[current_df['label'] == selected_label].iloc[0]

        career = load_player_career(p['web_name'])

        if career.empty:
            st.info(f"No multi-season data found for {p['web_name']}.")
        else:
            st.markdown(f"### {p['web_name']} — Career Summary")

            # Career totals
            t_pts  = int(career['total_points'].sum())
            t_mins = int(career['minutes'].sum())
            t_g    = int(career['goals_scored'].sum())
            t_a    = int(career['assists'].sum())
            t_cs   = int(career['clean_sheets'].sum())
            t_xg   = career['xg'].sum()
            t_xa   = career['xa'].sum()

            cc1, cc2, cc3, cc4, cc5, cc6 = st.columns(6)
            cc1.markdown(stat_card("Career Pts",    t_pts,         f"{len(career)} seasons"), unsafe_allow_html=True)
            cc2.markdown(stat_card("Career Mins",   f"{t_mins:,}", ""), unsafe_allow_html=True)
            cc3.markdown(stat_card("Goals",         t_g,           f"per 90: {per90(t_g, t_mins)}"), unsafe_allow_html=True)
            cc4.markdown(stat_card("Assists",       t_a,           f"per 90: {per90(t_a, t_mins)}"), unsafe_allow_html=True)
            cc5.markdown(stat_card("Clean Sheets",  t_cs,          ""), unsafe_allow_html=True)
            cc6.markdown(stat_card("Career xGI",    f"{(t_xg + t_xa):.1f}", ""), unsafe_allow_html=True)

            st.markdown("<br>", unsafe_allow_html=True)

            # Season-by-season bar chart
            fig_career = go.Figure()
            fig_career.add_trace(go.Bar(
                x=career['season'], y=career['total_points'],
                marker_color='#4472C4', name='Season Pts',
                hovertemplate='%{x}: <b>%{y} pts</b><extra></extra>'
            ))
            fig_career.add_trace(go.Scatter(
                x=career['season'], y=career['goals_scored'],
                mode='lines+markers', name='Goals',
                line=dict(color='#16A34A', width=2), yaxis='y2'
            ))
            fig_career.add_trace(go.Scatter(
                x=career['season'], y=career['assists'],
                mode='lines+markers', name='Assists',
                line=dict(color='#C4632A', width=2, dash='dot'), yaxis='y2'
            ))
            fig_career.update_layout(
                title=f"{p['web_name']} — Season-by-Season",
                xaxis_title="Season",
                yaxis=dict(title="Total Points"),
                yaxis2=dict(title="Goals / Assists", overlaying='y', side='right', showgrid=False),
                height=380, template='plotly_white',
                legend=dict(x=0.01, y=0.99), margin=dict(t=40)
            )
            st.plotly_chart(fig_career, use_container_width=True)

            # Season table
            st.subheader("Season Breakdown")
            career_show = career[['season', 'team', 'position', 'gws', 'minutes',
                                  'total_points', 'goals_scored', 'assists',
                                  'clean_sheets', 'bonus', 'xg', 'xa']].copy()
            career_show['xg'] = career_show['xg'].round(2)
            career_show['xa'] = career_show['xa'].round(2)
            career_show = career_show.rename(columns={
                'season': 'Season', 'team': 'Team', 'position': 'Pos',
                'gws': 'GWs', 'minutes': 'Mins', 'total_points': 'Pts',
                'goals_scored': 'G', 'assists': 'A', 'clean_sheets': 'CS',
                'bonus': 'Bonus', 'xg': 'xG', 'xa': 'xA'
            })
            st.dataframe(career_show, use_container_width=True, hide_index=True)

    # ── Current season view ────────────────────────────────────────────────
    elif selected_season == CURRENT:
        players_df = load_current_players()
        if players_df.empty:
            st.warning("No current season data. Run `python scripts/fpl_api_pull.py --full` on the server.")
            st.stop()

        players_df['pos']   = players_df['element_type'].map(POS_MAP)
        players_df['label'] = (players_df['web_name'] + '  ·  '
                               + players_df['team_short'].fillna('') + '  ·  '
                               + players_df['pos'] + '  ·  '
                               + players_df['now_cost'].apply(fmt_price))

        selected_label = st.selectbox("Search player", players_df['label'].tolist(),
                                      key="current_player_select")
        p = players_df[players_df['label'] == selected_label].iloc[0]

        st.markdown("---")
        col_name, col_flag = st.columns([5, 1])
        with col_name:
            st.markdown(f"### {p['web_name']}")
            st.caption(f"{p['first_name']} {p['second_name']}  ·  {p['team_name']}  ·  {p['pos']}")
        with col_flag:
            status_str = STATUS_MAP.get(p['status'], '')
            if status_str:
                st.markdown(f"<br><b>{status_str}</b>", unsafe_allow_html=True)

        if p['news'] and p['status'] != 'a':
            st.info(f"{STATUS_MAP.get(p['status'], '')} — {p['news']}")

        price_sub = f"Owned: {float(p['selected_by_percent'] or 0):.1f}%"
        c1, c2, c3, c4, c5, c6 = st.columns(6)
        c1.markdown(stat_card("Total Pts",    int(p['total_points'] or 0), f"Form: {float(p['form'] or 0):.1f}"), unsafe_allow_html=True)
        c2.markdown(stat_card("Price",        fmt_price(p['now_cost']),   price_sub), unsafe_allow_html=True)
        c3.markdown(stat_card("Minutes",      f"{int(p['minutes'] or 0):,}", f"EP Next: {float(p['ep_next'] or 0):.1f}"), unsafe_allow_html=True)
        c4.markdown(stat_card("Goals",        int(p['goals_scored'] or 0), f"per 90: {per90(p['goals_scored'], p['minutes'])}"), unsafe_allow_html=True)
        c5.markdown(stat_card("Assists",      int(p['assists'] or 0),      f"per 90: {per90(p['assists'], p['minutes'])}"), unsafe_allow_html=True)
        c6.markdown(stat_card("Clean Sheets", int(p['clean_sheets'] or 0), ""), unsafe_allow_html=True)

        st.markdown("<br>", unsafe_allow_html=True)
        gw_df = load_current_player_gw(int(p['element_id']))

        if gw_df.empty:
            st.info("No GW data yet.")
        else:
            player_stat_cards(gw_df, CURRENT)
            st.markdown("<br>", unsafe_allow_html=True)
            st.plotly_chart(gw_points_chart(gw_df), use_container_width=True)

            if gw_df['expected_goals'].sum() > 0 or gw_df['expected_assists'].sum() > 0:
                fig2 = go.Figure()
                fig2.add_trace(go.Bar(x=gw_df['gw'], y=gw_df['expected_goals'],
                                      name='xG', marker_color='#2563EB'))
                fig2.add_trace(go.Bar(x=gw_df['gw'], y=gw_df['expected_assists'],
                                      name='xA', marker_color='#16A34A'))
                fig2.update_layout(barmode='stack', xaxis_title="Gameweek",
                                   yaxis_title="xG / xA", height=260,
                                   template='plotly_white', margin=dict(t=20))
                st.plotly_chart(fig2, use_container_width=True)

            gw_tables(gw_df)

    # ── Historical season view ─────────────────────────────────────────────
    else:
        season = selected_season
        hist_players = load_vaastav_players(season)

        if hist_players.empty:
            st.info(f"No data for {season}. Run `python scripts/ingest_vaastav.py --seasons {season}` on the server.")
            st.stop()

        hist_players['label'] = (hist_players['name'] + '  ·  '
                                 + hist_players['team'] + '  ·  '
                                 + hist_players['position'])

        selected_label = st.selectbox("Search player", hist_players['label'].tolist(),
                                      key="hist_player_select")
        hp = hist_players[hist_players['label'] == selected_label].iloc[0]

        st.markdown("---")
        st.markdown(f"### {hp['name']}  <span style='font-size:14px;color:#888;font-weight:400'>{season}</span>", unsafe_allow_html=True)
        st.caption(f"{hp['team']}  ·  {hp['position']}")

        gw_df = load_vaastav_player_gw(hp['name'], hp['team'], season)

        if gw_df.empty:
            st.info("No GW breakdown available.")
        else:
            player_stat_cards(gw_df, season)
            st.markdown("<br>", unsafe_allow_html=True)
            st.plotly_chart(gw_points_chart(gw_df, f"{hp['name']} — {season}"),
                            use_container_width=True)

            if 'expected_goals' in gw_df.columns and gw_df['expected_goals'].sum() > 0:
                fig2 = go.Figure()
                fig2.add_trace(go.Bar(x=gw_df['gw'], y=gw_df['expected_goals'],
                                      name='xG', marker_color='#2563EB'))
                fig2.add_trace(go.Bar(x=gw_df['gw'], y=gw_df['expected_assists'],
                                      name='xA', marker_color='#16A34A'))
                fig2.update_layout(barmode='stack', xaxis_title="Gameweek",
                                   yaxis_title="xG / xA", height=260,
                                   template='plotly_white', margin=dict(t=20))
                st.plotly_chart(fig2, use_container_width=True)

            gw_tables(gw_df)

# ══ Team View ═════════════════════════════════════════════════════════════════

with tab_team:

    if selected_season == CURRENT:
        teams_df = load_current_teams()
        if teams_df.empty:
            st.info("No team data found.")
            st.stop()

        team_options = teams_df.set_index('team_id')['name'].to_dict()
        sel_team_id  = st.selectbox("Select team", list(team_options.keys()),
                                    format_func=lambda x: team_options[x], key="team_cur")
        team_row  = teams_df[teams_df['team_id'] == sel_team_id].iloc[0]

        st.markdown("---")
        st.markdown(f"### {team_row['name']}")

        cs1, cs2, cs3 = st.columns(3)
        cs1.markdown(stat_card("Attack (H/A)",
            f"{team_row['strength_attack_home']} / {team_row['strength_attack_away']}"), unsafe_allow_html=True)
        cs2.markdown(stat_card("Defence (H/A)",
            f"{team_row['strength_defence_home']} / {team_row['strength_defence_away']}"), unsafe_allow_html=True)
        cs3.markdown(stat_card("Overall (H/A)",
            f"{team_row['strength_overall_home']} / {team_row['strength_overall_away']}"), unsafe_allow_html=True)

        squad_df = load_current_team_players(sel_team_id)
        if not squad_df.empty:
            st.markdown("<br>", unsafe_allow_html=True)
            st.subheader("Squad")
            squad_df['Position'] = squad_df['element_type'].map(POS_MAP)
            squad_df['Price']    = squad_df['now_cost'].apply(fmt_price)
            squad_df['Owned%']   = squad_df['selected_by_percent'].round(1).astype(str) + '%'
            squad_df['Form']     = squad_df['form'].round(1)
            squad_df['Status']   = squad_df['status'].map(STATUS_MAP).fillna('')
            display = squad_df[['web_name', 'Position', 'Price', 'total_points',
                                 'minutes', 'goals_scored', 'assists', 'clean_sheets',
                                 'Form', 'Owned%', 'Status']].rename(columns={
                'web_name': 'Player', 'total_points': 'Pts', 'minutes': 'Mins',
                'goals_scored': 'G', 'assists': 'A', 'clean_sheets': 'CS'
            })
            pos_order = {'GK': 0, 'DEF': 1, 'MID': 2, 'FWD': 3}
            display = display.sort_values(['Position', 'Pts'],
                                          key=lambda s: s.map(pos_order) if s.name == 'Position' else s,
                                          ascending=[True, False])
            st.dataframe(display, use_container_width=True, hide_index=True)

        team_gw = load_current_team_gw(sel_team_id)
        if not team_gw.empty:
            st.subheader("Goals Scored / Conceded per GW")
            st.plotly_chart(goals_chart(team_gw), use_container_width=True)

    else:
        season       = selected_season if selected_season != ALL else (hist_seasons[0] if hist_seasons else None)
        if not season:
            st.info("No historical data available.")
            st.stop()

        vaastav_teams = load_vaastav_teams(season)
        if not vaastav_teams:
            st.info(f"No team data for {season}.")
            st.stop()

        sel_team = st.selectbox("Select team", vaastav_teams, key="team_hist")

        st.markdown("---")
        st.markdown(f"### {sel_team}  <span style='font-size:14px;color:#888;font-weight:400'>{season}</span>", unsafe_allow_html=True)

        squad_df = load_vaastav_team_players(sel_team, season)
        if not squad_df.empty:
            st.subheader("Squad")
            squad_df['xg'] = squad_df['xg'].round(2)
            squad_df['xa'] = squad_df['xa'].round(2)
            display = squad_df.rename(columns={
                'name': 'Player', 'position': 'Pos', 'team': 'Team',
                'total_points': 'Pts', 'minutes': 'Mins',
                'goals_scored': 'G', 'assists': 'A', 'clean_sheets': 'CS',
                'bonus': 'Bonus', 'xg': 'xG', 'xa': 'xA'
            })
            pos_order = {'GK': 0, 'DEF': 1, 'MID': 2, 'FWD': 3}
            display = display.sort_values(['Pos', 'Pts'],
                                          key=lambda s: s.map(pos_order) if s.name == 'Pos' else s,
                                          ascending=[True, False])
            st.dataframe(display[['Player', 'Pos', 'Pts', 'Mins', 'G', 'A', 'CS', 'Bonus', 'xG', 'xA']],
                         use_container_width=True, hide_index=True)

        team_gw = load_vaastav_team_gw(sel_team, season)
        if not team_gw.empty:
            st.subheader("Goals Scored / Conceded per GW")
            st.plotly_chart(goals_chart(team_gw), use_container_width=True)

st.markdown("---")
st.caption("Current season: FPL API (updated daily). Historical seasons: vaastav/Fantasy-Premier-League.")
