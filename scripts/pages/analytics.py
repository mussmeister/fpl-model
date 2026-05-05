"""
FPL Analytics — player and team stats explorer
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
def load_players():
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
def load_teams():
    try:
        with sqlite3.connect(str(DB_PATH), check_same_thread=False) as conn:
            return pd.read_sql("SELECT * FROM fpl_teams ORDER BY name", conn)
    except Exception:
        return pd.DataFrame()

@st.cache_data(ttl=300)
def load_player_gw(element_id):
    try:
        with sqlite3.connect(str(DB_PATH), check_same_thread=False) as conn:
            df = pd.read_sql("""
                SELECT s.gw, s.minutes, s.goals_scored, s.assists, s.clean_sheets,
                       s.goals_conceded, s.bonus, s.bps, s.total_points,
                       s.was_home, s.team_h_score, s.team_a_score,
                       s.expected_goals, s.expected_assists,
                       s.expected_goal_involvements, s.expected_goals_conceded,
                       s.value,
                       e.name AS gw_name
                FROM fpl_player_gw_stats s
                LEFT JOIN fpl_gw_events e ON s.gw = e.gw
                WHERE s.element_id = ?
                ORDER BY s.gw
            """, conn, params=(element_id,))
        return df
    except Exception:
        return pd.DataFrame()

@st.cache_data(ttl=300)
def load_team_players(team_id):
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
def load_team_gw_stats(team_id):
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

# ── Helpers ───────────────────────────────────────────────────────────────────

def stat_card(label, value, sub=""):
    sub_html = f'<div class="stat-sub">{sub}</div>' if sub else ''
    return (f'<div class="stat-card">'
            f'<div class="stat-label">{label}</div>'
            f'<div class="stat-value">{value}</div>'
            f'{sub_html}</div>')

def fmt_price(cost):
    return f"£{cost / 10:.1f}m"

def per90(val, mins):
    return f"{val / mins * 90:.2f}" if mins and mins > 0 else "—"

# ── Main UI ───────────────────────────────────────────────────────────────────

players_df = load_players()
teams_df   = load_teams()

if players_df.empty:
    st.warning("No player data found. Run `python scripts/fpl_api_pull.py --full` on the server first.")
    st.stop()

tab_player, tab_team = st.tabs(["👤 Player Search", "🏟️ Team View"])

# ══ Player Search ═════════════════════════════════════════════════════════════

with tab_player:
    players_df['pos']    = players_df['element_type'].map(POS_MAP)
    players_df['label']  = (players_df['web_name'] + '  ·  '
                            + players_df['team_short'].fillna('') + '  ·  '
                            + players_df['pos'] + '  ·  '
                            + players_df['now_cost'].apply(fmt_price))

    selected_label = st.selectbox(
        "Search player", players_df['label'].tolist(),
        index=0, key="player_select",
        help="Type to search by name"
    )

    p = players_df[players_df['label'] == selected_label].iloc[0]

    st.markdown("---")

    # ── Player header ──────────────────────────────────────────────────────
    name_str   = f"{p['first_name']} {p['second_name']}"
    status_str = STATUS_MAP.get(p['status'], '')
    news_str   = f" — {p['news']}" if p['news'] and p['status'] != 'a' else ''

    col_name, col_flag = st.columns([5, 1])
    with col_name:
        st.markdown(f"### {p['web_name']}")
        st.caption(f"{name_str}  ·  {p['team_name']}  ·  {p['pos']}")
    with col_flag:
        if status_str:
            st.markdown(f"<br><b>{status_str}</b>{news_str}", unsafe_allow_html=True)

    if news_str:
        st.info(f"{status_str}{news_str}")

    # ── Season summary cards ───────────────────────────────────────────────
    mins = int(p['minutes'] or 0)
    c1, c2, c3, c4, c5, c6 = st.columns(6)
    cards = [
        (c1, "Total Pts",   int(p['total_points'] or 0),  f"Form: {float(p['form'] or 0):.1f}"),
        (c2, "Price",       fmt_price(p['now_cost']),      f"Owned: {float(p['selected_by_percent'] or 0):.1f}%"),
        (c3, "Minutes",     f"{mins:,}",                   f"EP Next: {float(p['ep_next'] or 0):.1f}"),
        (c4, "Goals",       int(p['goals_scored'] or 0),   f"per 90: {per90(p['goals_scored'], mins)}"),
        (c5, "Assists",     int(p['assists'] or 0),        f"per 90: {per90(p['assists'], mins)}"),
        (c6, "Clean Sheets",int(p['clean_sheets'] or 0),   ""),
    ]
    for col, label, val, sub in cards:
        with col:
            st.markdown(stat_card(label, val, sub), unsafe_allow_html=True)

    # ── GW history ────────────────────────────────────────────────────────
    st.markdown("<br>", unsafe_allow_html=True)
    gw_df = load_player_gw(int(p['element_id']))

    if gw_df.empty:
        st.info("No GW-by-GW data yet. Run the full pull on the server.")
    else:
        # xStats summary row
        total_xg  = gw_df['expected_goals'].sum()
        total_xa  = gw_df['expected_assists'].sum()
        total_xgi = gw_df['expected_goal_involvements'].sum()
        total_bon = gw_df['bonus'].sum()

        cx1, cx2, cx3, cx4 = st.columns(4)
        xstats = [
            (cx1, "xG (season)",  f"{total_xg:.2f}",  f"Actual: {int(p['goals_scored'] or 0)}"),
            (cx2, "xA (season)",  f"{total_xa:.2f}",  f"Actual: {int(p['assists'] or 0)}"),
            (cx3, "xGI (season)", f"{total_xgi:.2f}", ""),
            (cx4, "Bonus (total)",f"{int(total_bon)}", ""),
        ]
        for col, label, val, sub in xstats:
            with col:
                st.markdown(stat_card(label, val, sub), unsafe_allow_html=True)

        # ── Points per GW chart ───────────────────────────────────────────
        st.markdown("<br>", unsafe_allow_html=True)
        st.subheader("Points per Gameweek")

        fig = go.Figure()

        fig.add_trace(go.Bar(
            x=gw_df['gw'], y=gw_df['total_points'],
            name='Points', marker_color='#4472C4',
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
            xaxis_title="Gameweek",
            yaxis=dict(title="GW Points"),
            yaxis2=dict(title="Cumulative Points", overlaying='y', side='right', showgrid=False),
            hovermode='x unified',
            height=350,
            template='plotly_white',
            legend=dict(x=0.01, y=0.99),
            margin=dict(t=20),
        )
        st.plotly_chart(fig, use_container_width=True)

        # ── xG/xA chart ───────────────────────────────────────────────────
        if total_xg > 0 or total_xa > 0:
            st.subheader("Expected Stats per Gameweek")
            fig2 = go.Figure()
            fig2.add_trace(go.Bar(
                x=gw_df['gw'], y=gw_df['expected_goals'],
                name='xG', marker_color='#2563EB',
                hovertemplate='GW%{x} xG: <b>%{y:.2f}</b><extra></extra>'
            ))
            fig2.add_trace(go.Bar(
                x=gw_df['gw'], y=gw_df['expected_assists'],
                name='xA', marker_color='#16A34A',
                hovertemplate='GW%{x} xA: <b>%{y:.2f}</b><extra></extra>'
            ))
            fig2.update_layout(
                barmode='stack', xaxis_title="Gameweek",
                yaxis_title="xG / xA", height=280,
                template='plotly_white', margin=dict(t=20),
                legend=dict(x=0.01, y=0.99)
            )
            st.plotly_chart(fig2, use_container_width=True)

        # ── Last 5 GWs + full table ───────────────────────────────────────
        display_cols = {
            'gw': 'GW', 'minutes': 'Mins', 'total_points': 'Pts',
            'goals_scored': 'G', 'assists': 'A', 'clean_sheets': 'CS',
            'bonus': 'Bonus', 'bps': 'BPS',
            'expected_goals': 'xG', 'expected_assists': 'xA',
            'team_h_score': 'H', 'team_a_score': 'A Score',
        }
        show_df = gw_df[list(display_cols.keys())].rename(columns=display_cols)
        show_df['xG']    = show_df['xG'].round(2)
        show_df['xA']    = show_df['xA'].round(2)

        col_l5, col_full = st.columns(2)
        with col_l5:
            st.markdown("**Last 5 Gameweeks**")
            st.dataframe(show_df.tail(5).sort_values('GW', ascending=False),
                         use_container_width=True, hide_index=True)
        with col_full:
            st.markdown("**Full Season**")
            st.dataframe(show_df.sort_values('GW', ascending=False),
                         use_container_width=True, hide_index=True)

# ══ Team View ═════════════════════════════════════════════════════════════════

with tab_team:
    if teams_df.empty:
        st.info("No team data found.")
    else:
        team_options = teams_df.set_index('team_id')['name'].to_dict()
        selected_team_id = st.selectbox(
            "Select team", list(team_options.keys()),
            format_func=lambda x: team_options[x],
            key="team_select"
        )

        team_row  = teams_df[teams_df['team_id'] == selected_team_id].iloc[0]
        squad_df  = load_team_players(selected_team_id)
        team_gw   = load_team_gw_stats(selected_team_id)

        st.markdown("---")
        st.markdown(f"### {team_row['name']}")

        # Strength cards
        cs1, cs2, cs3 = st.columns(3)
        with cs1:
            st.markdown(stat_card("Attack (H/A)",
                f"{team_row['strength_attack_home']} / {team_row['strength_attack_away']}"), unsafe_allow_html=True)
        with cs2:
            st.markdown(stat_card("Defence (H/A)",
                f"{team_row['strength_defence_home']} / {team_row['strength_defence_away']}"), unsafe_allow_html=True)
        with cs3:
            st.markdown(stat_card("Overall (H/A)",
                f"{team_row['strength_overall_home']} / {team_row['strength_overall_away']}"), unsafe_allow_html=True)

        # ── Squad table ───────────────────────────────────────────────────
        st.markdown("<br>", unsafe_allow_html=True)
        st.subheader("Squad")

        if not squad_df.empty:
            squad_df['Position'] = squad_df['element_type'].map(POS_MAP)
            squad_df['Price']    = squad_df['now_cost'].apply(fmt_price)
            squad_df['Owned%']   = squad_df['selected_by_percent'].round(1).astype(str) + '%'
            squad_df['Form']     = squad_df['form'].round(1)
            squad_df['Status']   = squad_df['status'].map(STATUS_MAP).fillna('')

            display = squad_df[[
                'web_name', 'Position', 'Price', 'total_points', 'minutes',
                'goals_scored', 'assists', 'clean_sheets', 'Form', 'Owned%', 'Status'
            ]].rename(columns={
                'web_name': 'Player', 'total_points': 'Pts', 'minutes': 'Mins',
                'goals_scored': 'G', 'assists': 'A', 'clean_sheets': 'CS'
            })

            pos_order = {'GK': 0, 'DEF': 1, 'MID': 2, 'FWD': 3}
            display = display.sort_values(['Position', 'Pts'],
                                          key=lambda s: s.map(pos_order) if s.name == 'Position' else s,
                                          ascending=[True, False])

            st.dataframe(display, use_container_width=True, hide_index=True)

        # ── Goals for/against chart ───────────────────────────────────────
        if not team_gw.empty:
            st.subheader("Goals Scored / Conceded per GW")

            team_gw = team_gw.copy()
            team_gw['scored']   = team_gw.apply(
                lambda r: r['team_h_score'] if r['was_home'] else r['team_a_score'], axis=1)
            team_gw['conceded'] = team_gw.apply(
                lambda r: r['team_a_score'] if r['was_home'] else r['team_h_score'], axis=1)

            fig3 = go.Figure()
            fig3.add_trace(go.Bar(
                x=team_gw['gw'], y=team_gw['scored'],
                name='Scored', marker_color='#16A34A',
                hovertemplate='GW%{x}: <b>%{y} scored</b><extra></extra>'
            ))
            fig3.add_trace(go.Bar(
                x=team_gw['gw'], y=-team_gw['conceded'],
                name='Conceded', marker_color='#DC2626',
                hovertemplate='GW%{x}: <b>%{customdata} conceded</b><extra></extra>',
                customdata=team_gw['conceded']
            ))
            fig3.update_layout(
                barmode='relative',
                xaxis_title='Gameweek', yaxis_title='Goals',
                height=320, template='plotly_white',
                yaxis=dict(tickformat=',d',
                           tickvals=list(range(-6, 7)),
                           ticktext=[str(abs(v)) for v in range(-6, 7)]),
                legend=dict(x=0.01, y=0.99),
                margin=dict(t=20)
            )
            st.plotly_chart(fig3, use_container_width=True)

st.markdown("---")
st.caption("Data sourced from the FPL API. Updated daily.")
