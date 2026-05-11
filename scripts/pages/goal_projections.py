"""
Player Projected Goals page
Uses Solio player-level goal projections from player_projection_snapshots.
SHARE = player prGOALS / DC model team projected goals (from projections_fixtures).
"""
import sys
import sqlite3
import numpy as np
import pandas as pd
from pathlib import Path
import streamlit as st

ROOT    = Path(__file__).resolve().parents[2]
DB_PATH = ROOT / 'outputs' / 'projections_history.db'

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from utils.auth import require_auth, show_logout_button

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

# ── Data loaders ──────────────────────────────────────────────────────────────

@st.cache_data(ttl=3600)
def load_abbr_map():
    try:
        with sqlite3.connect(str(DB_PATH)) as conn:
            cur = conn.execute(
                "SELECT team, abbr FROM solio_fixture_snapshots "
                "WHERE ingested_at = (SELECT MAX(ingested_at) FROM solio_fixture_snapshots) "
                "AND abbr IS NOT NULL AND abbr != ''"
            )
            return {row[0]: row[1] for row in cur.fetchall()}
    except Exception:
        return {}

@st.cache_data(ttl=600)
def load_available_gws():
    with sqlite3.connect(str(DB_PATH)) as conn:
        df = pd.read_sql(
            "SELECT DISTINCT pf.gw FROM projections_fixtures pf "
            "INNER JOIN fpl_gw_events e ON pf.gw = e.gw WHERE e.finished = 0 "
            "ORDER BY pf.gw", conn)
    return df['gw'].tolist()

@st.cache_data(ttl=600)
def load_data(gw):
    with sqlite3.connect(str(DB_PATH)) as conn:
        # Team fixture projections — latest poll for this GW
        fix_df = pd.read_sql("""
            SELECT f.home_team, f.away_team, f.home_g, f.away_g
            FROM projections_fixtures f
            INNER JOIN (
                SELECT gw, home_team, away_team, MAX(timestamp) AS max_ts
                FROM projections_fixtures WHERE gw=? GROUP BY home_team, away_team
            ) l ON f.gw=l.gw AND f.home_team=l.home_team
                  AND f.away_team=l.away_team AND f.timestamp=l.max_ts
        """, conn, params=(gw,))

        # Solio player projections (player_id = FPL element_id, confirmed 1:1)
        solio_df = pd.read_sql("""
            SELECT s.player_id AS fpl_id, s.xmins, s.goals AS pr_goals
            FROM player_projection_snapshots s
            INNER JOIN (
                SELECT player_id, gw, MAX(ingested_at) AS max_ia
                FROM player_projection_snapshots WHERE gw=? AND source='solio'
                GROUP BY player_id
            ) l ON s.player_id=l.player_id AND s.gw=l.gw AND s.ingested_at=l.max_ia
            WHERE s.goals > 0 AND s.xmins > 0
        """, conn, params=(gw,))

        # Season minutes — used only for the min-season-mins display filter
        xg_df = pd.read_sql("""
            SELECT element_id, SUM(minutes) AS season_mins
            FROM fpl_player_gw_stats
            GROUP BY element_id
        """, conn)

        # FPL player details + FPL team name (canonical, matches projections_fixtures)
        fp_df = pd.read_sql("""
            SELECT p.element_id, p.web_name, p.element_type,
                   p.now_cost, p.selected_by_percent,
                   t.short_name AS team_short,
                   t.name AS fpl_team
            FROM fpl_players p
            LEFT JOIN fpl_teams t ON p.team_id = t.team_id
        """, conn)

    return fix_df, solio_df, xg_df, fp_df


def build_table(fix_df, solio_df, xg_df, fp_df, abbr_map):
    # ── Team G totals, fixture labels, fixture counts ─────────────────────────
    team_g    = {}
    team_fix  = {}
    team_nfix = {}

    for _, r in fix_df.iterrows():
        ht, at = r['home_team'], r['away_team']
        at_abbr = abbr_map.get(at, at[:3].upper())
        ht_abbr = abbr_map.get(ht, ht[:3].upper())

        team_g[ht]    = team_g.get(ht, 0.0)  + r['home_g']
        team_g[at]    = team_g.get(at, 0.0)  + r['away_g']
        team_nfix[ht] = team_nfix.get(ht, 0) + 1
        team_nfix[at] = team_nfix.get(at, 0) + 1

        team_fix[ht]  = (team_fix.get(ht, '') + ('+' if ht in team_fix else '') + f"{at_abbr}(H)")
        team_fix[at]  = (team_fix.get(at, '') + ('+' if at in team_fix else '') + f"{ht_abbr}(A)")

    # ── Merge Solio projections with FPL player details ───────────────────────
    # Use fpl_team (from fpl_teams.name) for fixture matching — matches projections_fixtures
    df = (solio_df
          .merge(fp_df[['element_id', 'web_name', 'element_type',
                         'now_cost', 'selected_by_percent',
                         'team_short', 'fpl_team']],
                 left_on='fpl_id', right_on='element_id', how='inner')
          .merge(xg_df[['element_id', 'season_mins']],
                 left_on='fpl_id', right_on='element_id', how='left'))

    df['season_mins'] = df['season_mins'].fillna(0)
    df['team_g']  = df['fpl_team'].map(team_g)
    df['fixture'] = df['fpl_team'].map(team_fix)
    df['nfix']    = df['fpl_team'].map(team_nfix).fillna(1).astype(int)

    # Only players whose team has a fixture projection this GW
    df = df[df['team_g'].notna() & (df['team_g'] > 0)].copy()

    # pr_goals comes directly from Solio — no scaling needed
    df['prob_1p'] = 1.0 - np.exp(-df['pr_goals'].clip(lower=0.0))
    df['share']   = np.where(df['team_g'] > 0, df['pr_goals'] / df['team_g'], 0.0)

    return df[df['pr_goals'] > 0].sort_values('pr_goals', ascending=False).reset_index(drop=True)


# ── HTML rendering ────────────────────────────────────────────────────────────

POS_MAP    = {1: 'GK', 2: 'DEF', 3: 'MID', 4: 'FWD'}
POS_COLORS = {'GK': '#f59e0b', 'DEF': '#10b981', 'MID': '#3b82f6', 'FWD': '#ef4444'}
CF  = "font-family:'Barlow Condensed',sans-serif;"
BF  = CF + "font-weight:700;"
HDR = CF + "font-size:11px;font-weight:700;text-transform:uppercase;letter-spacing:0.8px;color:#9ca3af;"


def _goal_color(val, max_val):
    frac = min(val / max(max_val, 0.01), 1.0)
    r = int(191 + (30  - 191) * frac)
    g = int(219 + (64  - 219) * frac)
    b = int(254 + (175 - 254) * frac)
    text = '#fff' if frac > 0.35 else '#1e3a8a'
    return f"#{r:02x}{g:02x}{b:02x}", text


def _row_html(rank, row, max_g):
    pos  = POS_MAP.get(int(row.get('element_type') or 4), '?')
    pc   = POS_COLORS.get(pos, '#6b7280')
    bg, fg = _goal_color(row['pr_goals'], max_g)
    name = str(row.get('web_name', ''))
    fix  = str(row.get('fixture', ''))
    cost = f"£{row['now_cost'] / 10:.1f}m" if pd.notna(row.get('now_cost')) else ''
    sel  = f"{float(row['selected_by_percent']):.1f}%" if pd.notna(row.get('selected_by_percent')) else ''

    return f"""<tr style="border-bottom:1px solid #f3f4f6;">
  <td style="padding:5px 4px;{CF}font-size:12px;color:#d1d5db;text-align:right;">{rank}</td>
  <td style="padding:5px 8px;">
    <span style="background:{pc};color:#fff;border-radius:3px;padding:1px 5px;font-size:10px;{BF}">{pos}</span>
    <span style="{BF}font-size:15px;margin-left:5px;">{name}</span>
    <span style="{CF}font-size:11px;color:#9ca3af;margin-left:5px;">{fix}</span>
  </td>
  <td style="padding:5px 4px;{CF}font-size:12px;color:#9ca3af;text-align:right;">{cost}</td>
  <td style="padding:5px 8px;text-align:center;">
    <span style="background:{bg};color:{fg};padding:3px 12px;border-radius:5px;{BF}font-size:15px;">{row['pr_goals']:.2f}</span>
  </td>
  <td style="padding:5px 8px;text-align:center;{CF}font-size:14px;font-weight:600;">{row['prob_1p']*100:.0f}%</td>
  <td style="padding:5px 8px;text-align:center;{CF}font-size:14px;font-weight:600;">{row['share']*100:.0f}%</td>
  <td style="padding:5px 4px;{CF}font-size:12px;color:#9ca3af;text-align:right;">{sel}</td>
</tr>"""


def _table_html(df, max_g, start_rank):
    hdr = f"""<tr>
  <th style="{HDR}padding:6px 4px;text-align:right;">#</th>
  <th style="{HDR}padding:6px 8px;text-align:left;">Player</th>
  <th style="{HDR}padding:6px 4px;text-align:right;">Cost</th>
  <th style="{HDR}padding:6px 8px;text-align:center;">prGOALS</th>
  <th style="{HDR}padding:6px 8px;text-align:center;">1+</th>
  <th style="{HDR}padding:6px 8px;text-align:center;">SHARE</th>
  <th style="{HDR}padding:6px 4px;text-align:right;">Sel%</th>
</tr>"""
    rows = ''.join(
        _row_html(start_rank + i, row, max_g)
        for i, row in enumerate(df.to_dict('records'))
    )
    return f'<table style="width:100%;border-collapse:collapse;">{hdr}{rows}</table>'


# ── Main UI ───────────────────────────────────────────────────────────────────

abbr_map  = load_abbr_map()
avail_gws = load_available_gws()

if not avail_gws:
    st.warning("No upcoming GW data available.")
    st.stop()

st.title("⚽ Projected Goals")

c1, c2, c3, c4 = st.columns([1, 1, 1, 3])
with c1:
    selected_gw = st.selectbox("Gameweek", avail_gws, format_func=lambda g: f"GW{g}")
with c2:
    pos_filter = st.selectbox("Position", ["All", "FWD", "MID", "DEF", "GK"])
with c3:
    min_mins = st.selectbox(
        "Min season mins", [0, 270, 450, 900, 1350],
        index=2, format_func=lambda v: f"{v}+" if v else "All")

pos_inv = {'GK': 1, 'DEF': 2, 'MID': 3, 'FWD': 4}

tab1, tab2 = st.tabs(["📊 Ranked", "📅 Multi-GW"])

# ── Tab 1: single-GW ranked table ─────────────────────────────────────────────
with tab1:
    fix_df, solio_df, xg_df, fp_df = load_data(selected_gw)

    if fix_df.empty:
        st.warning(f"No projections available for GW{selected_gw} yet.")
    elif solio_df.empty:
        st.warning(f"No Solio player data available for GW{selected_gw} yet. Upload a Solio projection file.")
    else:
        full_df = build_table(fix_df, solio_df, xg_df, fp_df, abbr_map)

        if pos_filter != "All":
            full_df = full_df[full_df['element_type'] == pos_inv[pos_filter]]
        if min_mins > 0:
            full_df = full_df[full_df['season_mins'] >= min_mins]

        full_df = full_df.reset_index(drop=True)
        top40   = full_df.head(40)
        max_g   = float(top40['pr_goals'].max()) if len(top40) > 0 else 1.0

        st.caption(
            "Solio player projections. "
            "SHARE = player prGOALS as % of DC model team projected goals. "
            "1+ = P(≥1 goal | Poisson)."
        )

        if top40.empty:
            st.info("No players match the current filters.")
        else:
            left_df  = top40.iloc[:20]
            right_df = top40.iloc[20:]

            col_l, col_r = st.columns(2)
            with col_l:
                st.markdown(_table_html(left_df,  max_g, start_rank=1),  unsafe_allow_html=True)
            with col_r:
                if not right_df.empty:
                    st.markdown(_table_html(right_df, max_g, start_rank=21), unsafe_allow_html=True)

# ── Tab 2: multi-GW pivot table ────────────────────────────────────────────────
with tab2:
    gw_tables = {}
    for gw in avail_gws:
        try:
            fx, sol, xg, fp = load_data(gw)
            if not fx.empty and not sol.empty:
                gw_tables[gw] = build_table(fx, sol, xg, fp, abbr_map)
        except Exception:
            pass

    if not gw_tables:
        st.info("No multi-GW data available.")
    else:
        base_gws = sorted(gw_tables.keys())

        # Union of player metadata from all GWs
        all_meta = pd.concat(
            [gw_tables[gw][['fpl_id', 'web_name', 'fpl_team', 'element_type',
                             'now_cost', 'season_mins']]
             for gw in base_gws],
            ignore_index=True
        ).drop_duplicates('fpl_id', keep='first')

        # Merge per-GW prGOALS + fixture label
        pivot = all_meta.copy()
        for gw in base_gws:
            slice_df = (gw_tables[gw][['fpl_id', 'pr_goals', 'fixture']]
                        .rename(columns={'pr_goals': f'GW{gw}', 'fixture': f'fix_{gw}'}))
            pivot = pivot.merge(slice_df, on='fpl_id', how='left')

        # Apply shared filters
        if pos_filter != "All":
            pivot = pivot[pivot['element_type'] == pos_inv[pos_filter]]
        if min_mins > 0:
            pivot = pivot[pivot['season_mins'] >= min_mins]

        gw_cols = [f'GW{gw}' for gw in base_gws]
        pivot[gw_cols] = pivot[gw_cols].fillna(0)
        pivot['Total'] = pivot[gw_cols].sum(axis=1)
        pivot = pivot[pivot['Total'] > 0]

        sort_col = f'GW{selected_gw}' if f'GW{selected_gw}' in gw_cols else 'Total'
        pivot = pivot.sort_values(sort_col, ascending=False).reset_index(drop=True)

        pos_map_r = {1: 'GK', 2: 'DEF', 3: 'MID', 4: 'FWD'}
        display = pd.DataFrame({
            'Player': pivot['web_name'].fillna(pivot['fpl_team']).fillna(''),
            'Pos':    pivot['element_type'].map(pos_map_r).fillna('?'),
            'Cost':   pivot['now_cost'].apply(
                lambda v: f"£{v/10:.1f}m" if pd.notna(v) else ''),
        })
        for gw in base_gws:
            display[f'GW{gw}'] = pivot[f'GW{gw}'].round(2)
        display['Total'] = pivot['Total'].round(2)

        fmt_dict = {c: '{:.2f}' for c in gw_cols + ['Total']}
        styled = (
            display.style
            .background_gradient(subset=gw_cols, cmap='Oranges', vmin=0)
            .background_gradient(subset=['Total'], cmap='Blues', vmin=0)
            .format(fmt_dict)
        )

        st.caption(
            f"Sorted by GW{selected_gw}. Solio player projections. "
            "Position and season mins filters apply to both tabs."
        )
        st.dataframe(styled, use_container_width=True, height=600)
