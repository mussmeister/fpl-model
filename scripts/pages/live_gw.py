"""
FPL Live Gameweek Tracker
Translated from AppScript iteration 52.
"""
import sys
import html as _html
import unicodedata
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from utils.auth import require_auth, show_logout_button

st.set_page_config(page_title="FPL Live GW", layout="wide", page_icon="🔴")

require_auth()
show_logout_button()

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Barlow+Condensed:wght@400;600;700;800&family=Barlow:wght@400;500;600&display=swap');
html, body, [class*="css"], .stApp { font-family: 'Barlow', sans-serif !important; }
h1, h2, h3 { font-family: 'Barlow Condensed', sans-serif !important; font-weight: 800 !important; }
.block-container { padding-left: 1rem !important; padding-right: 1rem !important; }
</style>
""", unsafe_allow_html=True)

if st.button("← Back to Fixtures"):
    st.switch_page("fpl_app.py")

st.title("🔴 Live Gameweek")

# ── FPL ID gate ───────────────────────────────────────────────────────────────

if 'fpl_id' not in st.session_state:
    st.session_state.fpl_id = None

# Pre-populate from user profile if not already set
if not st.session_state.fpl_id:
    try:
        from utils import user_db
        _email = st.session_state.get('auth_email', '')
        if _email:
            _profile = user_db.get_profile(_email)
            _stored_id = _profile.get('fpl_team_id')
            if _stored_id:
                st.session_state.fpl_id = str(_stored_id)
                st.session_state['_fpl_id_from_profile'] = True
    except Exception:
        pass

if not st.session_state.fpl_id:
    st.markdown("### Enter your FPL Manager ID")
    st.markdown("Find it in your FPL URL: `fantasy.premierleague.com/entry/`**123456**`/history`")
    col_a, col_b = st.columns([3, 1])
    with col_a:
        entered = st.text_input("FPL ID", placeholder="e.g. 1102131", label_visibility="collapsed")
    with col_b:
        go = st.button("Load", type="primary", use_container_width=True)
    if go:
        if entered.strip().isdigit():
            st.session_state.fpl_id = entered.strip()
            st.rerun()
        else:
            st.error("Please enter a valid numeric ID")
    st.stop()

fpl_id = st.session_state.fpl_id

with st.sidebar:
    if st.session_state.get('_fpl_id_from_profile'):
        try:
            from utils import user_db as _udb
            _pname = _udb.get_profile(st.session_state.get('auth_email', '')).get('fpl_team_name', '')
            st.caption(f"**{_pname or 'FPL ID'}** `{fpl_id}`")
        except Exception:
            st.markdown(f"**FPL ID:** `{fpl_id}`")
        if st.button("Change ID"):
            st.session_state.fpl_id = None
            st.session_state.pop('_fpl_id_from_profile', None)
            st.rerun()
    else:
        st.markdown(f"**FPL ID:** `{fpl_id}`")
        if st.button("Change ID"):
            st.session_state.fpl_id = None
            st.rerun()

# ── Constants ─────────────────────────────────────────────────────────────────

FPL  = "https://fantasy.premierleague.com/api"
ESPN = "https://site.api.espn.com/apis/site/v2/sports/soccer/eng.1"
CARD = "font-family:'Barlow Condensed',sans-serif;"

ESPN_TO_FPL = {
    "Arsenal": "Arsenal", "Aston Villa": "Aston Villa",
    "Bournemouth": "Bournemouth", "AFC Bournemouth": "Bournemouth",
    "Brentford": "Brentford", "Brighton & Hove Albion": "Brighton",
    "Chelsea": "Chelsea", "Crystal Palace": "Crystal Palace",
    "Everton": "Everton", "Fulham": "Fulham",
    "Ipswich Town": "Ipswich", "Leicester City": "Leicester",
    "Liverpool": "Liverpool", "Manchester City": "Man City",
    "Manchester United": "Man Utd", "Newcastle United": "Newcastle",
    "Nottingham Forest": "Nott'm Forest", "Southampton": "Southampton",
    "Tottenham Hotspur": "Spurs", "West Ham United": "West Ham",
    "Wolverhampton Wanderers": "Wolves", "Leeds United": "Leeds",
    "Burnley": "Burnley", "Sunderland": "Sunderland",
}

# ── API helpers ───────────────────────────────────────────────────────────────

def _get(url, timeout=10):
    try:
        r = requests.get(url, timeout=timeout)
        return r.json() if r.status_code == 200 else None
    except Exception:
        return None

@st.cache_data(ttl=120)
def fetch_bootstrap():
    return _get(f"{FPL}/bootstrap-static/")

@st.cache_data(ttl=30)
def fetch_picks(fid, gw):
    return _get(f"{FPL}/entry/{fid}/event/{gw}/picks/")

@st.cache_data(ttl=30)
def fetch_live(gw):
    return _get(f"{FPL}/event/{gw}/live/")

@st.cache_data(ttl=30)
def fetch_fixtures(gw):
    return _get(f"{FPL}/fixtures/?event={gw}")

@st.cache_data(ttl=60)
def fetch_espn_scoreboard(date_str):
    return _get(f"{ESPN}/scoreboard?dates={date_str}&limit=100")

@st.cache_data(ttl=60)
def fetch_espn_summary(event_id):
    return _get(f"{ESPN}/summary?event={event_id}")

@st.cache_data(ttl=120)
def fetch_player_summary(pid):
    return _get(f"{FPL}/element-summary/{pid}/")

# ── Logic helpers ─────────────────────────────────────────────────────────────

def normalize(s):
    return unicodedata.normalize("NFD", s).encode("ascii", "ignore").decode().lower()

def name_match(fpl_name, espn_name):
    if not fpl_name or not espn_name:
        return False
    f, e = normalize(fpl_name), normalize(espn_name)
    if e in f or f in e:
        return True
    parts = f.split()
    return bool(parts) and e.endswith(parts[-1])

def espn_team(name):
    if name in ESPN_TO_FPL:
        return ESPN_TO_FPL[name]
    for k, v in ESPN_TO_FPL.items():
        if name in k or k in name:
            return v
    return name

def calc_projected_bonus(players):
    """players: list of {id, bps}. Returns {id: bonus_pts}"""
    s = sorted(players, key=lambda x: x['bps'], reverse=True)
    out = {}
    if not s or s[0]['bps'] == 0:
        return out
    r1_score = s[0]['bps']
    r1 = [p for p in s if p['bps'] == r1_score]
    for p in r1: out[p['id']] = 3
    if len(r1) >= 3: return out
    idx = len(r1)
    if idx < len(s):
        r2_score = s[idx]['bps']
        if r2_score > 0:
            r2 = [p for p in s if p['bps'] == r2_score]
            pts = 2 if len(r1) == 1 else 1
            for p in r2: out[p['id']] = pts
            if len(r1) + len(r2) >= 3: return out
            idx += len(r2)
            if idx < len(s):
                r3_score = s[idx]['bps']
                if r3_score > 0:
                    for p in [x for x in s if x['bps'] == r3_score]:
                        out[p['id']] = 1
    return out

def match_events(fixture, player_map, goals_pool):
    home, away = [], []
    if not fixture.get('stats'):
        return home, away
    pool = [g.copy() for g in goals_pool]
    order = [('goals_scored', '⚽'), ('own_goals', '⚽(OG)'), ('assists', '🅰️'),
             ('red_cards', '🟥'), ('yellow_cards', '🟨')]
    for stat_id, icon in order:
        stat = next((s for s in fixture['stats'] if s['identifier'] == stat_id), None)
        if not stat: continue
        for side, bucket in [('h', home), ('a', away)]:
            for item in stat[side]:
                name = player_map.get(item['element'], {}).get('web_name', '?')
                display = name
                if stat_id in ('goals_scored', 'own_goals'):
                    mins = []
                    for _ in range(item['value']):
                        m = next((g for g in pool if not g.get('used') and name_match(name, g['name'])), None)
                        if m:
                            m['used'] = True
                            mins.append(m['minute'])
                    if mins:
                        display += f" ({', '.join(mins)})"
                bucket.append(f"{display} {icon}" if side == 'h' else f"{icon} {display}")
    return home, away

def bps_leaders_html(fixture, player_map):
    if not fixture.get('stats'): return ""
    bps_stat = next((s for s in fixture['stats'] if s['identifier'] == 'bps'), None)
    if not bps_stat: return ""
    players = [{'id': p['element'], 'bps': p['value']} for p in bps_stat['h'] + bps_stat['a']]
    bonus = calc_projected_bonus(players)
    ranked = sorted(players, key=lambda x: x['bps'], reverse=True)
    medals = {3: '🥇', 2: '🥈', 1: '🥉'}
    rows = "".join(
        f'<div>{medals.get(bonus[p["id"]],"")}&nbsp;<b>{p["bps"]}</b> – {player_map.get(p["id"],{}).get("web_name","?")}</div>'
        for p in ranked if p['id'] in bonus
    )
    return f'<div style="font-size:11px;{CARD}"><b>Bonus Points</b>{rows}</div>' if rows else ""

# ── Load data ─────────────────────────────────────────────────────────────────

with st.spinner("Loading FPL data…"):
    bootstrap = fetch_bootstrap()

if not bootstrap:
    st.error("Could not reach the FPL API. Try again in a moment.")
    st.stop()

player_map = {p['id']: p for p in bootstrap['elements']}
team_map   = {t['id']: t['name'] for t in bootstrap['teams']}
team_short = {t['id']: t['short_name'] for t in bootstrap['teams']}
pos_map    = {p['id']: p['singular_name_short'] for p in bootstrap['element_types']}

current_event = next((e for e in bootstrap['events'] if e['is_current']), None)
if not current_event:
    finished = [e for e in bootstrap['events'] if e['is_finished']]
    current_event = finished[-1] if finished else bootstrap['events'][0]
gw = current_event['id']

picks = fetch_picks(fpl_id, gw)
if not picks:
    st.error(f"Couldn't load picks for FPL ID **{fpl_id}**. Is the ID correct?")
    if st.button("Try a different ID"):
        st.session_state.fpl_id = None
        st.rerun()
    st.stop()

live_raw = fetch_live(gw)
fixtures = fetch_fixtures(gw)
if not live_raw or not fixtures:
    st.error("FPL API unavailable. Try again shortly.")
    st.stop()

live_map = {el['id']: el for el in live_raw['elements']}

# Team xG
team_xg = {}
for el in live_raw['elements']:
    p = player_map.get(el['id'])
    if p and el['stats'].get('expected_goals'):
        tid = p['team']
        team_xg[tid] = team_xg.get(tid, 0) + float(el['stats']['expected_goals'])

# Global projected bonus (only for in-progress games)
bonus_map = {}
for fix in fixtures:
    if fix.get('started') and not fix.get('finished'):
        bps_stat = next((s for s in (fix.get('stats') or []) if s['identifier'] == 'bps'), None)
        if bps_stat:
            pl = [{'id': p['element'], 'bps': p['value']} for p in bps_stat['h'] + bps_stat['a']]
            for pid, pts in calc_projected_bonus(pl).items():
                bonus_map[pid] = bonus_map.get(pid, 0) + pts

# My picks
my_picks = {}
for p in picks['picks']:
    if p['is_captain']:
        status = f"C (x{p['multiplier']})"
    elif p['is_vice_captain']:
        status = "VC"
    elif p['position'] > 11:
        status = "Bench"
    else:
        status = "-"
    my_picks[p['element']] = {'status': status, 'multiplier': p['multiplier'], 'position': p['position']}

def live_score(pid):
    e = live_map.get(pid)
    if not e: return 0
    pts = e['stats']['total_points']
    if pid in bonus_map:
        pts -= e['stats'].get('bonus', 0)
        pts += bonus_map[pid]
    return pts

live_total = sum(live_score(p['element']) * p['multiplier'] for p in picks['picks'] if p['position'] <= 11)
played     = sum(1 for p in picks['picks'] if p['position'] <= 11
                 and live_map.get(p['element'], {}).get('stats', {}).get('minutes', 0) > 0)
prev_total = picks['entry_history']['total_points'] - picks['entry_history']['points']
xfer_cost  = picks['entry_history']['event_transfers_cost']

# ── ESPN — fetch scoreboard for full GW date range ────────────────────────────

is_live   = any(f.get('started') and not f.get('finished_provisional') for f in fixtures)
clock_map = {}
poss_map  = {}
goals_pool = []

dates    = [datetime.fromisoformat(f['kickoff_time'].replace('Z', '+00:00')) for f in fixtures]
min_date = min(dates)
max_date = max(dates)
# ESPN supports single date (YYYYMMDD) or range (YYYYMMDD-YYYYMMDD)
date_str = (min_date.strftime('%Y%m%d') if min_date.date() == max_date.date()
            else f"{min_date.strftime('%Y%m%d')}-{max_date.strftime('%Y%m%d')}")
espn = fetch_espn_scoreboard(date_str)

if espn and espn.get('events'):
    for ev in espn['events']:
        state  = ev['status']['type']['state']
        detail = ev['status']['type'].get('shortDetail', '')
        clock  = ev['status'].get('displayClock', '')
        if state == 'in':
            disp = "⏸️ HT" if detail == 'HT' else (f"🔴 {clock}'" if clock else f"🔴 {detail}")
        else:
            disp = ""

        comp     = ev.get('competitions', [{}])[0]
        home_fpl = home_eid = away_eid = None

        for c in comp.get('competitors', []):
            fpl_name = espn_team(c['team']['name'])
            clock_map[fpl_name] = disp
            if c['homeAway'] == 'home':
                home_fpl = fpl_name
                home_eid = str(c['team']['id'])
            else:
                away_eid = str(c['team']['id'])

        for d in comp.get('details', []):
            if d.get('scoringPlay') and d.get('clock') and d.get('athletesInvolved'):
                goals_pool.append({
                    'name': d['athletesInvolved'][0].get('displayName', ''),
                    'minute': d['clock'].get('displayValue', ''),
                    'used': False,
                })

        if state in ('in', 'post') and home_eid and home_fpl:
            summary = fetch_espn_summary(ev['id'])
            if summary and summary.get('boxscore', {}).get('teams'):
                hp = ap = '-'
                for td in summary['boxscore']['teams']:
                    tid  = str(td.get('team', {}).get('id', ''))
                    poss = next((s['displayValue'] for s in td.get('statistics', []) if s['name'] == 'possessionPct'), None)
                    if poss:
                        if tid == home_eid: hp = poss
                        elif tid == away_eid: ap = poss
                poss_map[home_fpl] = {'home': hp, 'away': ap}

# ── Player element summaries (parallel) ───────────────────────────────────────

summary_map = {}
with ThreadPoolExecutor(max_workers=8) as ex:
    futures = {ex.submit(fetch_player_summary, pid): pid for pid in my_picks}
    for fut in as_completed(futures):
        data = fut.result()
        if data:
            summary_map[futures[fut]] = data

# Players → fixtures
players_by_fix = {f['id']: [] for f in fixtures}
for pid in my_picks:
    p_data = player_map.get(pid)
    if not p_data: continue
    for fix in fixtures:
        if fix['team_h'] == p_data['team'] or fix['team_a'] == p_data['team']:
            players_by_fix[fix['id']].append(pid)

# Sort fixtures: Live → Upcoming → Completed
def fix_order(f):
    if f.get('started') and not f.get('finished_provisional'): return 0
    if not f.get('started'): return 1
    return 2

fixtures_sorted = sorted(fixtures, key=lambda f: (fix_order(f), f['kickoff_time']))

# ── Header metrics ─────────────────────────────────────────────────────────────

c1, c2, c3, c4 = st.columns(4)
c1.metric(f"GW{gw} Points", live_total)
c2.metric("Players Played", f"{played}/11")
c3.metric("Transfer Cost", f"-{xfer_cost}" if xfer_cost else "0")
c4.metric("Projected Total", prev_total + live_total - xfer_cost)

status_bg  = "#b6d7a8" if is_live else "#f4cccc"
status_txt = "🔴 LIVE — auto-refreshing every 60s" if is_live else "💤 Not currently live"
st.markdown(
    f'<div style="background:{status_bg};padding:8px;border-radius:6px;text-align:center;'
    f'font-weight:700;{CARD}margin:8px 0;">{status_txt}</div>',
    unsafe_allow_html=True
)

# ── Fixtures ───────────────────────────────────────────────────────────────────

current_cat = None

for fix in fixtures_sorted:
    if fix.get('finished_provisional') or fix.get('finished'):
        cat = "Completed"
    elif fix.get('started'):
        cat = "Live"
    else:
        cat = "Upcoming"

    if cat != current_cat:
        icons = {"Live": "🔴", "Upcoming": "⏳", "Completed": "✅"}
        bgs   = {"Live": "#cc0000", "Upcoming": "#555", "Completed": "#38003c"}
        st.markdown(
            f'<div style="background:{bgs[cat]};color:#fff;font-weight:800;font-size:13px;'
            f'letter-spacing:1px;text-transform:uppercase;padding:6px 12px;border-radius:6px;'
            f'text-align:center;{CARD}margin:20px 0 8px;">{icons[cat]} {cat}</div>',
            unsafe_allow_html=True
        )
        current_cat = cat

    ht = fix['team_h']
    at = fix['team_a']
    home_name  = team_map[ht]
    away_name  = team_map[at]
    home_short = team_short[ht]
    away_short = team_short[at]

    kickoff     = datetime.fromisoformat(fix['kickoff_time'].replace('Z', '+00:00'))
    kickoff_str = kickoff.strftime('%a %d %b %H:%M')

    if fix.get('finished_provisional') or fix.get('finished'):
        clock_str = "FT"
    elif fix.get('started'):
        clock_str = clock_map.get(home_name, "🔴 Live")
    else:
        clock_str = kickoff.strftime('%H:%M')

    score_str = f"{fix.get('team_h_score', 0)} - {fix.get('team_a_score', 0)}" if fix.get('started') else "v"
    home_xg   = f"({team_xg.get(ht, 0):.1f})" if fix.get('started') else ""
    away_xg   = f"({team_xg.get(at, 0):.1f})" if fix.get('started') else ""

    # Possession bar
    poss     = poss_map.get(home_name, {})
    hp_str   = poss.get('home', '-')
    ap_str   = poss.get('away', '-')
    poss_html = ""
    if fix.get('started') and hp_str != '-':
        hp = int(float(hp_str))
        ap = int(float(ap_str)) if ap_str != '-' else 100 - hp
        poss_html = (
            f'<div style="display:flex;align-items:center;gap:6px;padding:3px 8px;background:#d9d9d9;">'
            f'<span style="font-size:11px;color:#1155cc;font-weight:700;width:32px;text-align:right;">{hp}%</span>'
            f'<div style="flex:1;height:7px;background:#c9daf8;border-radius:4px;overflow:hidden;">'
            f'<div style="width:{hp}%;height:100%;background:#3d85c6;float:left;border-radius:4px 0 0 4px;"></div>'
            f'</div>'
            f'<span style="font-size:11px;color:#1155cc;font-weight:700;width:32px;">{ap}%</span>'
            f'</div>'
        )

    # Events
    home_evs, away_evs = match_events(fix, player_map, goals_pool) if fix.get('started') else ([], [])
    bps_html = bps_leaders_html(fix, player_map) if fix.get('started') else ""

    events_row = ""
    if fix.get('started'):
        h_ev = "<br>".join(home_evs)
        a_ev = "<br>".join(away_evs)
        events_row = (
            f'<tr style="background:#d9d9d9;">'
            f'<td style="padding:5px 8px;font-size:12px;text-align:right;vertical-align:top;{CARD}">{h_ev}</td>'
            f'<td style="background:#fffce0;font-size:11px;padding:5px 8px;vertical-align:top;{CARD}">{bps_html}</td>'
            f'<td style="padding:5px 8px;font-size:12px;text-align:left;vertical-align:top;{CARD}">{a_ev}</td>'
            f'</tr>'
        )

    # Player rows
    player_rows_html = ""
    fix_players = players_by_fix.get(fix['id'], [])
    if fix_players:
        header = (
            f'<tr style="background:#000;color:#fff;font-size:12px;font-weight:700;text-align:center;{CARD}">'
            f'<td style="padding:5px 8px;text-align:left;">Player</td>'
            f'<td>Pos</td><td>Team</td><td>Status</td>'
            f'<td>Pts</td><td>BPS</td><td>Mins</td><td>Saves</td><td>DefCon</td>'
            f'</tr>'
        )
        rows = ""
        for pid in fix_players:
            p_data    = player_map.get(pid, {})
            live_e    = live_map.get(pid, {})
            pick_info = my_picks.get(pid, {})
            pos       = pos_map.get(p_data.get('element_type'), '')
            ms        = {'mins': 0, 'pts': 0, 'bps': 0, 'saves': 0, 'cbit': 0, 'recoveries': 0}
            pts_breakdown = []

            if fix.get('started'):
                bps_s = next((s for s in (fix.get('stats') or []) if s['identifier'] == 'bps'), None)
                if bps_s:
                    bps_e = next((p for p in bps_s['h'] + bps_s['a'] if p['element'] == pid), None)
                    if bps_e: ms['bps'] = bps_e['value']

                if live_e.get('explain'):
                    fe = next((e for e in live_e['explain'] if e['fixture'] == fix['id']), None)
                    if fe:
                        ms['pts'] = sum(s['points'] for s in fe['stats'])
                        mins_s = next((s for s in fe['stats'] if s['identifier'] == 'minutes'), None)
                        if mins_s: ms['mins'] = mins_s['value']
                        pts_breakdown = [
                            f"{s['identifier']}: {s['value']} = {s['points']}pts"
                            for s in fe['stats'] if s['points'] != 0
                        ]

                if pid in summary_map:
                    hist = summary_map[pid].get('history', [])
                    fh   = next((h for h in hist if h['fixture'] == fix['id']), None)
                    if fh:
                        ms['saves']      = fh.get('saves', 0)
                        ms['recoveries'] = fh.get('recoveries', 0)
                        cbi = fh.get('clearances_blocks_interceptions',
                              fh.get('clearances', 0) + fh.get('blocks', 0) + fh.get('interceptions', 0))
                        ms['cbit'] = cbi + fh.get('tackles', 0)

                if pid in bonus_map:
                    official_bonus = live_e.get('stats', {}).get('bonus', 0)
                    ms['pts'] -= official_bonus
                    ms['pts'] += bonus_map[pid]
                    pts_breakdown.append(f"bonus: {bonus_map[pid]}pts projected (was {official_bonus}pts)")

            pts_tooltip = _html.escape(" | ".join(pts_breakdown)) if pts_breakdown else ""

            # DefCon
            if pos == 'DEF':
                dc_score, dc_target = ms['cbit'], 10
            elif pos in ('MID', 'FWD'):
                dc_score, dc_target = ms['cbit'] + ms['recoveries'], 12
            else:
                dc_score, dc_target = None, None

            dc_display = f"{dc_score}/{dc_target}" if dc_score is not None else "-"
            dc_pct     = (dc_score / dc_target) if (dc_score is not None and dc_target) else 0
            dc_bg      = ("#b6d7a8" if dc_pct >= 1.0 else "#fff2cc" if dc_pct >= 0.8 else "inherit") if dc_score is not None else "inherit"

            saves_display = str(ms['saves']) if pos == 'GKP' else "-"
            display_pts   = ms['pts'] * pick_info.get('multiplier', 1)
            is_bench      = pick_info.get('status') == 'Bench'
            row_bg        = '#cccccc' if is_bench else '#f3f3f3'
            tip_attr      = f' title="{pts_tooltip}" style="font-weight:700;cursor:help;"' if pts_tooltip else ' style="font-weight:700;"'

            rows += (
                f'<tr style="background:{row_bg};font-size:13px;text-align:center;{CARD}">'
                f'<td style="padding:5px 8px;text-align:left;font-weight:600;">{p_data.get("web_name", "")}</td>'
                f'<td>{pos}</td>'
                f'<td>{team_short.get(p_data.get("team"), "")}</td>'
                f'<td>{pick_info.get("status", "-")}</td>'
                f'<td{tip_attr}>{display_pts}</td>'
                f'<td>{ms["bps"]}</td>'
                f'<td>{ms["mins"]}</td>'
                f'<td>{saves_display}</td>'
                f'<td style="background:{dc_bg};">{dc_display}</td>'
                f'</tr>'
            )

        # Fixed column widths so all cards align identically
        colgroup = (
            '<colgroup>'
            '<col style="width:22%"><col style="width:8%"><col style="width:9%">'
            '<col style="width:11%"><col style="width:10%"><col style="width:10%">'
            '<col style="width:10%"><col style="width:10%"><col style="width:10%">'
            '</colgroup>'
        )
        player_rows_html = (
            f'<table style="width:100%;border-collapse:collapse;table-layout:fixed;border-top:2px solid #000;">'
            f'{colgroup}{header}{rows}</table>'
        )

    # Full card — 3-column fixed layout, built as single string (no blank lines → no CommonMark HTML-block breaks)
    card_html = (
        f'<div style="border:1px solid #ccc;border-radius:10px;overflow:hidden;margin-bottom:14px;{CARD}">'
        f'<div style="background:#d9ead3;text-align:center;font-weight:700;padding:5px;font-size:12px;">{kickoff_str}</div>'
        f'{poss_html}'
        f'<table style="width:100%;border-collapse:collapse;table-layout:fixed;">'
        f'<colgroup><col style="width:43%"><col style="width:14%"><col style="width:43%"></colgroup>'
        f'<tr style="background:#d9d9d9;">'
        f'<td style="padding:7px 10px;font-size:20px;font-weight:800;text-align:right;">{home_short}</td>'
        f'<td style="background:#000;color:#fff;text-align:center;padding:6px 8px;">'
        f'<div style="font-size:10px;font-weight:600;">{clock_str}</div>'
        f'<div style="font-size:17px;font-weight:800;">{score_str}</div>'
        f'<div style="font-size:10px;color:#aaa;">{home_xg} &nbsp; {away_xg}</div>'
        f'</td>'
        f'<td style="padding:7px 10px;font-size:20px;font-weight:800;text-align:left;">{away_short}</td>'
        f'</tr>'
        f'{events_row}'
        f'</table>'
        f'{player_rows_html}'
        f'</div>'
    )
    st.markdown(card_html, unsafe_allow_html=True)

# ── Auto-refresh: only when games are active or imminent ──────────────────────

now_utc = datetime.now(timezone.utc)

# Within 5 mins before any kickoff
near_kickoff = any(
    0 <= (datetime.fromisoformat(f['kickoff_time'].replace('Z', '+00:00')) - now_utc).total_seconds() <= 300
    for f in fixtures if not f.get('started')
)
# Within ~110 mins of any finished game's kickoff (covers 90 min game + buffer)
just_finished = any(
    (now_utc - datetime.fromisoformat(f['kickoff_time'].replace('Z', '+00:00'))).total_seconds() <= 110 * 60
    for f in fixtures if f.get('finished_provisional') or f.get('finished')
)

if is_live or near_kickoff or just_finished:
    st.markdown('<script>setTimeout(function(){window.location.reload();}, 60000);</script>', unsafe_allow_html=True)
