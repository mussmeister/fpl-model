"""
FPL API data ingestion — players, teams, GW events, and per-GW stats.

Usage:
    # One-time full pull (all players + all GW history for current season):
    python scripts/fpl_api_pull.py --full

    # Daily delta (update player info + pull stats for any newly finalised GWs):
    python scripts/fpl_api_pull.py --delta

    # Force re-pull stats for a specific GW:
    python scripts/fpl_api_pull.py --delta --gw 36
"""
import sys
import time
import sqlite3
import argparse
import requests
from pathlib import Path
from datetime import datetime

ROOT    = Path(__file__).resolve().parent.parent
DB_PATH = ROOT / 'outputs' / 'projections_history.db'

FPL_BASE = "https://fantasy.premierleague.com/api"
HEADERS  = {'User-Agent': 'Mozilla/5.0'}


# ── HTTP ──────────────────────────────────────────────────────────────────────

def fetch(url, retries=3, backoff=2.0):
    for attempt in range(retries):
        try:
            r = requests.get(url, headers=HEADERS, timeout=15)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            if attempt == retries - 1:
                raise
            time.sleep(backoff * (attempt + 1))


# ── DB setup ──────────────────────────────────────────────────────────────────

def ensure_tables(conn):
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS fpl_teams (
            team_id                 INTEGER PRIMARY KEY,
            name                    TEXT,
            short_name              TEXT,
            code                    INTEGER,
            strength_overall_home   INTEGER,
            strength_overall_away   INTEGER,
            strength_attack_home    INTEGER,
            strength_attack_away    INTEGER,
            strength_defence_home   INTEGER,
            strength_defence_away   INTEGER,
            pulled_at               TEXT
        );

        CREATE TABLE IF NOT EXISTS fpl_players (
            element_id              INTEGER PRIMARY KEY,
            web_name                TEXT,
            first_name              TEXT,
            second_name             TEXT,
            team_id                 INTEGER,
            element_type            INTEGER,
            now_cost                INTEGER,
            status                  TEXT,
            news                    TEXT,
            total_points            INTEGER,
            form                    REAL,
            selected_by_percent     REAL,
            transfers_in_event      INTEGER,
            transfers_out_event     INTEGER,
            ep_next                 REAL,
            minutes                 INTEGER,
            goals_scored            INTEGER,
            assists                 INTEGER,
            clean_sheets            INTEGER,
            pulled_at               TEXT
        );

        CREATE TABLE IF NOT EXISTS fpl_gw_events (
            gw                      INTEGER PRIMARY KEY,
            name                    TEXT,
            deadline_time           TEXT,
            average_entry_score     INTEGER,
            highest_score           INTEGER,
            finished                INTEGER,
            data_checked            INTEGER,
            most_captained          INTEGER,
            most_vice_captained     INTEGER,
            pulled_at               TEXT
        );

        CREATE TABLE IF NOT EXISTS fpl_player_gw_stats (
            element_id                      INTEGER,
            gw                              INTEGER,
            minutes                         INTEGER,
            goals_scored                    INTEGER,
            assists                         INTEGER,
            clean_sheets                    INTEGER,
            goals_conceded                  INTEGER,
            own_goals                       INTEGER,
            penalties_saved                 INTEGER,
            penalties_missed                INTEGER,
            yellow_cards                    INTEGER,
            red_cards                       INTEGER,
            saves                           INTEGER,
            bonus                           INTEGER,
            bps                             INTEGER,
            total_points                    INTEGER,
            was_home                        INTEGER,
            team_h_score                    INTEGER,
            team_a_score                    INTEGER,
            value                           INTEGER,
            selected                        INTEGER,
            transfers_balance               INTEGER,
            expected_goals                  REAL,
            expected_assists                REAL,
            expected_goal_involvements      REAL,
            expected_goals_conceded         REAL,
            pulled_at                       TEXT,
            PRIMARY KEY (element_id, gw)
        );

        CREATE INDEX IF NOT EXISTS idx_pgws_gw      ON fpl_player_gw_stats (gw);
        CREATE INDEX IF NOT EXISTS idx_pgws_element ON fpl_player_gw_stats (element_id);
    """)
    conn.commit()


# ── Upsert helpers ────────────────────────────────────────────────────────────

def _now():
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')


def upsert_teams(conn, teams):
    now = _now()
    conn.executemany("""
        INSERT OR REPLACE INTO fpl_teams VALUES (
            :id, :name, :short_name, :code,
            :strength_overall_home, :strength_overall_away,
            :strength_attack_home,  :strength_attack_away,
            :strength_defence_home, :strength_defence_away,
            :pulled_at
        )
    """, [{**t, 'pulled_at': now} for t in teams])
    conn.commit()
    print(f"  teams:      {len(teams)} upserted")


def upsert_players(conn, elements):
    now = _now()
    def _f(v):
        try:
            return float(v or 0)
        except (TypeError, ValueError):
            return 0.0

    rows = [{
        'element_id':           e['id'],
        'web_name':             e['web_name'],
        'first_name':           e['first_name'],
        'second_name':          e['second_name'],
        'team_id':              e['team'],
        'element_type':         e['element_type'],
        'now_cost':             e['now_cost'],
        'status':               e.get('status', ''),
        'news':                 e.get('news', ''),
        'total_points':         e.get('total_points', 0),
        'form':                 _f(e.get('form')),
        'selected_by_percent':  _f(e.get('selected_by_percent')),
        'transfers_in_event':   e.get('transfers_in_event', 0),
        'transfers_out_event':  e.get('transfers_out_event', 0),
        'ep_next':              _f(e.get('ep_next')),
        'minutes':              e.get('minutes', 0),
        'goals_scored':         e.get('goals_scored', 0),
        'assists':              e.get('assists', 0),
        'clean_sheets':         e.get('clean_sheets', 0),
        'pulled_at':            now,
    } for e in elements]

    conn.executemany("""
        INSERT OR REPLACE INTO fpl_players VALUES (
            :element_id, :web_name, :first_name, :second_name,
            :team_id, :element_type, :now_cost, :status, :news,
            :total_points, :form, :selected_by_percent,
            :transfers_in_event, :transfers_out_event, :ep_next,
            :minutes, :goals_scored, :assists, :clean_sheets,
            :pulled_at
        )
    """, rows)
    conn.commit()
    print(f"  players:    {len(rows)} upserted")


def upsert_events(conn, events):
    now = _now()
    rows = [{
        'gw':                   e['id'],
        'name':                 e['name'],
        'deadline_time':        e['deadline_time'],
        'average_entry_score':  e.get('average_entry_score') or 0,
        'highest_score':        e.get('highest_score') or 0,
        'finished':             int(bool(e.get('finished'))),
        'data_checked':         int(bool(e.get('data_checked'))),
        'most_captained':       e.get('most_captained'),
        'most_vice_captained':  e.get('most_vice_captained'),
        'pulled_at':            now,
    } for e in events]

    conn.executemany("""
        INSERT OR REPLACE INTO fpl_gw_events VALUES (
            :gw, :name, :deadline_time, :average_entry_score,
            :highest_score, :finished, :data_checked,
            :most_captained, :most_vice_captained, :pulled_at
        )
    """, rows)
    conn.commit()
    print(f"  GW events:  {len(rows)} upserted")


def upsert_player_gw_stats(conn, element_id, history):
    now = _now()
    def _f(v):
        try:
            return float(v or 0)
        except (TypeError, ValueError):
            return 0.0

    rows = [{
        'element_id':                   element_id,
        'gw':                           h['round'],
        'minutes':                      h.get('minutes', 0),
        'goals_scored':                 h.get('goals_scored', 0),
        'assists':                      h.get('assists', 0),
        'clean_sheets':                 h.get('clean_sheets', 0),
        'goals_conceded':               h.get('goals_conceded', 0),
        'own_goals':                    h.get('own_goals', 0),
        'penalties_saved':              h.get('penalties_saved', 0),
        'penalties_missed':             h.get('penalties_missed', 0),
        'yellow_cards':                 h.get('yellow_cards', 0),
        'red_cards':                    h.get('red_cards', 0),
        'saves':                        h.get('saves', 0),
        'bonus':                        h.get('bonus', 0),
        'bps':                          h.get('bps', 0),
        'total_points':                 h.get('total_points', 0),
        'was_home':                     int(bool(h.get('was_home'))),
        'team_h_score':                 h.get('team_h_score'),
        'team_a_score':                 h.get('team_a_score'),
        'value':                        h.get('value', 0),
        'selected':                     h.get('selected', 0),
        'transfers_balance':            h.get('transfers_balance', 0),
        'expected_goals':               _f(h.get('expected_goals')),
        'expected_assists':             _f(h.get('expected_assists')),
        'expected_goal_involvements':   _f(h.get('expected_goal_involvements')),
        'expected_goals_conceded':      _f(h.get('expected_goals_conceded')),
        'pulled_at':                    now,
    } for h in history]

    if rows:
        conn.executemany("""
            INSERT OR REPLACE INTO fpl_player_gw_stats VALUES (
                :element_id, :gw, :minutes, :goals_scored, :assists,
                :clean_sheets, :goals_conceded, :own_goals, :penalties_saved,
                :penalties_missed, :yellow_cards, :red_cards, :saves, :bonus,
                :bps, :total_points, :was_home, :team_h_score, :team_a_score,
                :value, :selected, :transfers_balance, :expected_goals,
                :expected_assists, :expected_goal_involvements,
                :expected_goals_conceded, :pulled_at
            )
        """, rows)
        conn.commit()
    return len(rows)


# ── Pull modes ────────────────────────────────────────────────────────────────

def _pull_element_summaries(conn, elements, gw_filter=None):
    total = 0
    n     = len(elements)
    for i, el in enumerate(elements, 1):
        eid = el['id']
        try:
            summary = fetch(f"{FPL_BASE}/element-summary/{eid}/")
            history = summary.get('history', [])
            if gw_filter:
                history = [h for h in history if h['round'] in gw_filter]
            total += upsert_player_gw_stats(conn, eid, history)
        except Exception as e:
            print(f"  warning: element {eid} ({el.get('web_name', '?')}) failed: {e}")
        if i % 100 == 0:
            print(f"  {i}/{n} players processed — {total} GW rows so far")
        time.sleep(0.05)  # 50ms gap — ~38s total for all players, well within rate limits
    return total


def pull_full(conn):
    print("── Full pull ──────────────────────────────────────")
    print("Fetching bootstrap-static...")
    boot = fetch(f"{FPL_BASE}/bootstrap-static/")

    upsert_teams(conn, boot['teams'])
    upsert_players(conn, boot['elements'])
    upsert_events(conn, boot['events'])

    elements = boot['elements']
    print(f"\nFetching element-summary for {len(elements)} players...")
    total = _pull_element_summaries(conn, elements)
    print(f"\nDone. {total} player GW stat rows ingested.")


def pull_delta(conn, force_gw=None):
    print("── Delta pull ─────────────────────────────────────")
    print("Fetching bootstrap-static...")
    boot = fetch(f"{FPL_BASE}/bootstrap-static/")

    upsert_teams(conn, boot['teams'])
    upsert_players(conn, boot['elements'])
    upsert_events(conn, boot['events'])

    # Work out which GWs to pull stats for
    finished_gws = sorted(e['id'] for e in boot['events'] if e.get('data_checked'))

    if force_gw:
        gws_to_pull = {force_gw}
        print(f"Force-pulling GW{force_gw} stats...")
    elif finished_gws:
        cur = conn.execute("SELECT DISTINCT gw FROM fpl_player_gw_stats")
        existing_gws = {row[0] for row in cur.fetchall()}
        new_gws      = set(finished_gws) - existing_gws
        # Always re-pull the most recent finalised GW — bonus points can update post-match
        latest_gw    = finished_gws[-1]
        gws_to_pull  = new_gws | {latest_gw}
    else:
        gws_to_pull = set()

    if gws_to_pull:
        gw_list = sorted(gws_to_pull)
        print(f"Pulling player stats for GW{gw_list}...")
        total = _pull_element_summaries(conn, boot['elements'], gw_filter=set(gw_list))
        print(f"  {total} rows upserted for GW{gw_list}")
    else:
        print("  No new finalised GWs — player info updated only")

    print("Done.")


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="FPL API data ingestion")
    mode   = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument('--full',  action='store_true', help='Full initial pull')
    mode.add_argument('--delta', action='store_true', help='Daily delta update')
    parser.add_argument('--gw', type=int, default=None,
                        help='Force re-pull stats for a specific GW (use with --delta)')
    args = parser.parse_args()

    with sqlite3.connect(str(DB_PATH)) as conn:
        ensure_tables(conn)
        if args.full:
            pull_full(conn)
        else:
            pull_delta(conn, force_gw=args.gw)


if __name__ == '__main__':
    main()
