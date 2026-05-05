"""
Ingest historical FPL data from vaastav/Fantasy-Premier-League (GitHub).

Usage:
    # Ingest last 3 seasons (default):
    python scripts/ingest_vaastav.py

    # Specific seasons:
    python scripts/ingest_vaastav.py --seasons 2022-23 2023-24 2024-25

    # All available seasons (2016-17 onwards):
    python scripts/ingest_vaastav.py --all

    # Force re-ingest a season (wipes existing rows first):
    python scripts/ingest_vaastav.py --seasons 2024-25 --force
"""
import sys
import time
import sqlite3
import argparse
import requests
import pandas as pd
from io import StringIO
from pathlib import Path
from datetime import datetime

ROOT    = Path(__file__).resolve().parent.parent
DB_PATH = ROOT / 'outputs' / 'projections_history.db'

BASE_URL = "https://raw.githubusercontent.com/vaastav/Fantasy-Premier-League/master/data"

ALL_SEASONS = [
    '2016-17', '2017-18', '2018-19', '2019-20',
    '2020-21', '2021-22', '2022-23', '2023-24', '2024-25', '2025-26',
]
DEFAULT_SEASONS = ALL_SEASONS[-3:]

# Columns we want to store — filled with None if absent in older seasons
FLOAT_COLS = ['influence', 'creativity', 'threat', 'ict_index',
              'expected_goals', 'expected_assists',
              'expected_goal_involvements', 'expected_goals_conceded']
INT_COLS   = ['minutes', 'goals_scored', 'assists', 'clean_sheets',
              'goals_conceded', 'own_goals', 'penalties_saved',
              'penalties_missed', 'yellow_cards', 'red_cards',
              'saves', 'bonus', 'bps', 'total_points',
              'team_h_score', 'team_a_score', 'value', 'selected',
              'transfers_in', 'transfers_out']


def fetch_csv(url, retries=3):
    headers = {'User-Agent': 'Mozilla/5.0'}
    for attempt in range(retries):
        try:
            r = requests.get(url, headers=headers, timeout=20)
            r.raise_for_status()
            return pd.read_csv(StringIO(r.text))
        except Exception as e:
            if attempt == retries - 1:
                raise
            time.sleep(2 * (attempt + 1))


def ensure_table(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS vaastav_gw_stats (
            season                          TEXT,
            element                         INTEGER,
            name                            TEXT,
            position                        TEXT,
            team                            TEXT,
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
            transfers_in                    INTEGER,
            transfers_out                   INTEGER,
            influence                       REAL,
            creativity                      REAL,
            threat                          REAL,
            ict_index                       REAL,
            expected_goals                  REAL,
            expected_assists                REAL,
            expected_goal_involvements      REAL,
            expected_goals_conceded         REAL,
            PRIMARY KEY (season, element, gw)
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_vaastav_name
        ON vaastav_gw_stats (name, season)
    """)
    conn.commit()


def already_ingested(conn, season):
    cur = conn.execute(
        "SELECT COUNT(*) FROM vaastav_gw_stats WHERE season = ?", (season,)
    )
    return cur.fetchone()[0] > 0


def ingest_season(conn, season, force=False):
    if not force and already_ingested(conn, season):
        print(f"  skip (already ingested): {season}")
        return 0

    if force and already_ingested(conn, season):
        conn.execute("DELETE FROM vaastav_gw_stats WHERE season = ?", (season,))
        conn.commit()
        print(f"  removed existing rows for {season}")

    url = f"{BASE_URL}/{season}/gws/merged_gw.csv"
    print(f"  fetching {url} ...")

    try:
        df = fetch_csv(url)
    except Exception as e:
        print(f"  error fetching {season}: {e}")
        return 0

    # Normalise GW column (older seasons use 'round', newer use 'GW')
    if 'GW' in df.columns:
        df = df.rename(columns={'GW': 'gw'})
    elif 'round' in df.columns:
        df = df.rename(columns={'round': 'gw'})

    if 'gw' not in df.columns or 'name' not in df.columns:
        print(f"  unexpected columns in {season}: {list(df.columns)}")
        return 0

    # Position column varies
    if 'position' not in df.columns and 'element_type' in df.columns:
        pos_map = {1: 'GK', 2: 'DEF', 3: 'MID', 4: 'FWD'}
        df['position'] = df['element_type'].map(pos_map)

    def safe_int(col):
        if col in df.columns:
            return pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
        return pd.Series(0, index=df.index)

    def safe_float(col):
        if col in df.columns:
            return pd.to_numeric(df[col], errors='coerce')
        return pd.Series(pd.NA, index=df.index)

    rows = []
    for _, r in df.iterrows():
        row = {
            'season':   season,
            'element':  int(r.get('element', 0) or 0),
            'name':     str(r.get('name', '')),
            'position': str(r.get('position', '')),
            'team':     str(r.get('team', '')),
            'gw':       int(r.get('gw', 0) or 0),
            'was_home': int(bool(r.get('was_home', 0))),
        }
        for col in INT_COLS:
            val = r.get(col)
            try:
                row[col] = int(float(val)) if pd.notna(val) else 0
            except (TypeError, ValueError):
                row[col] = 0
        for col in FLOAT_COLS:
            val = r.get(col)
            try:
                row[col] = float(val) if pd.notna(val) else None
            except (TypeError, ValueError):
                row[col] = None
        rows.append(row)

    if not rows:
        print(f"  no rows found for {season}")
        return 0

    conn.executemany("""
        INSERT OR REPLACE INTO vaastav_gw_stats VALUES (
            :season, :element, :name, :position, :team, :gw,
            :minutes, :goals_scored, :assists, :clean_sheets, :goals_conceded,
            :own_goals, :penalties_saved, :penalties_missed,
            :yellow_cards, :red_cards, :saves, :bonus, :bps, :total_points,
            :was_home, :team_h_score, :team_a_score, :value, :selected,
            :transfers_in, :transfers_out,
            :influence, :creativity, :threat, :ict_index,
            :expected_goals, :expected_assists,
            :expected_goal_involvements, :expected_goals_conceded
        )
    """, rows)
    conn.commit()
    print(f"  {len(rows)} rows ingested for {season}")
    return len(rows)


def main():
    parser = argparse.ArgumentParser(description="Ingest vaastav FPL historical data")
    parser.add_argument('--seasons', nargs='+', metavar='YYYY-YY',
                        help='Specific seasons to ingest (e.g. 2022-23 2023-24)')
    parser.add_argument('--all',   action='store_true', help='Ingest all seasons (2016-17 onwards)')
    parser.add_argument('--force', action='store_true', help='Re-ingest even if already loaded')
    args = parser.parse_args()

    if args.all:
        seasons = ALL_SEASONS
    elif args.seasons:
        seasons = args.seasons
    else:
        seasons = DEFAULT_SEASONS
        print(f"No seasons specified — defaulting to last 3: {seasons}")

    with sqlite3.connect(str(DB_PATH)) as conn:
        ensure_table(conn)
        total = 0
        for season in seasons:
            total += ingest_season(conn, season, force=args.force)

    print(f"\nDone. {total} total rows ingested.")


if __name__ == '__main__':
    main()
