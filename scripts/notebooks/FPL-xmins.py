"""
FPL xMins Pipeline
==================
Pulls data from the FPL API and builds a local SQLite database of
expected minutes (xMins) per player per fixture.

Usage:
    python fpl_xmins.py                   # Full run: ingest + calculate
    python fpl_xmins.py --ingest-only     # Only pull fresh FPL data
    python fpl_xmins.py --calc-only       # Only recalculate xMins
    python fpl_xmins.py --export          # Export xMins to CSV
    python fpl_xmins.py --player "Salah"  # Show xMins for a player
"""

import sqlite3
import requests
import pandas as pd
import numpy as np
import argparse
import json
import time
from datetime import datetime
from pathlib import Path
import os
import sys

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────
DRIVE_ROOT = r'G:\My Drive\FPL_Model'
OUTPUT_DIR = os.path.join(DRIVE_ROOT, 'outputs')
FPLREVIEW_DIR = os.path.join(DRIVE_ROOT, 'fplreview')
SOLIO_DIR = os.path.join(DRIVE_ROOT, 'solio')

DB_PATH = "fpl_xmins.db"
FPL_BASE = "https://fantasy.premierleague.com/api"

# Ensure output directory exists
os.makedirs(OUTPUT_DIR, exist_ok=True)

# How many recent gameweeks to use for base-rate calculation
HISTORY_WINDOW = 8

# Decay rate per gameweek into the future (0.95 = 5% per GW)
DECAY_RATE = 0.95

# Steeper decay for injury-prone players (injured 2+ times in window)
INJURY_PRONE_DECAY = 0.90

# Minute buckets
SCENARIO_MINS = {
    "full_starter":    90,
    "starter_subbed":  67,
    "sub_appearance":  23,
    "no_minutes":       0,
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; FPL-xMins-Pipeline/1.0)",
    "Accept": "application/json",
}


# ─────────────────────────────────────────────
# DATABASE SETUP
# ─────────────────────────────────────────────
def init_db(conn: sqlite3.Connection):
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS teams (
            id      INTEGER PRIMARY KEY,
            name    TEXT,
            short   TEXT
        );

        CREATE TABLE IF NOT EXISTS players (
            id              INTEGER PRIMARY KEY,
            name            TEXT NOT NULL,
            team_id         INTEGER,
            position        TEXT,
            now_cost        REAL,
            selected_by_pct REAL,
            chance_next     INTEGER,
            news            TEXT,
            news_added      TEXT,
            updated_at      TEXT
        );

        CREATE TABLE IF NOT EXISTS fixtures (
            id              INTEGER PRIMARY KEY,
            gw              INTEGER,
            home_team_id    INTEGER,
            away_team_id    INTEGER,
            home_team_name  TEXT,
            away_team_name  TEXT,
            fdr_home        INTEGER,
            fdr_away        INTEGER,
            kickoff_time    TEXT,
            finished        INTEGER DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS minutes_history (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            player_id   INTEGER,
            fixture_id  INTEGER,
            gw          INTEGER,
            minutes     INTEGER,
            UNIQUE(player_id, fixture_id)
        );

        CREATE TABLE IF NOT EXISTS xmins (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            player_id           INTEGER,
            fixture_id          INTEGER,
            gw                  INTEGER,
            xmins_raw           REAL,
            xmins_decayed       REAL,
            prob_full_starter   REAL,
            prob_starter_subbed REAL,
            prob_sub_appearance REAL,
            prob_no_minutes     REAL,
            avail_adjustment    REAL,
            rotation_risk       REAL,
            updated_at          TEXT,
            UNIQUE(player_id, fixture_id)
        );

        CREATE TABLE IF NOT EXISTS manual_overrides (
            player_id   INTEGER,
            fixture_id  INTEGER,
            xmins_value REAL,
            note        TEXT,
            updated_at  TEXT,
            PRIMARY KEY (player_id, fixture_id)
        );
    """)
    conn.commit()
    print("Database initialised")


# ─────────────────────────────────────────────
# FPL API HELPERS
# ─────────────────────────────────────────────
def fpl_get(endpoint: str) -> dict:
    url = f"{FPL_BASE}/{endpoint}"
    for attempt in range(3):
        try:
            r = requests.get(url, headers=HEADERS, timeout=15)
            r.raise_for_status()
            return r.json()
        except requests.RequestException as e:
            print(f"  ⚠ Attempt {attempt+1} failed for {endpoint}: {e}")
            time.sleep(2 ** attempt)
    raise RuntimeError(f"Failed to fetch {endpoint} after 3 attempts")


def position_label(element_type: int) -> str:
    return {1: "GKP", 2: "DEF", 3: "MID", 4: "FWD"}.get(element_type, "UNK")


# ─────────────────────────────────────────────
# INGEST
# ─────────────────────────────────────────────
def ingest_bootstrap(conn: sqlite3.Connection):
    """Pull teams + players from /bootstrap-static/"""
    print("\n⬇  Fetching bootstrap data...")
    data = fpl_get("bootstrap-static/")

    # Teams
    teams = [(t["id"], t["name"], t["short_name"]) for t in data["teams"]]
    conn.executemany(
        "INSERT OR REPLACE INTO teams VALUES (?,?,?)", teams
    )

    # Players
    now = datetime.utcnow().isoformat()
    players = []
    for p in data["elements"]:
        players.append((
            p["id"],
            f"{p['first_name']} {p['second_name']}",
            p["team"],
            position_label(p["element_type"]),
            p["now_cost"] / 10,
            float(p.get("selected_by_percent", 0) or 0),
            p.get("chance_of_playing_next_round"),
            p.get("news", ""),
            p.get("news_added", ""),
            now,
        ))
    conn.executemany(
        """INSERT OR REPLACE INTO players
           VALUES (?,?,?,?,?,?,?,?,?,?)""",
        players,
    )
    conn.commit()
    print(f"  {len(teams)} teams, {len(players)} players saved")


def ingest_fixtures(conn: sqlite3.Connection):
    """Pull all fixtures"""
    print("\n⬇  Fetching fixtures...")
    data = fpl_get("fixtures/")

    # Build team name lookup
    teams = {r[0]: (r[1], r[2]) for r in conn.execute("SELECT id, name, short FROM teams")}

    fixtures = []
    for f in data:
        ht = f.get("team_h")
        at = f.get("team_a")
        fixtures.append((
            f["id"],
            f.get("event"),
            ht,
            at,
            teams.get(ht, ("?", "?"))[0],
            teams.get(at, ("?", "?"))[0],
            f.get("team_h_difficulty"),
            f.get("team_a_difficulty"),
            f.get("kickoff_time", ""),
            1 if f.get("finished") else 0,
        ))
    conn.executemany(
        "INSERT OR REPLACE INTO fixtures VALUES (?,?,?,?,?,?,?,?,?,?)",
        fixtures,
    )
    conn.commit()
    print(f"  {len(fixtures)} fixtures saved")


def ingest_player_history(conn: sqlite3.Connection):
    """
    Pull per-player history. Only fetches players not yet loaded or
    those whose data may have changed (started, dubious news).
    """
    print("\n⬇  Fetching player histories (this takes a minute)...")

    player_ids = [
        r[0] for r in conn.execute(
            "SELECT id FROM players ORDER BY selected_by_pct DESC"
        )
    ]

    inserted = 0
    for i, pid in enumerate(player_ids):
        try:
            data = fpl_get(f"element-summary/{pid}/")
        except RuntimeError:
            continue

        rows = []
        for h in data.get("history", []):
            rows.append((
                pid,
                h.get("fixture"),
                h.get("round"),
                h.get("minutes", 0),
            ))

        if rows:
            conn.executemany(
                """INSERT OR IGNORE INTO minutes_history
                   (player_id, fixture_id, gw, minutes) VALUES (?,?,?,?)""",
                rows,
            )
            inserted += len(rows)

        if (i + 1) % 50 == 0:
            conn.commit()
            print(f"    {i+1}/{len(player_ids)} players processed...")

    conn.commit()
    print(f"  {inserted} history rows saved")


# ─────────────────────────────────────────────
# XMINS CALCULATION
# ─────────────────────────────────────────────
def classify_minutes(mins: int) -> str:
    if mins >= 75:  return "full_starter"
    if mins >= 45:  return "starter_subbed"
    if mins >= 1:   return "sub_appearance"
    return "no_minutes"


def availability_multiplier(chance: int | None) -> float:
    """Convert FPL's chance_of_playing into a starter probability scalar."""
    if chance is None:
        return 1.0   # No news = assume available
    return {100: 1.0, 75: 0.75, 50: 0.50, 25: 0.25, 0: 0.0}.get(chance, 1.0)


def rotation_risk_score(history_df: pd.DataFrame) -> float:
    """
    0.0 = never rotated, 1.0 = always rotated.
    Based on proportion of recent games with 0 minutes (when named in squad).
    """
    if history_df.empty:
        return 0.3  # unknown = moderate risk
    no_mins = (history_df["minutes"] == 0).sum()
    return round(no_mins / len(history_df), 3)


def injury_prone(history_df: pd.DataFrame) -> bool:
    """True if player had 2+ distinct injury absences in the window."""
    mins = history_df["minutes"].tolist()
    absences = 0
    in_absence = False
    for m in mins:
        if m == 0:
            if not in_absence:
                absences += 1
                in_absence = True
        else:
            in_absence = False
    return absences >= 2


def compute_scenario_probs(history_df: pd.DataFrame) -> dict:
    """
    Derive scenario probabilities from historical minute classifications.
    Returns a dict of {scenario: probability}.
    """
    if history_df.empty:
        return {
            "full_starter":    0.50,
            "starter_subbed":  0.15,
            "sub_appearance":  0.15,
            "no_minutes":      0.20,
        }

    counts = history_df["scenario"].value_counts()
    total  = len(history_df)

    probs = {}
    for s in SCENARIO_MINS:
        probs[s] = counts.get(s, 0) / total

    # Laplace smoothing — prevents any scenario being hard-zero
    alpha = 0.5
    n_scenarios = len(SCENARIO_MINS)
    for s in probs:
        probs[s] = (probs[s] * total + alpha) / (total + alpha * n_scenarios)

    return probs


def weighted_scenario_probs(history_df: pd.DataFrame) -> dict:
    """Weight recent games more heavily when deriving scenario probabilities."""
    if history_df.empty:
        return compute_scenario_probs(history_df)

    history_df = history_df.reset_index(drop=True).copy()
    history_df['weight'] = [0.6 ** i for i in range(len(history_df) - 1, -1, -1)]
    counts = history_df.groupby('scenario')['weight'].sum().to_dict()
    total = history_df['weight'].sum()

    probs = {}
    for s in SCENARIO_MINS:
        probs[s] = counts.get(s, 0.0) / total

    alpha = 0.5
    n_scenarios = len(SCENARIO_MINS)
    for s in probs:
        probs[s] = (probs[s] * total + alpha) / (total + alpha * n_scenarios)

    return probs


def quality_adjustment(position: str, cost: float, selected_by_pct: float | None) -> float:
    """Adjust starter probability based on cost and ownership quality signals."""
    quality = 1.0
    if selected_by_pct is not None:
        quality += (selected_by_pct - 20.0) / 250.0

    if position in ('MID', 'FWD'):
        quality += (cost - 7.0) / 80.0
    else:
        quality += (cost - 5.0) / 120.0

    return max(0.80, min(quality, 1.15))


def fixture_difficulty_multiplier(position: str, difficulty: int, home: bool) -> float:
    """Scale xMins modestly by fixture difficulty and home advantage."""
    diff = float(difficulty) if difficulty is not None else 3.0
    multiplier = 1.0 - 0.03 * (diff - 3.0)
    if home:
        multiplier += 0.02
    return max(0.82, min(multiplier, 1.08))


def calculate_xmins(conn: sqlite3.Connection):
    """Main xMins calculation loop over all upcoming fixtures."""
    print("\nCalculating xMins...")

    # Current GW
    current_gw = conn.execute(
        "SELECT MAX(gw) FROM minutes_history"
    ).fetchone()[0] or 1

    upcoming = pd.read_sql(
        """SELECT id as fixture_id, gw, home_team_id, away_team_id,
                  fdr_home, fdr_away, kickoff_time
           FROM fixtures
           WHERE finished = 0 AND gw IS NOT NULL
           ORDER BY gw""",
        conn,
    )

    players = pd.read_sql(
        "SELECT id, name, team_id, position, now_cost, selected_by_pct, chance_next FROM players",
        conn,
    )

    history = pd.read_sql(
        """SELECT player_id, fixture_id, gw, minutes
           FROM minutes_history
           ORDER BY player_id, gw""",
        conn,
    )

    now = datetime.utcnow().isoformat()
    rows = []

    for _, player in players.iterrows():
        pid = player["id"]
        team = player["team_id"]

        # Recent history for this player
        ph = history[history["player_id"] == pid].tail(HISTORY_WINDOW).copy()
        ph["scenario"] = ph["minutes"].apply(classify_minutes)

        probs      = weighted_scenario_probs(ph)
        rot_risk   = rotation_risk_score(ph)
        prone      = injury_prone(ph)
        avail_mult = availability_multiplier(player["chance_next"])
        quality    = quality_adjustment(player["position"], player["now_cost"], player["selected_by_pct"])

        # Base xMins (no decay, no availability adjustment yet)
        base_xmins = sum(probs[s] * SCENARIO_MINS[s] for s in SCENARIO_MINS)

        # Apply availability multiplier (scales down starter probability)
        adj_probs = probs.copy()
        adj_probs["full_starter"]    *= avail_mult
        adj_probs["starter_subbed"]  *= avail_mult
        # Redistribute lost probability to no_minutes
        lost = (1 - avail_mult) * (probs["full_starter"] + probs["starter_subbed"])
        adj_probs["no_minutes"] = min(1.0, adj_probs["no_minutes"] + lost)

        # Adjust for player quality: premium players should have stronger starter probability,
        # cheap/low-owned players should be slightly more likely to see lower-minute outcomes.
        adj_probs["full_starter"]    *= quality
        adj_probs["starter_subbed"]  *= 1.0 + (quality - 1.0) * 0.4
        adj_probs["sub_appearance"]  *= 1.0 - (quality - 1.0) * 0.3

        total_prob = sum(adj_probs.values())
        if total_prob > 0:
            for s in adj_probs:
                adj_probs[s] = adj_probs[s] / total_prob

        xmins_adjusted = sum(adj_probs[s] * SCENARIO_MINS[s] for s in SCENARIO_MINS)

        # Fixtures this player's team plays
        team_fixtures = upcoming[
            (upcoming["home_team_id"] == team) | (upcoming["away_team_id"] == team)
        ].sort_values("gw")

        decay = INJURY_PRONE_DECAY if prone else DECAY_RATE

        for gw_offset, (_, fix) in enumerate(team_fixtures.iterrows()):
            decayed = xmins_adjusted * (decay ** gw_offset)
            home = fix["home_team_id"] == team
            difficulty = fix["fdr_home"] if home else fix["fdr_away"]
            diff_factor = fixture_difficulty_multiplier(player["position"], difficulty, home)
            decayed = round(min(decayed * diff_factor, 90), 2)

            rows.append((
                pid,
                fix["fixture_id"],
                fix["gw"],
                round(base_xmins, 2),
                decayed,
                round(adj_probs["full_starter"], 4),
                round(adj_probs["starter_subbed"], 4),
                round(adj_probs["sub_appearance"], 4),
                round(adj_probs["no_minutes"], 4),
                round(avail_mult, 2),
                round(rot_risk, 3),
                now,
            ))

    conn.executemany(
        """INSERT OR REPLACE INTO xmins
           (player_id, fixture_id, gw, xmins_raw, xmins_decayed,
            prob_full_starter, prob_starter_subbed, prob_sub_appearance,
            prob_no_minutes, avail_adjustment, rotation_risk, updated_at)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
        rows,
    )
    conn.commit()
    print(f"  {len(rows)} xMins rows calculated")


# ─────────────────────────────────────────────
# MANUAL OVERRIDE
# ─────────────────────────────────────────────
def set_override(conn: sqlite3.Connection, player_name: str,
                 gw: int, xmins_value: float, note: str = ""):
    """Manually override xMins for a player for a specific GW."""
    player = conn.execute(
        "SELECT id, name FROM players WHERE name LIKE ?",
        (f"%{player_name}%",)
    ).fetchone()

    if not player:
        print(f"  ✗ Player '{player_name}' not found")
        return

    fixtures = conn.execute(
        "SELECT id FROM fixtures WHERE gw = ? LIMIT 1", (gw,)
    ).fetchone()

    if not fixtures:
        print(f"  ✗ No fixture found for GW{gw}")
        return

    now = datetime.utcnow().isoformat()
    conn.execute(
        """INSERT OR REPLACE INTO manual_overrides
           VALUES (?,?,?,?,?)""",
        (player[0], fixtures[0], xmins_value, note, now),
    )
    conn.commit()
    print(f"  Override set: {player[1]} GW{gw} -> {xmins_value} mins")


# ─────────────────────────────────────────────
# EXPORT + DISPLAY
# ─────────────────────────────────────────────
def export_csv(conn: sqlite3.Connection, path: str = None):
    """Export xMins joined with player/fixture info to CSV."""
    if path is None:
        path = os.path.join(OUTPUT_DIR, 'fpl_xmins_export.csv')
    
    df = pd.read_sql(
        """SELECT
               p.id AS fpl_id,
               p.name,
               p.position,
               t.name AS team,
               p.now_cost AS cost,
               f.gw,
               f.home_team_name,
               f.away_team_name,
               CASE WHEN f.home_team_id = p.team_id
                    THEN 'H' ELSE 'A' END AS venue,
               COALESCE(mo.xmins_value, x.xmins_decayed) AS xmins,
               x.xmins_raw,
               x.prob_full_starter,
               x.prob_starter_subbed,
               x.prob_sub_appearance,
               x.prob_no_minutes,
               x.avail_adjustment,
               x.rotation_risk,
               p.chance_next AS availability_pct,
               p.news
           FROM xmins x
           JOIN players  p ON p.id = x.player_id
           JOIN teams    t ON t.id = p.team_id
           JOIN fixtures f ON f.id = x.fixture_id
           LEFT JOIN manual_overrides mo
               ON mo.player_id = x.player_id AND mo.fixture_id = x.fixture_id
           ORDER BY f.gw, xmins DESC""",
        conn,
    )
    df.to_csv(path, index=False)
    print(f"  Exported {len(df)} rows to {path}")
    return df


def load_benchmarks():
    """Load FPLReview and Solio benchmark xMins data."""
    benchmarks = {}
    
    # Load FPLReview
    try:
        fpl_files = [f for f in os.listdir(FPLREVIEW_DIR) if f.endswith('.csv')]
        if fpl_files:
            fpl_file = os.path.join(FPLREVIEW_DIR, sorted(fpl_files)[-1])  # Use latest file
            fpl_df = pd.read_csv(fpl_file)
            if 'ID' in fpl_df.columns:
                # Melt GW columns (35_xMins, 36_xMins, etc.)
                gw_cols = [c for c in fpl_df.columns if '_xMins' in c]
                if gw_cols:
                    id_vars = ['ID', 'Name', 'Team']
                    fpl_df = fpl_df.melt(id_vars=id_vars, value_vars=gw_cols, 
                                        var_name='GW', value_name='xMins')
                    fpl_df['GW'] = fpl_df['GW'].str.replace('_xMins', '').astype(int)
                    benchmarks['fplreview'] = fpl_df[['ID', 'Name', 'Team', 'GW', 'xMins']].copy()
                    benchmarks['fplreview'].rename(columns={'ID': 'fpl_id'}, inplace=True)
    except Exception as e:
        print(f"  ⚠ Could not load FPLReview data: {e}")
    
    # Load Solio
    try:
        solio_files = [f for f in os.listdir(SOLIO_DIR) if f.startswith('projection_all_metrics') and f.endswith('.csv')]
        if solio_files:
            solio_file = os.path.join(SOLIO_DIR, sorted(solio_files)[-1])  # Use latest file
            solio_df = pd.read_csv(solio_file)
            if 'ID' in solio_df.columns:
                # Melt GW columns (35_xMins, 36_xMins, etc.)
                gw_cols = [c for c in solio_df.columns if '_xMins' in c]
                if gw_cols:
                    id_vars = ['ID', 'Name', 'Team']
                    solio_df = solio_df.melt(id_vars=id_vars, value_vars=gw_cols,
                                            var_name='GW', value_name='xMins')
                    solio_df['GW'] = solio_df['GW'].str.replace('_xMins', '').astype(int)
                    benchmarks['solio'] = solio_df[['ID', 'Name', 'Team', 'GW', 'xMins']].copy()
                    benchmarks['solio'].rename(columns={'ID': 'fpl_id'}, inplace=True)
    except Exception as e:
        print(f"  ⚠ Could not load Solio data: {e}")
    
    return benchmarks


def benchmark_xmins(df_export):
    """Merge calculated xMins with benchmarks and compute deltas."""
    print("\nBenchmarking against FPLReview and Solio...")
    benchmarks = load_benchmarks()
    
    df_bench = df_export.copy()
    
    # Merge with FPLReview
    if benchmarks.get('fplreview') is not None:
        fpl = benchmarks['fplreview'][['fpl_id', 'GW', 'xMins']].rename(columns={'xMins': 'fplreview_xMins', 'GW': 'gw'})
        df_bench = df_bench.merge(fpl, on=['fpl_id', 'gw'], how='left')
    else:
        df_bench['fplreview_xMins'] = np.nan
    
    # Merge with Solio
    if benchmarks.get('solio') is not None:
        solio = benchmarks['solio'][['fpl_id', 'GW', 'xMins']].rename(columns={'xMins': 'solio_xMins', 'GW': 'gw'})
        df_bench = df_bench.merge(solio, on=['fpl_id', 'gw'], how='left')
    else:
        df_bench['solio_xMins'] = np.nan
    
    # Calculate mean benchmark and delta
    df_bench['benchmark_mean'] = df_bench[['fplreview_xMins', 'solio_xMins']].mean(axis=1)
    df_bench['delta'] = df_bench['xmins'] - df_bench['benchmark_mean']
    df_bench['delta'] = df_bench['delta'].round(2)
    
    # Calculate summary stats
    valid_deltas = df_bench['delta'].dropna()
    if not valid_deltas.empty:
        mae = valid_deltas.abs().mean()
        mean_delta = valid_deltas.mean()
        print(f"  MAE vs benchmark average: {mae:.3f}")
        print(f"  Mean delta: {mean_delta:.3f}")
    
    return df_bench


def show_player(conn: sqlite3.Connection, name: str):
    """Print xMins breakdown for a specific player."""
    df = pd.read_sql(
        """SELECT
               p.name,
               f.gw,
               f.home_team_name || ' v ' || f.away_team_name AS fixture,
               CASE WHEN f.home_team_id = p.team_id THEN 'H' ELSE 'A' END AS venue,
               COALESCE(mo.xmins_value, x.xmins_decayed) AS xmins,
               ROUND(x.prob_full_starter * 100) || '%'    AS p_full,
               ROUND(x.prob_sub_appearance * 100) || '%'  AS p_sub,
               ROUND(x.prob_no_minutes * 100) || '%'      AS p_none,
               x.rotation_risk,
               p.news
           FROM xmins x
           JOIN players  p ON p.id = x.player_id
           JOIN fixtures f ON f.id = x.fixture_id
           LEFT JOIN manual_overrides mo
               ON mo.player_id = x.player_id AND mo.fixture_id = x.fixture_id
           WHERE p.name LIKE ?
           ORDER BY f.gw""",
        conn,
        params=(f"%{name}%",),
    )
    if df.empty:
        print(f"No xMins data found for '{name}'")
    else:
        print(f"\n{'─'*70}")
        print(f"  {df['name'].iloc[0]}")
        print(f"{'─'*70}")
        print(df.drop(columns="name").to_string(index=False))
        news = df["news"].iloc[0]
        if news:
            print(f"\n  📰 News: {news}")


# ─────────────────────────────────────────────
# MOCK DATA (for demo / offline testing)
# ─────────────────────────────────────────────
def load_mock_data(conn: sqlite3.Connection):
    """
    Loads realistic mock FPL data so you can test the full pipeline
    without hitting the FPL API. Mirrors a typical mid-season state.
    """
    print("\nLoading demo data...")

    teams = [
        (1, "Arsenal",          "ARS"), (2, "Aston Villa",    "AVL"),
        (3, "Bournemouth",      "BOU"), (4, "Brentford",       "BRE"),
        (5, "Brighton",         "BHA"), (6, "Chelsea",         "CHE"),
        (7, "Crystal Palace",   "CRY"), (8, "Everton",         "EVE"),
        (9, "Fulham",           "FUL"), (10,"Ipswich",         "IPS"),
        (11,"Leicester",        "LEI"), (12,"Liverpool",       "LIV"),
        (13,"Man City",         "MCI"), (14,"Man Utd",         "MUN"),
        (15,"Newcastle",        "NEW"), (16,"Nottm Forest",    "NFO"),
        (17,"Southampton",      "SOU"), (18,"Spurs",           "TOT"),
        (19,"West Ham",         "WHU"), (20,"Wolves",          "WOL"),
    ]
    conn.executemany("INSERT OR REPLACE INTO teams VALUES (?,?,?)", teams)

    now = datetime.utcnow().isoformat()
    # (id, name, team_id, pos, cost, sel_pct, chance_next, news, news_added, updated_at)
    players = [
        # Liverpool
        (1,  "Mohamed Salah",        12, "MID", 13.5, 65.2, None,  "",                                    "",  now),
        (2,  "Trent Alexander-Arnold",12,"DEF",  9.0, 40.1, None,  "",                                    "",  now),
        (3,  "Darwin Nunez",          12, "FWD",  7.5, 12.3,   50, "Doubt: knock - 50% chance of playing","",  now),
        # Man City
        (4,  "Phil Foden",            13, "MID", 10.0, 35.4, None,  "",                                    "",  now),
        (5,  "Erling Haaland",        13, "FWD", 14.0, 72.1, None,  "",                                    "",  now),
        (6,  "Kevin De Bruyne",       13, "MID",  9.5, 20.2,   75, "Doubt: muscle - 75% chance",          "",  now),
        # Arsenal
        (7,  "Bukayo Saka",            1, "MID", 10.5, 55.3, None,  "",                                    "",  now),
        (8,  "Martin Odegaard",        1, "MID",  9.0, 28.7, None,  "",                                    "",  now),
        (9,  "Gabriel Martinelli",     1, "MID",  7.5,  8.9, None,  "",                                    "",  now),
        # Chelsea
        (10, "Cole Palmer",            6, "MID", 11.0, 48.2, None,  "",                                    "",  now),
        (11, "Nicolas Jackson",        6, "FWD",  7.5, 18.6, None,  "",                                    "",  now),
        # Spurs
        (12, "Son Heung-min",         18, "MID",  9.5, 25.4, None,  "",                                    "",  now),
        (13, "James Maddison",        18, "MID",  7.5,  9.1,    0, "Injured - out",                       "",  now),
        # Newcastle
        (14, "Alexander Isak",        15, "FWD", 10.0, 38.7, None,  "",                                    "",  now),
        (15, "Bruno Guimaraes",       15, "MID",  6.5,  7.2, None,  "",                                    "",  now),
    ]
    conn.executemany(
        "INSERT OR REPLACE INTO players VALUES (?,?,?,?,?,?,?,?,?,?)", players
    )

    # GW36–38 fixtures
    fixtures = [
        (101, 36,  1, 13, "Arsenal",   "Man City",  4, 5, "2025-05-03T14:00:00Z", 0),
        (102, 36, 12, 18, "Liverpool", "Spurs",     2, 4, "2025-05-03T16:30:00Z", 0),
        (103, 36,  6, 15, "Chelsea",   "Newcastle", 3, 3, "2025-05-04T14:00:00Z", 0),
        (104, 37, 13,  1, "Man City",  "Arsenal",   4, 4, "2025-05-10T14:00:00Z", 0),
        (105, 37, 18, 12, "Spurs",     "Liverpool", 4, 2, "2025-05-10T16:30:00Z", 0),
        (106, 37, 15,  6, "Newcastle", "Chelsea",   3, 3, "2025-05-11T14:00:00Z", 0),
        (107, 38,  1, 15, "Arsenal",   "Newcastle", 3, 3, "2025-05-18T16:00:00Z", 0),
        (108, 38, 12,  6, "Liverpool", "Chelsea",   2, 4, "2025-05-18T16:00:00Z", 0),
        (109, 38, 13, 18, "Man City",  "Spurs",     3, 4, "2025-05-18T16:00:00Z", 0),
    ]
    conn.executemany(
        "INSERT OR REPLACE INTO fixtures VALUES (?,?,?,?,?,?,?,?,?,?)", fixtures
    )

    # Realistic minutes history (last 8 GWs)
    # Format: (player_id, fixture_id, gw, minutes)
    # We'll use fixture IDs 1-80 as historical (already finished)
    history_raw = {
        # Salah - nailed, always plays 80-90
        1:  [90,90,88,90,90,85,90,90],
        # TAA - nailed defender
        2:  [90,90,90,85,90,90,88,90],
        # Nunez - rotation risk, some blanks
        3:  [45,0,67,90,0,55,80,0],
        # Foden - rotation, sometimes rested
        4:  [72,0,90,85,55,0,90,80],
        # Haaland - nailed, monster mins
        5:  [90,90,90,88,90,85,90,90],
        # KDB - injury history, mixed
        6:  [0,0,67,80,90,0,75,85],
        # Saka - nailed
        7:  [90,85,90,90,88,90,90,85],
        # Odegaard - regular starter
        8:  [85,90,78,90,90,85,90,82],
        # Martinelli - some rotation
        9:  [90,72,0,85,90,55,0,90],
        # Cole Palmer - nailed
        10: [90,90,88,90,85,90,90,78],
        # Jackson - moderate rotation
        11: [75,90,55,0,90,80,90,65],
        # Son - regular
        12: [85,90,90,72,88,90,85,90],
        # Maddison - injured (recent 0s)
        13: [90,85,0,0,0,0,0,0],
        # Isak - nailed
        14: [90,88,85,90,90,82,90,85],
        # Bruno G - regular
        15: [88,90,85,90,78,90,90,85],
    }

    rows = []
    for pid, mins_list in history_raw.items():
        for i, mins in enumerate(mins_list):
            rows.append((pid, 200 + i, 28 + i, mins))

    conn.executemany(
        "INSERT OR IGNORE INTO minutes_history (player_id, fixture_id, gw, minutes) VALUES (?,?,?,?)",
        rows,
    )
    conn.commit()
    print(f"  Mock data loaded: {len(teams)} teams, {len(players)} players, "
          f"{len(fixtures)} fixtures, {len(rows)} history rows")


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="FPL xMins Pipeline")
    parser.add_argument("--ingest-only", action="store_true")
    parser.add_argument("--calc-only",   action="store_true")
    parser.add_argument("--export",      action="store_true")
    parser.add_argument("--demo",        action="store_true",
                        help="Load realistic mock data (no FPL API needed)")
    parser.add_argument("--player",      type=str, help="Show xMins for a player")
    parser.add_argument("--override",    nargs=4,
                        metavar=("PLAYER", "GW", "XMINS", "NOTE"),
                        help="Set a manual override")
    args = parser.parse_args()

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    init_db(conn)

    if args.demo:
        load_mock_data(conn)
        calculate_xmins(conn)
        df_export = export_csv(conn)
        df_bench = benchmark_xmins(df_export)
        bench_path = os.path.join(OUTPUT_DIR, 'fpl_xmins_benchmarked.csv')
        df_bench.to_csv(bench_path, index=False)
        print(f"  Benchmarked output saved to {bench_path}")
        print("\nDemo pipeline complete!")

    elif args.override:
        name, gw, xmins_val, note = args.override
        set_override(conn, name, int(gw), float(xmins_val), note)

    elif args.player:
        show_player(conn, args.player)

    elif args.export:
        df_export = export_csv(conn)
        df_bench = benchmark_xmins(df_export)
        bench_path = os.path.join(OUTPUT_DIR, 'fpl_xmins_benchmarked.csv')
        df_bench.to_csv(bench_path, index=False)
        print(f"  Benchmarked output saved to {bench_path}")

    elif args.ingest_only:
        ingest_bootstrap(conn)
        ingest_fixtures(conn)
        ingest_player_history(conn)

    elif args.calc_only:
        calculate_xmins(conn)

    else:
        # Full run
        ingest_bootstrap(conn)
        ingest_fixtures(conn)
        ingest_player_history(conn)
        calculate_xmins(conn)
        df_export = export_csv(conn)
        df_bench = benchmark_xmins(df_export)
        bench_path = os.path.join(OUTPUT_DIR, 'fpl_xmins_benchmarked.csv')
        df_bench.to_csv(bench_path, index=False)
        print(f"  Benchmarked output saved to {bench_path}")
        print("\nPipeline complete!")

    conn.close()


if __name__ == "__main__":
    main()