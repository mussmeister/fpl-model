"""
Poll independent player xPts projections every 2 hours, store in SQLite.

Uses FPL bootstrap-static API for the player universe (no Solio/FPLReview CSVs)
and a Dixon-Coles team model for expected goals → player xPts.

Run:
    python scripts/poll_player_projections.py

Schedule via cron (server):
    0 */2 * * * /home/ubuntu/fpl-model/venv/bin/python3 /home/ubuntu/fpl-model/scripts/poll_player_projections.py >> /home/ubuntu/fpl-model/logs/poll_players.log 2>&1
"""
import sys
import json
import sqlite3
import requests
import numpy as np
import pandas as pd
from pathlib import Path
from datetime import datetime

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / 'scripts'))

from utils.dc_model import fit_dc_ratings, run_projections
from utils.data_loaders import load_season_results, load_fixtures, fetch_odds
from utils.player_model import estimate_player_xpts
from utils.team_mappings import to_short, ODDS_TO_SHORT

DB_PATH     = ROOT / 'outputs' / 'projections_history.db'
CONFIG_PATH = ROOT / 'config' / 'config.json'

FPL_BOOTSTRAP_URL = "https://fantasy.premierleague.com/api/bootstrap-static/"

POS_MAP = {1: "G", 2: "D", 3: "M", 4: "F"}

_DDL = """
CREATE TABLE IF NOT EXISTS player_projection_model (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp    TEXT    NOT NULL,
    fpl_id       INTEGER,
    name         TEXT    NOT NULL,
    team         TEXT    NOT NULL,
    pos          TEXT    NOT NULL,
    bv           REAL,
    gw           INTEGER NOT NULL,
    xmins        REAL,
    xpts         REAL,
    goal_prob    REAL,
    assist_prob  REAL,
    cs_prob      REAL,
    bonus_prob   REAL,
    appearance_pts REAL,
    save_pts     REAL,
    UNIQUE(timestamp, fpl_id, gw)
)
"""

_IDX = """
CREATE INDEX IF NOT EXISTS idx_ppm_ts_gw
ON player_projection_model (timestamp, gw)
"""

_IDX2 = """
CREATE INDEX IF NOT EXISTS idx_ppm_name_team
ON player_projection_model (name, team)
"""


def ensure_table(conn):
    conn.execute(_DDL)
    conn.execute(_IDX)
    conn.execute(_IDX2)
    conn.commit()


def fetch_fpl_bootstrap():
    r = requests.get(FPL_BOOTSTRAP_URL, timeout=30,
                     headers={"User-Agent": "FPL-Model/1.0"})
    r.raise_for_status()
    return r.json()


def build_player_df(fpl_data):
    """Build player DataFrame from FPL bootstrap-static JSON.
    Maps FPL team names → DC model short names via ODDS_TO_SHORT.
    """
    team_map = {}
    for t in fpl_data.get("teams", []):
        dc_name = to_short(t["name"], ODDS_TO_SHORT)
        team_map[t["id"]] = dc_name

    rows = []
    for p in fpl_data.get("elements", []):
        if p.get("status") == "u":      # skip permanently unavailable
            continue
        team_dc = team_map.get(p["team"])
        if not team_dc:
            continue
        rows.append({
            "fpl_id": p["id"],
            "Name":   p["web_name"],
            "Team":   team_dc,
            "Pos":    POS_MAP.get(p["element_type"], "M"),
            "BV":     p["now_cost"] / 10.0,
            "xMins":  0.0,   # placeholder; overwritten by estimate_player_xpts
        })

    return pd.DataFrame(rows)


def build_fixture_counts(df_fixtures, target_gws):
    """Return {(gw, team): fixture_count} — 2 for DGW teams, 1 for SGW."""
    counts = {}
    for gw in target_gws:
        gw_fix = df_fixtures[df_fixtures["GW"] == gw]
        for _, row in gw_fix.iterrows():
            for team in (row["Home"], row["Away"]):
                counts[(gw, team)] = counts.get((gw, team), 0) + 1
    return counts


def store_projections(conn, timestamp, df):
    rows = []
    for _, r in df.iterrows():
        rows.append({
            "timestamp":     timestamp,
            "fpl_id":        int(r["fpl_id"]) if pd.notna(r.get("fpl_id")) else None,
            "name":          str(r["Name"]),
            "team":          str(r["Team"]),
            "pos":           str(r["Pos"]),
            "bv":            float(r["BV"])          if pd.notna(r.get("BV"))          else None,
            "gw":            int(r["GW"]),
            "xmins":         float(r["xMins"])       if pd.notna(r.get("xMins"))       else None,
            "xpts":          float(r["model_xPts"])  if pd.notna(r.get("model_xPts"))  else None,
            "goal_prob":     float(r["goal_prob"])   if pd.notna(r.get("goal_prob"))   else None,
            "assist_prob":   float(r["assist_prob"]) if pd.notna(r.get("assist_prob")) else None,
            "cs_prob":       float(r["cs_prob"])     if pd.notna(r.get("cs_prob"))     else None,
            "bonus_prob":    float(r["bonus_prob"])  if pd.notna(r.get("bonus_prob"))  else None,
            "appearance_pts":float(r["appearance_pts"]) if pd.notna(r.get("appearance_pts")) else None,
            "save_pts":      float(r["save_pts"])    if pd.notna(r.get("save_pts"))    else None,
        })

    conn.executemany("""
        INSERT OR IGNORE INTO player_projection_model
            (timestamp, fpl_id, name, team, pos, bv, gw,
             xmins, xpts, goal_prob, assist_prob, cs_prob,
             bonus_prob, appearance_pts, save_pts)
        VALUES
            (:timestamp, :fpl_id, :name, :team, :pos, :bv, :gw,
             :xmins, :xpts, :goal_prob, :assist_prob, :cs_prob,
             :bonus_prob, :appearance_pts, :save_pts)
    """, rows)
    conn.commit()
    return len(rows)


def poll_and_store():
    config    = json.loads(CONFIG_PATH.read_text())
    season    = config.get("season", "2526")
    timestamp = datetime.now().isoformat(timespec="seconds")

    print(f"\n[{timestamp}] Starting player projection model poll...")

    with sqlite3.connect(str(DB_PATH)) as conn:
        ensure_table(conn)

        # ── 1. FPL API — player universe ──────────────────────────────────
        print("  Fetching FPL bootstrap-static...")
        fpl_data   = fetch_fpl_bootstrap()
        df_players = build_player_df(fpl_data)
        print(f"  Players loaded: {len(df_players)}")

        # ── 2. Season results + DC ratings ───────────────────────────────
        print("  Loading season results...")
        results, all_teams, team_idx, avg_home, avg_away = load_season_results(season)

        print("  Fitting DC ratings...")
        ratings, home_adv = fit_dc_ratings(
            config.get("xi", 0.002),
            config.get("dc_rho", -0.073),
            results, all_teams, team_idx, avg_home, avg_away,
            ah_weight=config.get("ah_weight", 0.0),
        )

        # ── 3. Fixtures + odds + team projections ─────────────────────────
        print("  Loading fixtures...")
        df_target, target_gws = load_fixtures(ROOT / "fixtures")

        print("  Fetching odds...")
        try:
            odds_lookup = fetch_odds(config.get("odds_api_key"))
        except Exception as e:
            print(f"  Odds fetch failed ({e}), using ratings only")
            odds_lookup = {}

        print("  Running team projections...")
        projections = run_projections(
            ratings, home_adv,
            config.get("shrinkage_weight", 0.9357),
            config.get("blend_weight", 0.9322),
            config.get("dc_rho", -0.073),
            df_target, list(target_gws),
            odds_lookup, avg_home, avg_away,
        )

        # ── 4. Expand players × GWs and estimate xPts ────────────────────
        fixture_counts = build_fixture_counts(df_target, target_gws)

        gw_frames = []
        for gw in target_gws:
            frame = df_players.copy()
            frame["GW"] = gw
            gw_frames.append(frame)

        df_all = pd.concat(gw_frames, ignore_index=True)

        print("  Estimating player xPts...")
        df_result = estimate_player_xpts(
            df_all, projections, fixture_counts,
            use_source_minutes=False,
        )
        df_result = df_result[df_result["xMins"] > 0].copy()
        print(f"  Players with xMins > 0: {len(df_result)}")

        # ── 5. Store ──────────────────────────────────────────────────────
        n = store_projections(conn, timestamp, df_result)
        print(f"[{timestamp}] [OK] Stored {n} rows across {len(target_gws)} GWs")


if __name__ == "__main__":
    poll_and_store()
