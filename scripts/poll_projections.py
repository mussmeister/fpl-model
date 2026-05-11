"""
Polling script: fetch odds + run projections every 2 hours, store in SQLite.

Runs two pipelines in one pass (single DC fit, single odds call):
  1. Team projections  → projections table  (goals, CS per team per GW)
  2. Player projections → player_projection_model table  (xMins, xPts per player per GW)
"""
import sys
from pathlib import Path
import json
import sqlite3
import requests
from datetime import datetime
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'scripts'))

from utils.data_loaders import load_season_results, fetch_odds, load_fixtures
from utils.dc_model import fit_dc_ratings, run_projections
from utils.player_model import estimate_player_xpts
from utils.team_mappings import to_short, ODDS_TO_SHORT

FPL_BOOTSTRAP_URL = "https://fantasy.premierleague.com/api/bootstrap-static/"
POS_MAP = {1: "G", 2: "D", 3: "M", 4: "F"}

# ── Schema ────────────────────────────────────────────────────────────────────

def init_db(db_path):
    conn = sqlite3.connect(db_path)
    c = conn.cursor()

    c.execute("""
        CREATE TABLE IF NOT EXISTS projections (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            gw INTEGER NOT NULL,
            team TEXT NOT NULL,
            opponent TEXT NOT NULL,
            home_away TEXT NOT NULL,
            g REAL, gc REAL, cs REAL, method TEXT,
            UNIQUE(timestamp, gw, team)
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS projections_fixtures (
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT    NOT NULL,
            gw        INTEGER NOT NULL,
            home_team TEXT    NOT NULL,
            away_team TEXT    NOT NULL,
            home_g    REAL,
            away_g    REAL,
            home_cs   REAL,
            away_cs   REAL,
            method    TEXT,
            UNIQUE(timestamp, gw, home_team, away_team)
        )
    """)

    c.execute("""
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
    """)
    c.execute("CREATE INDEX IF NOT EXISTS idx_ppm_ts_gw   ON player_projection_model (timestamp, gw)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_ppm_name    ON player_projection_model (name, team)")

    conn.commit()
    conn.close()


# ── Team projection helpers ───────────────────────────────────────────────────

def insert_fixture_projections(db_path, timestamp, fixtures_by_gw):
    rows = []
    for gw, df_fix in fixtures_by_gw.items():
        gw = int(gw)
        for _, r in df_fix.iterrows():
            rows.append((timestamp, gw,
                         r['Home'], r['Away'],
                         float(r['home_g']), float(r['away_g']),
                         float(r['home_cs']), float(r['away_cs']),
                         r.get('Method', 'Unknown')))
    conn = sqlite3.connect(db_path)
    conn.executemany("""
        INSERT OR IGNORE INTO projections_fixtures
        (timestamp, gw, home_team, away_team, home_g, away_g, home_cs, away_cs, method)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, rows)
    conn.commit()
    conn.close()
    return len(rows)


def insert_team_projections(db_path, timestamp, projections_by_gw, df_fixtures):
    conn = sqlite3.connect(db_path)
    rows = []
    for gw, df_proj in projections_by_gw.items():
        gw = int(gw)
        gw_fix = df_fixtures[df_fixtures['GW'] == gw]
        for _, row in df_proj.iterrows():
            team   = row['Team']
            h_fix  = gw_fix[gw_fix['Home'] == team]
            a_fix  = gw_fix[gw_fix['Away'] == team]
            if len(h_fix) > 0:
                opp, ha = h_fix.iloc[0]['Away'], 'H'
            elif len(a_fix) > 0:
                opp, ha = a_fix.iloc[0]['Home'], 'A'
            else:
                continue
            rows.append((timestamp, gw, team, opp, ha,
                         float(row['G']), float(row['GC']), float(row['CS']),
                         row.get('Method', 'Unknown')))

    conn.executemany("""
        INSERT OR IGNORE INTO projections
        (timestamp, gw, team, opponent, home_away, g, gc, cs, method)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, rows)
    conn.commit()
    conn.close()
    return len(rows)


# ── Player projection helpers ─────────────────────────────────────────────────

def get_recent_player_form(db_path, n_gws=5):
    """Return per-player xG/xA rates from the last N finished GWs."""
    try:
        with sqlite3.connect(str(db_path)) as conn:
            recent_gws = [r[0] for r in conn.execute(
                "SELECT gw FROM fpl_gw_events WHERE finished=1 ORDER BY gw DESC LIMIT ?",
                (n_gws,)
            ).fetchall()]
            if not recent_gws:
                return pd.DataFrame()
            placeholders = ','.join('?' for _ in recent_gws)
            df = pd.read_sql(f"""
                SELECT s.element_id,
                       SUM(s.minutes)                      AS recent_mins,
                       SUM(CAST(s.expected_goals   AS REAL)) AS recent_xg,
                       SUM(CAST(s.expected_assists AS REAL)) AS recent_xa
                FROM fpl_player_gw_stats s
                WHERE s.gw IN ({placeholders})
                GROUP BY s.element_id
                HAVING SUM(s.minutes) >= 90
            """, conn, params=recent_gws)
        df["recent_xg_p90"] = (df["recent_xg"] / df["recent_mins"].clip(lower=1)) * 90
        df["recent_xa_p90"] = (df["recent_xa"] / df["recent_mins"].clip(lower=1)) * 90
        return df[["element_id", "recent_xg_p90", "recent_xa_p90"]]
    except Exception as e:
        print(f"  Warning: recent form query failed ({e})")
        return pd.DataFrame()


def fetch_fpl_bootstrap():
    r = requests.get(FPL_BOOTSTRAP_URL, timeout=30,
                     headers={"User-Agent": "FPL-Model/1.0"})
    r.raise_for_status()
    return r.json()


def _load_set_pieces():
    path = ROOT / "config" / "set_pieces.json"
    try:
        data = json.loads(path.read_text())
        return (
            {(t, p) for t, p in data.get("penalty_takers", {}).items()},
            {(t, p) for t, p in data.get("corner_takers",  {}).items()},
        )
    except Exception:
        return set(), set()


def build_player_df(fpl_data):
    team_map = {t["id"]: to_short(t["name"], ODDS_TO_SHORT)
                for t in fpl_data.get("teams", [])}
    pen_pairs, corner_pairs = _load_set_pieces()
    rows = []
    for p in fpl_data.get("elements", []):
        if p.get("status") == "u":
            continue
        team_dc = team_map.get(p["team"])
        if not team_dc:
            continue

        status = p.get("status", "a")
        cop_pct = p.get("chance_of_playing_next_round")
        if cop_pct is not None:
            chance = float(cop_pct) / 100.0
        elif status == "a":
            chance = 1.0
        elif status == "d":
            chance = 0.50   # doubt with no % given
        else:
            chance = 0.0    # injured / suspended / not available

        name = p["web_name"]
        rows.append({
            "fpl_id":             p["id"],
            "Name":               name,
            "Team":               team_dc,
            "Pos":                POS_MAP.get(p["element_type"], "M"),
            "BV":                 p["now_cost"] / 10.0,
            "xMins":              0.0,
            "status":             status,
            "chance_of_playing":  chance,
            "season_mins":        p.get("minutes", 0),
            "season_goals":       p.get("goals_scored", 0),
            "season_assists":     p.get("assists", 0),
            "season_xg":          float(p.get("expected_goals")  or 0),
            "season_xa":          float(p.get("expected_assists") or 0),
            "pen_taker":          (team_dc, name) in pen_pairs,
            "corner_taker":       (team_dc, name) in corner_pairs,
        })
    return pd.DataFrame(rows)


def build_fixture_counts(df_fixtures, target_gws):
    counts = {}
    for gw in target_gws:
        for _, row in df_fixtures[df_fixtures["GW"] == gw].iterrows():
            for team in (row["Home"], row["Away"]):
                counts[(gw, team)] = counts.get((gw, team), 0) + 1
    return counts


def insert_player_projections(db_path, timestamp, df):
    rows = []
    for _, r in df.iterrows():
        rows.append({
            "timestamp":      timestamp,
            "fpl_id":         int(r["fpl_id"])        if pd.notna(r.get("fpl_id"))        else None,
            "name":           str(r["Name"]),
            "team":           str(r["Team"]),
            "pos":            str(r["Pos"]),
            "bv":             float(r["BV"])           if pd.notna(r.get("BV"))            else None,
            "gw":             int(r["GW"]),
            "xmins":          float(r["xMins"])        if pd.notna(r.get("xMins"))         else None,
            "xpts":           float(r["model_xPts"])   if pd.notna(r.get("model_xPts"))    else None,
            "goal_prob":      float(r["goal_prob"])    if pd.notna(r.get("goal_prob"))     else None,
            "assist_prob":    float(r["assist_prob"])  if pd.notna(r.get("assist_prob"))   else None,
            "cs_prob":        float(r["cs_prob"])      if pd.notna(r.get("cs_prob"))       else None,
            "bonus_prob":     float(r["bonus_prob"])   if pd.notna(r.get("bonus_prob"))    else None,
            "appearance_pts": float(r["appearance_pts"]) if pd.notna(r.get("appearance_pts")) else None,
            "save_pts":       float(r["save_pts"])     if pd.notna(r.get("save_pts"))      else None,
        })

    conn = sqlite3.connect(db_path)
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
    conn.close()
    return len(rows)


# ── Main ──────────────────────────────────────────────────────────────────────

def poll_and_store():
    db_path     = ROOT / 'outputs' / 'projections_history.db'
    config_path = ROOT / 'config' / 'config.json'

    init_db(db_path)
    config    = json.loads(config_path.read_text())
    season    = config.get('season', '2526')
    timestamp = datetime.now().isoformat(timespec='seconds')

    print(f"\n[{timestamp}] Starting projection poll...")

    try:
        # ── 1. Season results + DC ratings (shared) ───────────────────────
        results, all_teams, team_idx, avg_home, avg_away = load_season_results(season)

        ratings, home_adv = fit_dc_ratings(
            config.get('xi', 0.002),
            config.get('dc_rho', -0.073),
            results, all_teams, team_idx, avg_home, avg_away,
            ah_weight=config.get('ah_weight', 0.0),
        )

        # ── 2. FPL bootstrap (fetched once, used for deadlines + players) ───
        print("  Fetching FPL bootstrap-static...")
        fpl_data = fetch_fpl_bootstrap()

        # Build GW deadline lookup {gw_id: utc_datetime}.
        # FPL stores deadline_time in UTC (e.g. "2026-05-09T10:00:00Z").
        # We use this as the cutoff — stop projecting only after the deadline
        # passes, not after the first kickoff (which may be hours earlier).
        gw_deadlines = {}
        for event in fpl_data.get("events", []):
            dl = event.get("deadline_time")
            if dl:
                gw_deadlines[event["id"]] = pd.Timestamp(dl).tz_localize(None)

        # ── 3. Fixtures + odds (shared) ───────────────────────────────────
        df_target, target_gws = load_fixtures(ROOT / 'fixtures')

        now = pd.Timestamp.utcnow().tz_localize(None)
        live_gws = set()
        for gw in target_gws:
            gw_int = int(gw)
            deadline = gw_deadlines.get(gw_int)
            if deadline is not None:
                if deadline <= now:
                    live_gws.add(gw_int)
            elif 'Kickoff_Date' in df_target.columns:
                # Fallback: use first kickoff if no deadline found
                df_target['Kickoff_Date'] = pd.to_datetime(df_target['Kickoff_Date'], errors='coerce')
                min_ko = df_target[df_target['GW'] == gw]['Kickoff_Date'].min()
                if pd.notna(min_ko) and min_ko <= now:
                    live_gws.add(gw_int)

        if live_gws:
            print(f"  Skipping GW(s) past deadline: {sorted(live_gws)}")
            target_gws = [g for g in target_gws if int(g) not in live_gws]
            df_target  = df_target[~df_target['GW'].isin(live_gws)]

        if not target_gws:
            print(f"[{timestamp}] No upcoming GWs to project — exiting.")
            return

        try:
            odds_lookup = fetch_odds(config.get('odds_api_key'))
        except Exception as e:
            print(f"  Odds fetch failed ({e}), using ratings only")
            odds_lookup = {}

        # ── 4. Team projections ───────────────────────────────────────────
        projections, fixtures_by_gw = run_projections(
            ratings, home_adv,
            config.get('shrinkage_weight', 0.9357),
            config.get('blend_weight', 0.9322),
            config.get('dc_rho', -0.073),
            df_target, list(target_gws),
            odds_lookup, avg_home, avg_away,
            return_fixtures=True,
        )

        n_team = insert_team_projections(db_path, timestamp, projections, df_target)
        print(f"  [team]   stored {n_team} rows")

        n_fix = insert_fixture_projections(db_path, timestamp, fixtures_by_gw)
        print(f"  [fix]    stored {n_fix} fixture rows")

        # ── 5. Player projections (reuse bootstrap + team projections) ────
        df_players = build_player_df(fpl_data)
        print(f"  Players loaded: {len(df_players)} (bootstrap already fetched)")

        recent_form = get_recent_player_form(db_path)
        if not recent_form.empty:
            df_players = df_players.merge(
                recent_form, left_on="fpl_id", right_on="element_id", how="left"
            ).drop(columns=["element_id"], errors="ignore")
            n_form = int(df_players["recent_xg_p90"].notna().sum())
            print(f"  Recent form: {n_form} players with last-5-GW xG/xA")

        fixture_counts = build_fixture_counts(df_target, target_gws)

        gw_frames = [df_players.assign(GW=gw) for gw in target_gws]
        df_all    = pd.concat(gw_frames, ignore_index=True)

        # GWs elapsed = first upcoming GW − 1 (e.g. first upcoming=36 → 35 played)
        gws_elapsed = max(int(min(target_gws)) - 1, 1)
        league_avg_g = (avg_home + avg_away) / 2.0
        print(f"  gws_elapsed={gws_elapsed}, league_avg_g={league_avg_g:.3f}")

        df_result = estimate_player_xpts(
            df_all, projections, fixture_counts,
            use_source_minutes=False,
            gws_elapsed=gws_elapsed,
            league_avg_g=league_avg_g,
        )
        df_result = df_result[df_result["xMins"] > 0]

        n_player = insert_player_projections(db_path, timestamp, df_result)
        print(f"  [player] stored {n_player} rows  ({len(df_players)} players × {len(target_gws)} GWs)")

        print(f"[{timestamp}] [OK] Done — 1 odds call, 1 DC fit")

    except Exception as e:
        print(f"[{timestamp}] [ERROR] {e}")
        raise


if __name__ == '__main__':
    poll_and_store()
