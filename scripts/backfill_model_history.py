"""
Backfill player model projections for GW1-34 (one-off exercise).

Inputs per GW:
  - Solio team G/GC/CS projections  → proxy for DC model team-level output
  - Cumulative season stats to that GW → rate-based goal/assist probability
  - Current player BV + position   → minute estimation (approximation)

Output: rows in player_projection_model with timestamp = 'backfill'.
Re-running is safe: already-backfilled GWs are skipped.
"""
import sys
import sqlite3
import pandas as pd
import numpy as np
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))

from utils.player_model import estimate_player_xpts

DB_PATH      = ROOT / "outputs" / "projections_history.db"
FIXTURE_PATH = ROOT / "fixtures" / "fixtures_all.csv"
TIMESTAMP    = "backfill"
LEAGUE_AVG_G = 1.38


def main():
    print("Loading base data…")
    with sqlite3.connect(str(DB_PATH)) as conn:

        players_base = pd.read_sql("""
            SELECT p.element_id,
                   p.web_name   AS name,
                   CASE p.element_type
                       WHEN 1 THEN 'G' WHEN 2 THEN 'D'
                       WHEN 3 THEN 'M' WHEN 4 THEN 'F'
                   END AS pos,
                   p.now_cost / 10.0 AS bv,
                   t.short_name AS team
            FROM fpl_players p
            LEFT JOIN fpl_teams t ON p.team_id = t.team_id
            WHERE t.short_name IS NOT NULL
        """, conn)

        all_stats = pd.read_sql("""
            SELECT element_id, gw, minutes, goals_scored, assists
            FROM fpl_player_gw_stats
            ORDER BY element_id, gw
        """, conn)

        solio_teams = pd.read_sql("""
            WITH latest AS (
                SELECT team, gw, MAX(ingested_at) AS max_ia
                FROM solio_fixture_snapshots
                GROUP BY team, gw
            )
            SELECT s.team AS Team, s.gw,
                   s.g  AS G,
                   s.gc AS GC,
                   s.cs AS CS
            FROM solio_fixture_snapshots s
            JOIN latest l ON s.team=l.team AND s.gw=l.gw AND s.ingested_at=l.max_ia
            WHERE s.g IS NOT NULL
        """, conn)

        done_gws = {r[0] for r in conn.execute(
            "SELECT DISTINCT gw FROM player_projection_model WHERE timestamp = ?",
            (TIMESTAMP,)
        ).fetchall()}

    # Fixture counts per (gw, team) for DGW detection
    fix_counts = {}
    try:
        df_fix = pd.read_csv(str(FIXTURE_PATH))
        for _, row in df_fix.iterrows():
            gw = int(row["GW"])
            for team in [str(row["Home"]).strip(), str(row["Away"]).strip()]:
                fix_counts[(gw, team)] = fix_counts.get((gw, team), 0) + 1
        print(f"Loaded fixture counts for {len(fix_counts)} team-GW pairs.")
    except Exception as e:
        print(f"Warning: fixture file not loaded ({e}) — defaulting to 1 per team.")

    target_gws = sorted(solio_teams["gw"].unique())
    print(f"Solio team data covers GW{target_gws[0]}–GW{target_gws[-1]} ({len(target_gws)} GWs)")
    print(f"Already backfilled: {sorted(done_gws) or 'none'}\n")

    rows_inserted = 0

    with sqlite3.connect(str(DB_PATH)) as conn:
        for gw in target_gws:
            if gw in done_gws:
                print(f"GW{gw:02d}: skip (already done)")
                continue

            gws_elapsed = max(gw - 1, 0)

            # Cumulative stats from all previous GWs
            prev = (
                all_stats[all_stats["gw"] < gw]
                .groupby("element_id", as_index=False)
                .agg(season_mins=("minutes", "sum"),
                     season_goals=("goals_scored", "sum"),
                     season_assists=("assists", "sum"))
            )

            # Build player frame
            df = players_base.merge(prev, on="element_id", how="left")
            df["season_mins"]    = df["season_mins"].fillna(0.0)
            df["season_goals"]   = df["season_goals"].fillna(0.0)
            df["season_assists"] = df["season_assists"].fillna(0.0)
            df["status"]         = "a"
            df["GW"]             = gw
            df["xMins"]          = 0.0
            df["Pos"]            = df["pos"]
            df["BV"]             = df["bv"]
            df["Team"]           = df["team"]

            gw_teams = solio_teams[solio_teams["gw"] == gw].copy()
            if gw_teams.empty:
                print(f"GW{gw:02d}: no Solio team data — skip")
                continue

            team_proj_dict = {gw: gw_teams[["Team", "G", "GC", "CS"]]}
            fixture_counts_dict = {
                (gw, team): fix_counts.get((gw, team), 1)
                for team in df["Team"].unique()
            }

            result = estimate_player_xpts(
                df,
                team_proj_dict,
                fixture_counts=fixture_counts_dict,
                use_source_minutes=False,
                gws_elapsed=gws_elapsed,
                league_avg_g=LEAGUE_AVG_G,
            )

            # Map back to DB schema
            out = result[[
                "element_id", "name", "team", "pos", "bv", "GW",
                "xMins", "model_xPts",
                "goal_prob", "assist_prob", "cs_prob", "bonus_prob",
                "appearance_pts", "save_pts",
            ]].copy()
            out = out.rename(columns={
                "element_id": "fpl_id",
                "GW":         "gw",
                "xMins":      "xmins",
                "model_xPts": "xpts",
            })
            out["timestamp"] = TIMESTAMP
            out = out[out["xmins"] > 0].reset_index(drop=True)

            out.to_sql("player_projection_model", conn, if_exists="append", index=False)
            rows_inserted += len(out)
            print(f"GW{gw:02d}: {len(out):4d} rows  (gws_elapsed={gws_elapsed}, "
                  f"solio teams={len(gw_teams)})")

    print(f"\nBackfill complete — {rows_inserted:,} rows across {len(target_gws)} GWs.")


if __name__ == "__main__":
    main()
