"""
Polling script: Fetch odds & projections every 2 hours, store in SQLite.
Run via Windows Task Scheduler with 2-hour interval.
"""
import sys
from pathlib import Path
import json
import sqlite3
from datetime import datetime
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'scripts'))

from utils.data_loaders import (
    load_season_results, fetch_odds, load_fixtures, load_all_solio
)
from utils.dc_model import fit_dc_ratings, run_projections

# === DATABASE SETUP ===
def init_db(db_path):
    """Create SQLite schema if not exists."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS projections (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            gw INTEGER NOT NULL,
            team TEXT NOT NULL,
            opponent TEXT NOT NULL,
            home_away TEXT NOT NULL,
            g REAL,
            gc REAL,
            cs REAL,
            method TEXT,
            UNIQUE(timestamp, gw, team)
        )
    """)
    conn.commit()
    conn.close()

def insert_projections(db_path, timestamp, projections_by_gw, df_fixtures):
    """Insert projection snapshot into SQLite."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    rows = []
    for gw, df_proj in projections_by_gw.items():
        gw = int(gw)  # Ensure integer
        gw_fixtures = df_fixtures[df_fixtures['GW'] == gw]
        
        for _, row in df_proj.iterrows():
            team = row['Team']
            g = float(row['G'])
            gc = float(row['GC'])
            cs = float(row['CS'])
            method = row.get('Method', 'Unknown')
            
            # Find opponent
            home_fixtures = gw_fixtures[gw_fixtures['Home'] == team]
            away_fixtures = gw_fixtures[gw_fixtures['Away'] == team]
            
            if len(home_fixtures) > 0:
                opponent = home_fixtures.iloc[0]['Away']
                home_away = 'H'
            elif len(away_fixtures) > 0:
                opponent = away_fixtures.iloc[0]['Home']
                home_away = 'A'
            else:
                continue
            
            rows.append((timestamp, gw, team, opponent, home_away, g, gc, cs, method))
    
    # Insert with conflict handling (ignore if already exists for this timestamp)
    cursor.executemany("""
        INSERT OR IGNORE INTO projections 
        (timestamp, gw, team, opponent, home_away, g, gc, cs, method)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, rows)
    
    conn.commit()
    conn.close()
    return len(rows)

# === MAIN POLLING LOGIC ===
def poll_and_store():
    """Fetch current odds, run projections, store in DB."""
    db_path = ROOT / 'outputs' / 'projections_history.db'
    config_path = ROOT / 'config' / 'config.json'
    
    init_db(db_path)
    config = json.loads(config_path.read_text())
    season = config.get('season', '2526')
    timestamp = datetime.now().isoformat(timespec='seconds')
    
    try:
        print(f"\n[{timestamp}] Starting projection poll...")
        
        # Load base data
        results, all_teams, team_idx, avg_home, avg_away = load_season_results(season)
        df_target, target_gws = load_fixtures(ROOT / 'fixtures')
        
        # Fit DC ratings
        ratings, home_adv = fit_dc_ratings(
            config.get('xi', 0.002),
            config.get('dc_rho', -0.073),
            results,
            all_teams,
            team_idx,
            avg_home,
            avg_away,
            ah_weight=config.get('ah_weight', 0.0),
        )
        
        # Fetch odds
        odds_lookup = fetch_odds(config.get('odds_api_key'))
        
        # Run projections for all upcoming GWs
        projections = run_projections(
            ratings, home_adv,
            config.get('shrinkage_weight', 0.9357),
            config.get('blend_weight', 0.9322),
            config.get('dc_rho', -0.073),
            df_target,
            list(target_gws),
            odds_lookup,
            avg_home,
            avg_away,
        )
        
        # Store in DB
        n_rows = insert_projections(db_path, timestamp, projections, df_target)
        print(f"[{timestamp}] [OK] Stored {n_rows} projections")
        
    except Exception as e:
        print(f"[{timestamp}] [ERROR] {e}")
        raise

if __name__ == '__main__':
    poll_and_store()
