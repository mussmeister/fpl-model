"""
Extract market-inferred team ratings from GW1-2 odds.
Compare against DC historical and Solio for GW3+.
"""
import sys
from pathlib import Path
import json
import numpy as np
import pandas as pd
from scipy.optimize import minimize
from itertools import product

ROOT = Path('g:/My Drive/FPL_Model')
sys.path.insert(0, str(ROOT / 'scripts'))

from utils.data_loaders import (
    load_season_results, fetch_odds, load_fixtures, load_all_solio
)
from utils.dc_model import (
    fit_dc_ratings, run_projections, score_projections, 
    remove_margin, fit_odds_lambdas
)

config_path = ROOT / 'config' / 'config.json'
config = json.loads(config_path.read_text())
season = config.get('season', '2526')

print("=" * 80)
print("MARKET-INFERRED RATINGS vs HISTORICAL DC RATINGS")
print("=" * 80)

# Load base data
print("\n[1/8] Loading season results...")
results, all_teams, team_idx, avg_home, avg_away = load_season_results(season)

print("[2/8] Loading fixtures...")
df_target, target_gws = load_fixtures(ROOT / 'fixtures')
all_gws = list(target_gws)
early_gws = all_gws[:2]  # GW1-2 for market extraction
later_gws = all_gws[2:4] if len(all_gws) >= 4 else []

if len(later_gws) == 0:
    print("ERROR: Need at least 4 GWs. Only found:", all_gws)
    raise SystemExit

print(f"     Early GWs (market inference): {early_gws}")
print(f"     Later GWs (comparison): {later_gws}")

print("[3/8] Loading Solio benchmarks...")
solio_by_gw = load_all_solio(ROOT / 'solio', all_gws)

print("[4/8] Fitting DC historical ratings...")
ratings_dc, home_adv = fit_dc_ratings(
    config.get('xi', 0.002),
    config.get('dc_rho', -0.073),
    results,
    all_teams,
    team_idx,
    avg_home,
    avg_away,
    ah_weight=config.get('ah_weight', 0.0),
)

print("[5/8] Fetching odds for market inference...")
odds_lookup = fetch_odds(config.get('odds_api_key'))

# === EXTRACT MARKET-INFERRED RATINGS FROM GW1-2 ===
print("[6/8] Extracting market-inferred ratings from early GWs...")

def extract_market_ratings(early_gws_list, df_target_early, odds_lookup, dc_rho, all_teams, team_idx):
    """
    Reverse-engineer attack/defense ratings from early GW odds.
    Uses lambdas extracted from odds to solve for team ratings.
    """
    dc_rho_val = dc_rho
    
    # Collect lambdas per team
    team_lambdas = {t: {"attack_lambdas": [], "defence_lambdas": []} for t in all_teams}
    
    for _, fix in df_target_early.iterrows():
        home, away = fix["Home"], fix["Away"]
        odds = odds_lookup.get((home, away))
        
        if odds is None:
            continue
        
        # Extract probabilities
        ph, pd, pa = remove_margin(odds["home_odds"], odds["draw_odds"], odds["away_odds"])
        lh_odds, la_odds = fit_odds_lambdas(ph, pd, pa, dc_rho_val)
        
        # Store lambdas
        team_lambdas[home]["attack_lambdas"].append(lh_odds)
        team_lambdas[home]["defence_lambdas"].append(la_odds)
        team_lambdas[away]["attack_lambdas"].append(la_odds)
        team_lambdas[away]["defence_lambdas"].append(lh_odds)
    
    # Solve for attack/defense ratings
    # lh = exp(attack_h + def_a + ha) * avg_h
    # la = exp(attack_a + def_h) * avg_a
    # We'll use a simple average approach: derive rating from average lambdas
    
    ratings_market = {}
    for team in all_teams:
        attack_lam = np.mean(team_lambdas[team]["attack_lambdas"]) if team_lambdas[team]["attack_lambdas"] else avg_home
        def_lam = np.mean(team_lambdas[team]["defence_lambdas"]) if team_lambdas[team]["defence_lambdas"] else avg_away
        
        # Solve: attack_rating ≈ log(attack_lambda / avg_home)
        # Defence rating ≈ log(def_lambda / avg_away)
        attack_rating = np.log(max(attack_lam / avg_home, 0.1))
        defence_rating = np.log(max(def_lam / avg_away, 0.1))
        
        ratings_market[team] = {
            "attack": float(attack_rating),
            "defence": float(defence_rating),
            "attack_lambda": float(attack_lam),
            "defence_lambda": float(def_lam),
        }
    
    # Normalize to zero-mean for consistency
    attack_mean = np.mean([r["attack"] for r in ratings_market.values()])
    defence_mean = np.mean([r["defence"] for r in ratings_market.values()])
    
    for team in ratings_market:
        ratings_market[team]["attack"] -= attack_mean
        ratings_market[team]["defence"] -= defence_mean
    
    return ratings_market

df_early = df_target[df_target["GW"].isin(early_gws)]
ratings_market = extract_market_ratings(early_gws, df_early, odds_lookup, config.get('dc_rho', -0.073), all_teams, team_idx)

print(f"  Extracted market ratings for {len(ratings_market)} teams from {len(df_early)} fixtures")

# === GENERATE PROJECTIONS: DC vs MARKET ===
print("[7/8] Generating projections for GW3+...")

df_later = df_target[df_target["GW"].isin(later_gws)]

proj_dc = run_projections(
    ratings_dc, home_adv,
    config.get('shrinkage_weight', 0.9357),
    config.get('blend_weight', 0.9322),
    config.get('dc_rho', -0.073),
    df_later,
    later_gws,
    odds_lookup,
    avg_home,
    avg_away,
)

proj_market = run_projections(
    ratings_market, home_adv,
    config.get('shrinkage_weight', 0.9357),
    config.get('blend_weight', 0.9322),
    config.get('dc_rho', -0.073),
    df_later,
    later_gws,
    odds_lookup,
    avg_home,
    avg_away,
)

# === SCORE AGAINST SOLIO ===
print("[8/8] Scoring against Solio...")

scores_dc = score_projections(proj_dc, solio_by_gw)
scores_market = score_projections(proj_market, solio_by_gw)

print("\n" + "=" * 80)
print("RESULTS: DC HISTORICAL vs MARKET-INFERRED")
print("=" * 80)

print("\nDC HISTORICAL RATINGS (GW3+):")
print(f"  Combined Score: {scores_dc['combined_score']:.6f}")
print(f"  Blend MAE (G, GC, GD, CS): {scores_dc['blend_mae']}")
print(f"  Ratings MAE (G, GC, GD, CS): {scores_dc['ratings_mae']}")

print("\nMARKET-INFERRED RATINGS (GW3+):")
print(f"  Combined Score: {scores_market['combined_score']:.6f}")
print(f"  Blend MAE (G, GC, GD, CS): {scores_market['blend_mae']}")
print(f"  Ratings MAE (G, GC, GD, CS): {scores_market['ratings_mae']}")

delta = scores_market['combined_score'] - scores_dc['combined_score']
pct_change = (delta / scores_dc['combined_score'] * 100) if scores_dc['combined_score'] != 0 else 0

print("\nCOMPARISON:")
print(f"  Delta (Market - DC): {delta:+.6f}")
print(f"  Change: {pct_change:+.2f}%")

if delta < 0:
    print(f"  ✓ Market-inferred is BETTER by {abs(delta):.6f}")
else:
    print(f"  ✗ Market-inferred is WORSE by {abs(delta):.6f}")

# === SAVE DETAILED COMPARISON ===
comparison_rows = []
for gw in later_gws:
    if gw not in solio_by_gw:
        continue
    
    sol = solio_by_gw[gw]
    dc_proj = proj_dc[gw].merge(sol[['Team', 'Solio_G', 'Solio_GC', 'Solio_GD', 'Solio_CS']], on='Team', how='inner')
    mkt_proj = proj_market[gw].merge(sol[['Team', 'Solio_G', 'Solio_GC', 'Solio_GD', 'Solio_CS']], on='Team', how='inner')
    
    for _, row in dc_proj.iterrows():
        team = row['Team']
        mkt_row = mkt_proj[mkt_proj['Team'] == team].iloc[0] if len(mkt_proj[mkt_proj['Team'] == team]) > 0 else None
        
        if mkt_row is not None:
            comparison_rows.append({
                'GW': gw,
                'Team': team,
                'Solio_G': row['Solio_G'],
                'Solio_GC': row['Solio_GC'],
                'Solio_GD': row['Solio_GD'],
                'Solio_CS': row['Solio_CS'],
                'DC_G': row['G'],
                'DC_GC': row['GC'],
                'DC_GD': row['GD'],
                'DC_CS': row['CS'],
                'Market_G': mkt_row['G'],
                'Market_GC': mkt_row['GC'],
                'Market_GD': mkt_row['GD'],
                'Market_CS': mkt_row['CS'],
                'DC_Err_G': abs(row['G'] - row['Solio_G']),
                'Market_Err_G': abs(mkt_row['G'] - row['Solio_G']),
                'DC_Err_GD': abs(row['GD'] - row['Solio_GD']),
                'Market_Err_GD': abs(mkt_row['GD'] - row['Solio_GD']),
            })

df_comp = pd.DataFrame(comparison_rows)
output_file = ROOT / 'outputs' / 'market_vs_dc_comparison.csv'
df_comp.to_csv(output_file, index=False)
print(f"\nDetailed comparison saved: {output_file}")

print("\n" + "=" * 80)
