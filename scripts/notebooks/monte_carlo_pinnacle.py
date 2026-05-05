"""
Monte Carlo optimization for Pinnacle-only odds.
Optimizes shrinkage_weight and blend_weight to minimize error against Solio.
"""
import sys
from pathlib import Path
import json
import itertools
import numpy as np
import pandas as pd
from joblib import Parallel, delayed

ROOT = Path('g:/My Drive/FPL_Model')
sys.path.insert(0, str(ROOT / 'scripts'))

from utils.data_loaders import load_season_results, fetch_odds, load_fixtures, load_all_solio
from utils.dc_model import fit_dc_ratings, run_projections, score_projections

config_path = ROOT / 'config' / 'config.json'
config = json.loads(config_path.read_text())
season = config.get('season', '2526')

print("=" * 70)
print("MONTE CARLO: Pinnacle-Only Optimization")
print("=" * 70)

# Load base data
print("\n[1/6] Loading season results...")
results, all_teams, team_idx, avg_home, avg_away = load_season_results(season)

print("[2/6] Loading fixtures...")
df_target, target_gws = load_fixtures(ROOT / 'fixtures')
first_two = list(target_gws[:2])
print(f"     Target GWs: {first_two}")

print("[3/6] Loading Solio benchmarks...")
solio_by_gw = load_all_solio(ROOT / 'solio', first_two)

print("[4/6] Fitting DC ratings...")
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

print("[5/6] Fetching Pinnacle odds...")
pin_odds = fetch_odds(config.get('odds_api_key'), bookmakers=['pinnacle'])

# Parameter grid
shrinkage_values = np.linspace(0.85, 0.98, 8)  # 8 values
blend_values = np.linspace(0.85, 0.98, 8)      # 8 values
param_grid = list(itertools.product(shrinkage_values, blend_values))
print(f"\n[6/6] Running {len(param_grid)} parameter combinations...")

def test_params(shrinkage_weight, blend_weight):
    """Test a single parameter combination."""
    try:
        proj = run_projections(
            ratings, home_adv,
            shrinkage_weight, blend_weight,
            config.get('dc_rho', -0.073),
            df_target[df_target['GW'].isin(first_two)],
            first_two,
            pin_odds,
            avg_home,
            avg_away,
        )
        scores = score_projections(proj, solio_by_gw)
        return {
            'shrinkage_weight': shrinkage_weight,
            'blend_weight': blend_weight,
            'combined_score': scores['combined_score'],
            'blend_score': scores['blend_score'],
            'ratings_score': scores['ratings_score'],
            'blend_mae_g': scores['blend_mae']['G'],
            'blend_mae_gc': scores['blend_mae']['GC'],
            'blend_mae_gd': scores['blend_mae']['GD'],
            'blend_mae_cs': scores['blend_mae']['CS'],
            'blend_n': scores['blend_n'],
        }
    except Exception as e:
        return None

results_list = Parallel(n_jobs=-1)(
    delayed(test_params)(s, b) for s, b in param_grid
)
results_list = [r for r in results_list if r is not None]

df_results = pd.DataFrame(results_list)
df_results = df_results.sort_values('combined_score')

print(f"\nCompleted {len(df_results)} successful runs.")
print("\n" + "=" * 70)
print("TOP 10 PINNACLE-OPTIMIZED PARAMETER SETS")
print("=" * 70)
print(df_results.head(10)[['shrinkage_weight', 'blend_weight', 'combined_score', 'blend_score', 'ratings_score']].to_string(index=False))

# Save results
output_file = ROOT / 'outputs' / 'monte_carlo_pinnacle_results.csv'
df_results.to_csv(output_file, index=False)
print(f"\nResults saved to: {output_file}")

# Compare vs current baseline (all-odds)
baseline_combined = config.get('baseline_combined_score', 0.05)  # approximate
best_pinnacle_score = df_results.iloc[0]['combined_score']
print("\n" + "=" * 70)
print("COMPARISON")
print("=" * 70)
print(f"Current all-odds baseline (approx): {baseline_combined:.6f}")
print(f"Best Pinnacle-only:                 {best_pinnacle_score:.6f}")
print(f"Delta (lower is better):            {best_pinnacle_score - baseline_combined:+.6f}")

if best_pinnacle_score < baseline_combined:
    print("\n✓ Pinnacle-optimized is BETTER than current all-odds baseline.")
else:
    print("\n✗ Pinnacle-optimized is WORSE than current all-odds baseline.")
