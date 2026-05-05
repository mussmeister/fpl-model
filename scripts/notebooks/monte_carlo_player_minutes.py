import sys
sys.path.append(r'G:\My Drive\FPL_Model\scripts')

import os
import json
import warnings
import numpy as np
import pandas as pd
import time
from datetime import datetime
from joblib import Parallel, delayed

from utils.data_loaders import load_fplreview_player_data, load_solio_player_data, melt_player_projections, load_fixtures
from utils.dc_model import fit_dc_ratings, run_projections
from utils.player_model import estimate_player_xpts

warnings.filterwarnings('ignore')
pd.set_option('future.no_silent_downcasting', True)

DRIVE_ROOT  = r'G:\My Drive\FPL_Model'
CONFIG_PATH = f'{DRIVE_ROOT}/config/config.json'
FIXTURE_DIR = f'{DRIVE_ROOT}/fixtures'
FPLREVIEW_DIR = f'{DRIVE_ROOT}/fplreview'
SOLIO_DIR   = f'{DRIVE_ROOT}/solio'
OUTPUT_DIR  = f'{DRIVE_ROOT}/outputs'
os.makedirs(OUTPUT_DIR, exist_ok=True)

with open(CONFIG_PATH, 'r') as f:
    config = json.load(f)

SEASON = config.get('season', '2526')

N_SAMPLES   = 200
RANDOM_SEED = 42
N_JOBS      = 4

TARGET_GWS = [35, 36, 37, 38]

# Load data once
print('=== LOADING DATA ===')
df_fpl = load_fplreview_player_data(FPLREVIEW_DIR)
df_sol = load_solio_player_data(SOLIO_DIR)
long_fpl = melt_player_projections(df_fpl)
long_sol = melt_player_projections(df_sol)
model_inputs = pd.concat([long_fpl, long_sol], ignore_index=True)
df_target, target_gws = load_fixtures(FIXTURE_DIR, n_gws=12)
model_inputs = model_inputs[model_inputs['GW'].isin(TARGET_GWS)].copy()
model_inputs = model_inputs.sort_values(['Name','Team','GW','xMins'], ascending=[True,True,True,False])
model_inputs = model_inputs.drop_duplicates(['Name','Team','GW'], keep='first')

# Build fixture counts
fixture_counts = pd.concat([
    df_target[["GW", "Home"]].rename(columns={"Home": "Team"}),
    df_target[["GW", "Away"]].rename(columns={"Away": "Team"}),
], ignore_index=True)
fixture_counts = (
    fixture_counts.groupby(["GW", "Team"]).size().rename("fixtures").to_dict()
)

# Load team projections (use baseline)
from utils.data_loaders import load_season_results, fetch_odds
df_results, all_teams, team_idx, LEAGUE_AVG_HOME, LEAGUE_AVG_AWAY = load_season_results(SEASON)
odds_lookup = fetch_odds(
    config.get('odds_api_key'),
    bookmakers=config.get('odds_bookmakers', ['pinnacle']),
)
ratings, home_adv = fit_dc_ratings(
    config.get('xi', 0.002), config.get('dc_rho', -0.073), df_results, all_teams, team_idx,
    LEAGUE_AVG_HOME, LEAGUE_AVG_AWAY, ah_weight=config.get('ah_weight', 0.0))
team_projections = run_projections(
    ratings, home_adv, config.get('shrinkage_weight', 0.9357), config.get('blend_weight', 0.9322), config.get('dc_rho', -0.073),
    df_target, TARGET_GWS, odds_lookup, LEAGUE_AVG_HOME, LEAGUE_AVG_AWAY)

# Load benchmarks for comparison
fplreview_stats = long_fpl[long_fpl['GW'].isin(TARGET_GWS)].copy()
solio_stats = long_sol[long_sol['GW'].isin(TARGET_GWS)].copy()
fplreview_stats = fplreview_stats.sort_values(['Name','Team','GW','xMins'], ascending=[True,True,True,False])
solio_stats = solio_stats.sort_values(['Name','Team','GW','xMins'], ascending=[True,True,True,False])
fplreview_stats = fplreview_stats.drop_duplicates(['Name','Team','GW'], keep='first')
solio_stats = solio_stats.drop_duplicates(['Name','Team','GW'], keep='first')

# Merge benchmarks
benchmarks = fplreview_stats[['Name','Team','GW','xMins']].rename(columns={'xMins':'fpl_xMins'}).merge(
    solio_stats[['Name','Team','GW','xMins']].rename(columns={'xMins':'sol_xMins'}), on=['Name','Team','GW'], how='outer')
benchmarks['target_xMins'] = benchmarks[['fpl_xMins','sol_xMins']].mean(axis=1).fillna(0)

print(f'Loaded {len(model_inputs)} player rows for GWs {TARGET_GWS}')

# Define parameter space for player minute model
# Vary multipliers for play probabilities by position
PARAM_SPACE = {
    'gk_mult': (0.5, 2.0),
    'def_mult': (0.5, 2.0),
    'mid_mult': (0.5, 2.0),
    'fwd_mult': (0.5, 2.0),
}

print('=== MONTE CARLO FOR PLAYER MINUTES ===')
print(f'  Samples: {N_SAMPLES}')
print(f'  Workers: {N_JOBS}')
print(f'  Target: Average of FPL and Solio xMins')
print('  Parameters:')
for k, (lo, hi) in PARAM_SPACE.items():
    print(f'    {k}: [{lo}, {hi}]')

np.random.seed(RANDOM_SEED)
samples = [
    {k: np.random.uniform(lo, hi) for k, (lo, hi) in PARAM_SPACE.items()}
    for _ in range(N_SAMPLES)
]

def score_sample(params):
    # Modify the player model temporarily
    import utils.player_model as pm
    original_prob = pm._estimate_play_probability

    def modified_prob(pos, bv):
        base_prob = original_prob(pos, bv)
        mult = {
            'G': params['gk_mult'],
            'D': params['def_mult'],
            'M': params['mid_mult'],
            'F': params['fwd_mult'],
        }.get(pos, 1.0)
        return base_prob * mult

    pm._estimate_play_probability = modified_prob

    try:
        model_players = estimate_player_xpts(
            model_inputs, team_projections, fixture_counts=fixture_counts, use_source_minutes=False)
        
        # Merge with benchmarks
        comp = model_players[['Name','Team','GW','xMins']].merge(
            benchmarks, on=['Name','Team','GW'], how='inner')
        comp['xMins'] = pd.to_numeric(comp['xMins'], errors='coerce').fillna(0)
        comp['target_xMins'] = pd.to_numeric(comp['target_xMins'], errors='coerce').fillna(0)
        
        # Compute MAE
        mae = (comp['xMins'] - comp['target_xMins']).abs().mean()
        
        return {
            'gk_mult': round(params['gk_mult'], 3),
            'def_mult': round(params['def_mult'], 3),
            'mid_mult': round(params['mid_mult'], 3),
            'fwd_mult': round(params['fwd_mult'], 3),
            'mae': mae,
        }
    finally:
        pm._estimate_play_probability = original_prob

print(f'\n=== RUNNING {N_SAMPLES} SAMPLES ===')
t1 = time.time()
results = Parallel(n_jobs=N_JOBS, verbose=5)(
    delayed(score_sample)(params) for params in samples
)
t1_elapsed = time.time() - t1
print(f'Done in {t1_elapsed:.1f}s')

df_mc = pd.DataFrame(results)
df_mc = df_mc.sort_values('mae').reset_index(drop=True)

print('\n' + '='*60)
print('TOP 10 PARAMETER COMBINATIONS')
print('='*60)
print(f"  {'Rank':>4} {'gk_mult':>8} {'def_mult':>9} {'mid_mult':>9} {'fwd_mult':>9} {'MAE':>8}")
print('  ' + '-'*60)
for i, row in df_mc.head(10).iterrows():
    marker = ' <- BEST' if i == 0 else ''
    print(f"  {i+1:>4} {row['gk_mult']:>8.3f} {row['def_mult']:>9.3f} "
          f"{row['mid_mult']:>9.3f} {row['fwd_mult']:>9.3f} "
          f"{row['mae']:>8.3f}{marker}")

best = df_mc.iloc[0]
print('\n' + '='*60)
print('OPTIMAL PARAMETERS')
print('='*60)
print(f"  GK Mult  = {best['gk_mult']:.3f}")
print(f"  DEF Mult = {best['def_mult']:.3f}")
print(f"  MID Mult = {best['mid_mult']:.3f}")
print(f"  FWD Mult = {best['fwd_mult']:.3f}")
print(f"  MAE      = {best['mae']:.3f}")

# Save results
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
output_path = f'{OUTPUT_DIR}/monte_carlo_player_minutes_{timestamp}.csv'
df_mc.to_csv(output_path, index=False)
print(f'\nSaved results to {output_path}')