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

from utils.data_loaders import load_season_results, fetch_odds, load_fixtures, load_all_solio
from utils.dc_model import fit_dc_ratings, run_projections, score_projections

warnings.filterwarnings('ignore')
pd.set_option('future.no_silent_downcasting', True)

DRIVE_ROOT  = r'G:\My Drive\FPL_Model'
CONFIG_PATH = f'{DRIVE_ROOT}/config/config.json'
FIXTURE_DIR = f'{DRIVE_ROOT}/fixtures'
SOLIO_DIR   = f'{DRIVE_ROOT}/solio'
OUTPUT_DIR  = f'{DRIVE_ROOT}/outputs'
os.makedirs(OUTPUT_DIR, exist_ok=True)

with open(CONFIG_PATH, 'r') as f:
    config = json.load(f)

ODDS_API_KEY = config['odds_api_key']
SEASON       = config.get('season', '2526')

BASELINE_XI        = config.get('xi', 0.002)
BASELINE_RHO       = config.get('dc_rho', -0.078)
BASELINE_SHRINKAGE = config.get('shrinkage_weight', 0.9373)
BASELINE_BLEND     = config.get('blend_weight', 0.924)
BASELINE_AH        = config.get('ah_weight', 0.0)

N_SAMPLES   = 500
RANDOM_SEED = 42
N_JOBS      = 8

BENCHMARK_GWS = [29, 30, 31, 32, 33, 35, 36, 37, 38]

def make_search_space(xi, rho, shrink, blend, ah):
    return {
        'xi':               (max(0.001, xi - 0.005),   min(0.020, xi + 0.005)),
        'dc_rho':           (max(-0.25, rho - 0.06),   min(-0.01, rho + 0.06)),
        'shrinkage_weight': (max(0.60, shrink - 0.10), min(0.95, shrink + 0.10)),
        'blend_weight':     (max(0.35, blend - 0.15),  min(0.99, blend + 0.15)),
        'ah_weight':        (0.0, 0.8), 
    }

PARAM_SPACE = make_search_space(
    BASELINE_XI, BASELINE_RHO, BASELINE_SHRINKAGE, BASELINE_BLEND, BASELINE_AH
)

print('=== MONTE CARLO OPTIMISER V4 (MULTI-GW BENCHMARK) ===')
print(f'  Samples       : {N_SAMPLES}')
print(f'  Workers       : {N_JOBS}')
print(f'  Season        : {SEASON}')
print(f'  Benchmark GWs : {BENCHMARK_GWS}')
print('\n  Baseline params (from config):')
print(f'    xi               = {BASELINE_XI}')
print(f'    dc_rho           = {BASELINE_RHO}')
print(f'    shrinkage_weight = {BASELINE_SHRINKAGE}')
print(f'    blend_weight     = {BASELINE_BLEND}')
print(f'    ah_weight        = {BASELINE_AH}')
print('\n  Search space:')
for k, (lo, hi) in PARAM_SPACE.items():
    print(f'    {k:<22}: [{lo:.4f}, {hi:.4f}]')

print('\n=== LOADING DATA ===')
df_results, all_teams, team_idx, LEAGUE_AVG_HOME, LEAGUE_AVG_AWAY = load_season_results(SEASON)

print('\n=== FETCHING ODDS ===')
odds_lookup = fetch_odds(
    ODDS_API_KEY,
    bookmakers=config.get('odds_bookmakers', ['pinnacle']),
)

print('\n=== LOADING UPCOMING FIXTURES ===')
df_target, TARGET_GWS = load_fixtures(FIXTURE_DIR)

print('\n=== LOADING SOLIO BENCHMARK DATA ===')
solio_by_gw = load_all_solio(SOLIO_DIR, BENCHMARK_GWS)
print(f'\n  Total benchmark GWs loaded: {len(solio_by_gw)} / {len(BENCHMARK_GWS)}')

# Load all fixtures for benchmark scoring
df_all_fix = pd.read_csv(f'{FIXTURE_DIR}/fixtures_all.csv')
df_all_fix['GW']   = pd.to_numeric(df_all_fix['GW'], errors='coerce')
df_all_fix['Home'] = df_all_fix['Home'].str.strip()
df_all_fix['Away'] = df_all_fix['Away'].str.strip()
df_benchmark = df_all_fix[df_all_fix['GW'].isin(BENCHMARK_GWS)].copy()

print(f'\nFitting baseline (xi={BASELINE_XI}, rho={BASELINE_RHO}, ah={BASELINE_AH})...')
b_ratings, b_home_adv = fit_dc_ratings(
    BASELINE_XI, BASELINE_RHO, df_results, all_teams, team_idx,
    LEAGUE_AVG_HOME, LEAGUE_AVG_AWAY, ah_weight=BASELINE_AH)
b_proj  = run_projections(
    b_ratings, b_home_adv, BASELINE_SHRINKAGE, BASELINE_BLEND, BASELINE_RHO,
    df_benchmark, BENCHMARK_GWS, odds_lookup, LEAGUE_AVG_HOME, LEAGUE_AVG_AWAY)
b_score = score_projections(b_proj, solio_by_gw)
print(f"  Baseline combined score : {b_score['combined_score']:.4f}")
print(f"  Blend   GD MAE          : {b_score['blend_mae']['GD']:.4f} (n={b_score['blend_n']})")
print(f"  Ratings GD MAE          : {b_score['ratings_mae']['GD']:.4f} (n={b_score['ratings_n']})")

np.random.seed(RANDOM_SEED)
samples = [
    {k: np.random.uniform(lo, hi) for k, (lo, hi) in PARAM_SPACE.items()}
    for _ in range(N_SAMPLES)
]

unique_pairs = list(set(
    (round(s['xi'], 3), round(s['dc_rho'], 3), round(s['ah_weight'], 2))
    for s in samples
))
print(f'\n  {N_SAMPLES} samples -> {len(unique_pairs)} unique (xi, rho, ah) pairs to fit')

print(f'\n=== STAGE 1: FITTING {len(unique_pairs)} RATING SETS ({N_JOBS} workers) ===')
t1 = time.time()

def fit_pair(xi, rho, ah):
    ratings, home_adv = fit_dc_ratings(
        xi, rho, df_results, all_teams, team_idx,
        LEAGUE_AVG_HOME, LEAGUE_AVG_AWAY, ah_weight=ah)
    return (xi, rho, ah), (ratings, home_adv)

fitted = Parallel(n_jobs=N_JOBS, verbose=5)(
    delayed(fit_pair)(xi, rho, ah) for xi, rho, ah in unique_pairs
)
ratings_cache = {key: val for key, val in fitted}
t1_elapsed = time.time() - t1
print(f'\nStage 1 done in {t1_elapsed:.1f}s  ({len(ratings_cache)} rating sets cached)')

print(f'\n=== STAGE 2: SCORING {N_SAMPLES} SAMPLES ({N_JOBS} workers) ===')
t2 = time.time()

def score_sample(params, ratings_cache):
    xi  = round(params['xi'], 3)
    rho = round(params['dc_rho'], 3)
    sw  = params['shrinkage_weight']
    bw  = params['blend_weight']
    ah  = round(params['ah_weight'], 2)

    ratings, home_adv = ratings_cache[(xi, rho, ah)]
    proj   = run_projections(
        ratings, home_adv, sw, bw, rho,
        df_benchmark, BENCHMARK_GWS, odds_lookup,
        LEAGUE_AVG_HOME, LEAGUE_AVG_AWAY)
    scores = score_projections(proj, solio_by_gw)

    return {
        'xi':               xi,
        'dc_rho':           rho,
        'shrinkage_weight': round(sw, 4),
        'blend_weight':     round(bw, 4),
        'ah_weight':        ah,
        'combined_score':   scores['combined_score'],
        'blend_score':      scores['blend_score'],
        'ratings_score':    scores['ratings_score'],
        'blend_GD_mae':     scores['blend_mae']['GD'],
        'blend_G_mae':      scores['blend_mae']['G'],
        'blend_CS_mae':     scores['blend_mae']['CS'],
        'ratings_GD_mae':   scores['ratings_mae']['GD'],
        'ratings_G_mae':    scores['ratings_mae']['G'],
        'ratings_CS_mae':   scores['ratings_mae']['CS'],
        'blend_n':          scores['blend_n'],
        'ratings_n':        scores['ratings_n'],
    }

results_all = Parallel(n_jobs=N_JOBS, verbose=10)(
    delayed(score_sample)(params, ratings_cache) for params in samples
)
t2_elapsed = time.time() - t2
elapsed_total = t1_elapsed + t2_elapsed
print(f'\nStage 2 done in {t2_elapsed:.1f}s')
print(f'Total time: {elapsed_total:.1f}s  (Stage 1: {t1_elapsed:.1f}s | Stage 2: {t2_elapsed:.1f}s)')

df_mc = pd.DataFrame(results_all)
df_mc = df_mc.dropna(subset=['combined_score'])
df_mc = df_mc.sort_values('combined_score').reset_index(drop=True)

print('\n' + '='*60)
print('TOP 15 PARAMETER COMBINATIONS')
print('='*60)
print(f"  {'Rank':>4} {'xi':>7} {'rho':>7} {'shrink':>8} {'blend':>7} {'ah':>6} "
      f"{'combined':>10} {'Blend_GD':>10} {'Rat_GD':>8}")
print('  ' + '-'*80)
for i, row in df_mc.head(15).iterrows():
    marker = ' <- BEST' if i == 0 else ''
    print(f"  {i+1:>4} {row['xi']:>7.4f} {row['dc_rho']:>7.4f} "
          f"{row['shrinkage_weight']:>8.4f} {row['blend_weight']:>7.4f} "
          f"{row['ah_weight']:>6.2f} "
          f"{row['combined_score']:>10.4f} {row['blend_GD_mae']:>10.4f} "
          f"{row['ratings_GD_mae']:>8.4f}{marker}")

best = df_mc.iloc[0]
improvement = (b_score['combined_score'] - best['combined_score']) / b_score['combined_score'] * 100

print('\n' + '='*60)
print('OPTIMAL PARAMETERS')
print('='*60)
print(f"  XI               = {best['xi']:.4f}  (was {BASELINE_XI})")
print(f"  DC_RHO           = {best['dc_rho']:.4f}  (was {BASELINE_RHO})")
print(f"  SHRINKAGE_WEIGHT = {best['shrinkage_weight']:.4f}  (was {BASELINE_SHRINKAGE})")
print(f"  BLEND_WEIGHT     = {best['blend_weight']:.4f}  (was {BASELINE_BLEND})")
print(f"  AH_WEIGHT        = {best['ah_weight']:.4f}  (was {BASELINE_AH})")
print(f"  Combined score   = {best['combined_score']:.4f}  (baseline: {b_score['combined_score']:.4f})")
print(f"  Improvement      = {improvement:.1f}%")

print('\n' + '='*60)
print('PARAMETER SENSITIVITY')
print('='*60)
for param in PARAM_SPACE.keys():
    corr   = df_mc[param].corr(df_mc['combined_score'])
    top25  = df_mc.head(len(df_mc) // 4)
    p_mean = top25[param].mean()
    p_std  = top25[param].std()
    bar    = ('<' * int(abs(corr) * 20)) if corr < 0 else ('>' * int(abs(corr) * 20))
    print(f'  {param:<22}: corr={corr:>+.3f}  {bar}')
    print(f'  {"":22}  optimal range: {p_mean-p_std:.4f} - {p_mean+p_std:.4f}')

df_mc.to_csv(f'{OUTPUT_DIR}/monte_carlo_results.csv', index=False)
df_mc.head(20).to_csv(f'{OUTPUT_DIR}/monte_carlo_top20.csv', index=False)
print(f'\nResults saved to {OUTPUT_DIR}')

config_updated = dict(config)
config_updated['shrinkage_weight']   = round(float(best['shrinkage_weight']), 4)
config_updated['blend_weight']       = round(float(best['blend_weight']), 4)
config_updated['dc_rho']             = round(float(best['dc_rho']), 4)
config_updated['xi']                 = round(float(best['xi']), 4)
config_updated['ah_weight']          = round(float(best['ah_weight']), 4)
config_updated['mc_last_run']        = datetime.now().strftime('%Y-%m-%d %H:%M')
config_updated['mc_samples']         = N_SAMPLES
config_updated['mc_score']           = round(float(best['combined_score']), 6)
config_updated['mc_improvement_pct'] = round(improvement, 2)

with open(CONFIG_PATH, 'w') as f:
    json.dump(config_updated, f, indent=4)

print('config.json updated with optimal parameters')
print(f'\nTotal time: {elapsed_total:.1f}s')
