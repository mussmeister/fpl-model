import sys
from pathlib import Path
import json
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.append(str(ROOT / 'scripts'))
from utils.data_loaders import load_season_results, fetch_odds, load_fixtures, load_all_solio
from utils.dc_model import fit_dc_ratings, run_projections

config_path = ROOT / 'config' / 'config.json'
config = json.loads(config_path.read_text())
season = config.get('season', '2526')

results, all_teams, team_idx, avg_home, avg_away = load_season_results(season)
df_target, target_gws = load_fixtures(ROOT / 'fixtures')
first_two = list(target_gws[:2])
if len(first_two) < 2:
    raise SystemExit('Need at least two upcoming GWs')

solio_by_gw = load_all_solio(ROOT / 'solio', first_two)
print('Target GWs:', first_two)
print('Loaded Solio GWs:', sorted(solio_by_gw.keys()))

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

all_odds = fetch_odds(config.get('odds_api_key'), bookmakers=None)
pin_odds = fetch_odds(config.get('odds_api_key'), bookmakers=['pinnacle'])

proj_all = run_projections(
    ratings, home_adv,
    config.get('shrinkage_weight', 0.9357),
    config.get('blend_weight', 0.9322),
    config.get('dc_rho', -0.073),
    df_target[df_target['GW'].isin(first_two)],
    first_two,
    all_odds,
    avg_home,
    avg_away,
)
proj_pin = run_projections(
    ratings, home_adv,
    config.get('shrinkage_weight', 0.9357),
    config.get('blend_weight', 0.9322),
    config.get('dc_rho', -0.073),
    df_target[df_target['GW'].isin(first_two)],
    first_two,
    pin_odds,
    avg_home,
    avg_away,
)

for gw in first_two:
    if gw not in solio_by_gw:
        print(f'GW {gw}: no Solio data')
        continue
    sol = solio_by_gw[gw][['Team', 'Solio_G', 'Solio_GC', 'Solio_GD', 'Solio_CS']]
    df_all = proj_all[gw].merge(sol, on='Team', how='inner')
    df_pin = proj_pin[gw].merge(sol, on='Team', how='inner')
    for label, df in [('all', df_all), ('pinnacle', df_pin)]:
        df[f'Abs_G_{label}'] = (df['G'] - df['Solio_G']).abs()
        df[f'Abs_GC_{label}'] = (df['GC'] - df['Solio_GC']).abs()
        df[f'Abs_GD_{label}'] = (df['GD'] - df['Solio_GD']).abs()
        df[f'Abs_CS_{label}'] = (df['CS'] - df['Solio_CS']).abs()
    mean_all = df_all[[f'Abs_G_all', f'Abs_GC_all', f'Abs_GD_all', f'Abs_CS_all']].mean()
    mean_pin = df_pin[[f'Abs_G_pin', f'Abs_GC_pin', f'Abs_GD_pin', f'Abs_CS_pin']].mean()
    diff = mean_pin - mean_all
    print(f'GW {gw}:')
    print('  All odds mean abs:', mean_all.to_dict())
    print('  Pinnacle mean abs:', mean_pin.to_dict())
    print('  Pinnacle - All:', diff.to_dict())
    print()
