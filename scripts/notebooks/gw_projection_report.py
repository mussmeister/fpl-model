import sys
sys.path.append(r'G:\My Drive\FPL_Model\scripts')

import os
import json
import warnings
import numpy as np
import pandas as pd
from datetime import datetime

from utils.data_loaders import load_season_results, fetch_odds, load_fixtures, load_all_solio
from utils.dc_model import fit_dc_ratings, run_projections

warnings.filterwarnings('ignore')
pd.set_option('future.no_silent_downcasting', True)

# ============================================================
# CONFIG
# ============================================================

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
XI           = config.get('xi', 0.002)
RHO          = config.get('dc_rho', -0.073)
SHRINKAGE    = config.get('shrinkage_weight', 0.9357)
BLEND        = config.get('blend_weight', 0.9322)
AH           = config.get('ah_weight', 0.65)

REPORT_GWS   = [35, 36, 37, 38]

print('=== GW PROJECTION REPORT ===')
print(f'  Season  : {SEASON}')
print(f'  GWs     : {REPORT_GWS}')
print(f'  Params  : xi={XI}, rho={RHO}, shrink={SHRINKAGE}, blend={BLEND}, ah={AH}')
print(f'  Run at  : {datetime.now().strftime("%Y-%m-%d %H:%M")}')


# ============================================================
# LOAD DATA
# ============================================================

print('\n=== LOADING DATA ===')
df_results, all_teams, team_idx, LEAGUE_AVG_HOME, LEAGUE_AVG_AWAY = load_season_results(SEASON)

print('\n=== FETCHING ODDS ===')
odds_lookup = fetch_odds(
    ODDS_API_KEY,
    bookmakers=config.get('odds_bookmakers', ['pinnacle']),
)

print('\n=== LOADING FIXTURES ===')
df_all_fix = pd.read_csv(f'{FIXTURE_DIR}/fixtures_all.csv')
df_all_fix['GW']   = pd.to_numeric(df_all_fix['GW'], errors='coerce')
df_all_fix['Home'] = df_all_fix['Home'].str.strip()
df_all_fix['Away'] = df_all_fix['Away'].str.strip()
df_report = df_all_fix[df_all_fix['GW'].isin(REPORT_GWS)].copy()

print('\n=== LOADING SOLIO DATA ===')
solio_by_gw = load_all_solio(SOLIO_DIR, REPORT_GWS)


# ============================================================
# FIT RATINGS & GENERATE PROJECTIONS
# ============================================================

print(f'\nFitting DC ratings (xi={XI}, rho={RHO}, ah={AH})...')
ratings, home_adv = fit_dc_ratings(
    XI, RHO, df_results, all_teams, team_idx,
    LEAGUE_AVG_HOME, LEAGUE_AVG_AWAY, ah_weight=AH)

projections = run_projections(
    ratings, home_adv, SHRINKAGE, BLEND, RHO,
    df_report, REPORT_GWS, odds_lookup,
    LEAGUE_AVG_HOME, LEAGUE_AVG_AWAY)

print('  Done.')


# ============================================================
# BUILD COMPARISON REPORT
# ============================================================

all_rows = []

for GW in REPORT_GWS:
    if GW not in projections:
        print(f'  GW{GW}: no projections')
        continue

    df_proj = projections[GW].copy()
    df_proj['GW'] = GW

    if GW in solio_by_gw:
        df_solio = solio_by_gw[GW]
        df_comp  = df_proj.merge(df_solio[['Team','Solio_G','Solio_GC','Solio_GD','Solio_CS']],
                                 on='Team', how='left')
    else:
        df_comp = df_proj.copy()
        df_comp['Solio_G']  = np.nan
        df_comp['Solio_GC'] = np.nan
        df_comp['Solio_GD'] = np.nan
        df_comp['Solio_CS'] = np.nan

    all_rows.append(df_comp)

df_report_out = pd.concat(all_rows, ignore_index=True)

# Round model columns
for col in ['G', 'GC', 'GD', 'CS']:
    df_report_out[col] = df_report_out[col].round(2)

# Calculate differences where Solio data exists
df_report_out['Diff_G']  = (df_report_out['G']  - df_report_out['Solio_G']).round(2)
df_report_out['Diff_GC'] = (df_report_out['GC'] - df_report_out['Solio_GC']).round(2)
df_report_out['Diff_GD'] = (df_report_out['GD'] - df_report_out['Solio_GD']).round(2)
df_report_out['Diff_CS'] = (df_report_out['CS'] - df_report_out['Solio_CS']).round(2)

# Reorder columns
df_report_out = df_report_out[[
    'GW', 'Team', 'Method',
    'G', 'Solio_G', 'Diff_G',
    'GC', 'Solio_GC', 'Diff_GC',
    'GD', 'Solio_GD', 'Diff_GD',
    'CS', 'Solio_CS', 'Diff_CS',
]]


# ============================================================
# PRINT REPORT
# ============================================================

for GW in REPORT_GWS:
    gw_data = df_report_out[df_report_out['GW'] == GW].sort_values('GD', ascending=False)
    has_solio = gw_data['Solio_G'].notna().any()

    print(f'\n{"="*80}')
    print(f'  GW{GW}  |  Method: {gw_data["Method"].mode()[0]}')
    print(f'{"="*80}')

    if has_solio:
        print(f'  {"Team":<18} {"Mod_G":>6} {"Sol_G":>6} {"Dif_G":>6} | '
              f'{"Mod_GC":>6} {"Sol_GC":>6} {"Dif_GC":>6} | '
              f'{"Mod_GD":>6} {"Sol_GD":>6} {"Dif_GD":>6} | '
              f'{"Mod_CS":>6} {"Sol_CS":>6} {"Dif_CS":>6}')
        print(f'  {"-"*76}')
        for _, row in gw_data.iterrows():
            print(f'  {row["Team"]:<18} '
                  f'{row["G"]:>6.2f} {row["Solio_G"]:>6.2f} {row["Diff_G"]:>+6.2f} | '
                  f'{row["GC"]:>6.2f} {row["Solio_GC"]:>6.2f} {row["Diff_GC"]:>+6.2f} | '
                  f'{row["GD"]:>6.2f} {row["Solio_GD"]:>6.2f} {row["Diff_GD"]:>+6.2f} | '
                  f'{row["CS"]:>6.2f} {row["Solio_CS"]:>6.2f} {row["Diff_CS"]:>+6.2f}')

        # GW summary MAE
        mae_g  = gw_data['Diff_G'].abs().mean()
        mae_gc = gw_data['Diff_GC'].abs().mean()
        mae_gd = gw_data['Diff_GD'].abs().mean()
        mae_cs = gw_data['Diff_CS'].abs().mean()
        print(f'\n  MAE: G={mae_g:.3f}  GC={mae_gc:.3f}  GD={mae_gd:.3f}  CS={mae_cs:.3f}')
    else:
        print(f'  {"Team":<18} {"G":>6} {"GC":>6} {"GD":>6} {"CS":>6} {"Method":>8}')
        print(f'  {"-"*52}')
        for _, row in gw_data.iterrows():
            print(f'  {row["Team"]:<18} '
                  f'{row["G"]:>6.2f} {row["GC"]:>6.2f} {row["GD"]:>6.2f} '
                  f'{row["CS"]:>6.2f} {row["Method"]:>8}')
        print('  (No Solio data available for comparison)')


# ============================================================
# OVERALL SUMMARY
# ============================================================

df_comp_only = df_report_out.dropna(subset=['Solio_G'])
if len(df_comp_only) > 0:
    print(f'\n{"="*80}')
    print('  OVERALL SUMMARY (GW35-38 vs Solio)')
    print(f'{"="*80}')

    for method in ['Blend', 'Ratings', 'Mixed']:
        sub = df_comp_only[df_comp_only['Method'] == method]
        if len(sub) == 0: continue
        print(f'\n  {method} mode (n={len(sub)} team-GWs):')
        print(f'    G  MAE : {sub["Diff_G"].abs().mean():.3f}')
        print(f'    GC MAE : {sub["Diff_GC"].abs().mean():.3f}')
        print(f'    GD MAE : {sub["Diff_GD"].abs().mean():.3f}')
        print(f'    CS MAE : {sub["Diff_CS"].abs().mean():.3f}')

    print(f'\n  Overall (all methods):')
    print(f'    G  MAE : {df_comp_only["Diff_G"].abs().mean():.3f}')
    print(f'    GC MAE : {df_comp_only["Diff_GC"].abs().mean():.3f}')
    print(f'    GD MAE : {df_comp_only["Diff_GD"].abs().mean():.3f}')
    print(f'    CS MAE : {df_comp_only["Diff_CS"].abs().mean():.3f}')


# ============================================================
# SAVE CSV
# ============================================================

out_path = f'{OUTPUT_DIR}/gw_projection_report.csv'
df_report_out.to_csv(out_path, index=False)
print(f'\n  Report saved to: {out_path}')
