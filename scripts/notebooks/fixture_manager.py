import sys
sys.path.append(r'G:\My Drive\FPL_Model\scripts')

# ============================================================
# FPL FIXTURE LIST MANAGER
# Local VS Code Script — Google Drive folder compatible
# ============================================================
# WHAT THIS DOES:
#   1. Pulls the full fixture list from the official FPL API
#   2. Applies manual GW overrides (e.g. postponed/rescheduled)
#   3. Flags DGW, BGW and unscheduled fixtures
#   4. Saves CSVs to your local FPL_Model/fixtures/ folder
#
# FILES SAVED TO FPL_Model/fixtures/:
#   fixtures_all.csv          — every fixture, full detail
#   fixtures_gw_summary.csv   — team x GW grid (like Solio ticker)
#   fixtures_overrides.csv    — override audit log
#   fixtures_gw_deadlines.csv — GW deadline times
#
# UPDATE EACH WEEK:
#   Add new overrides to OVERRIDES dict below and rerun
# ============================================================

import os
import json
import requests
import pandas as pd
from datetime import datetime, timezone


# ============================================================
# 1. CONFIG — LOCAL PATHS
# ============================================================

DRIVE_ROOT  = r'G:\My Drive\FPL_Model'
FIXTURE_DIR = f'{DRIVE_ROOT}/fixtures'
os.makedirs(FIXTURE_DIR, exist_ok=True)

print(f'Output folder: {FIXTURE_DIR}')


# ============================================================
# 2. MANUAL GW OVERRIDES
#    Add fixtures here that FPL hasn't assigned to a GW yet,
#    or that have been rescheduled to a different GW.
#
#    Format:
#    ("Home Team", "Away Team"): {
#        "event":  GW_NUMBER,
#        "reason": "why this override exists",
#    }
#
#    Team names must match FPL API names exactly.
#    Run the script once — it prints all team names at the top.
# ============================================================

OVERRIDES = {
    ("Manchester City", "Crystal Palace"): {
        "event":  36,
        "reason": "Postponed from GW31 (EFL Cup final clash). "
                  "Confirmed 28 Apr 2026 — rescheduled to Wed 13 May (GW36 midweek).",
    },
    # Template for future overrides — copy and fill in:
    # ("Home Team Name", "Away Team Name"): {
    #     "event":  37,
    #     "reason": "Reason for rescheduling",
    # },
}


# ============================================================
# 3. FETCH FPL DATA
# ============================================================

def get_bootstrap():
    url = 'https://fantasy.premierleague.com/api/bootstrap-static/'
    r = requests.get(url, timeout=15, headers={'User-Agent': 'Mozilla/5.0'})
    r.raise_for_status()
    return r.json()

def get_fixtures():
    url = 'https://fantasy.premierleague.com/api/fixtures/'
    r = requests.get(url, timeout=15, headers={'User-Agent': 'Mozilla/5.0'})
    r.raise_for_status()
    return r.json()

print('Fetching FPL data...')
bootstrap = get_bootstrap()
fixtures  = get_fixtures()

# Team ID -> name
teams = {t['id']: t['name'] for t in bootstrap['teams']}
print(f"Teams: {len(teams)}  |  Fixtures: {len(fixtures)}")

# GW metadata
gws = []
for e in bootstrap['events']:
    gws.append({
        'GW':         e['id'],
        'Name':       e['name'],
        'Deadline':   e['deadline_time'],
        'Is_Current': e['is_current'],
        'Is_Next':    e['is_next'],
        'Finished':   e['finished'],
    })
df_gws = pd.DataFrame(gws)

current_gw = next((g['GW'] for g in gws if g['Is_Current']), None)
next_gw    = next((g['GW'] for g in gws if g['Is_Next']),    None)
print(f'Current GW: {current_gw}  |  Next GW: {next_gw}')

# Print all team names so overrides can be copy-pasted accurately
print(f'\nAll team names (use these exactly in OVERRIDES):')
for tid, tname in sorted(teams.items(), key=lambda x: x[1]):
    print(f'  {tname}')


# ============================================================
# 4. BUILD FIXTURE DATAFRAME
# ============================================================

rows = []
for f in fixtures:
    home_id   = f.get('team_h')
    away_id   = f.get('team_a')
    home_name = teams.get(home_id, f'Team {home_id}')
    away_name = teams.get(away_id, f'Team {away_id}')
    gw        = f.get('event')
    kickoff   = f.get('kickoff_time')
    finished  = f.get('finished', False)
    started   = f.get('started',  False)

    # Parse kickoff time
    if kickoff:
        ko_dt   = datetime.fromisoformat(kickoff.replace('Z', '+00:00'))
        ko_str  = ko_dt.strftime('%a %d %b %Y %H:%M')
        ko_date = ko_dt.strftime('%Y-%m-%d')
        ko_sort = ko_dt.strftime('%Y%m%d%H%M')
    else:
        ko_str  = 'TBC'
        ko_date = 'TBC'
        ko_sort = '99999999'

    # Apply override if exists
    override_key  = (home_name, away_name)
    override      = OVERRIDES.get(override_key)
    original_gw   = gw

    if override:
        gw            = override['event']
        override_note = override['reason']
        overridden    = True
    else:
        override_note = ''
        overridden    = False

    # Status
    if finished:
        h_score = f.get('team_h_score', '')
        a_score = f.get('team_a_score', '')
        result  = f'{h_score}-{a_score}'
        status  = 'Finished'
    elif started:
        status = 'Live'
        result = ''
    elif gw is None:
        status = 'Unscheduled'
        result = ''
    else:
        status = 'Upcoming'
        result = ''

    rows.append({
        'Fixture_ID':      f.get('id'),
        'GW':              gw,
        'Original_GW':     original_gw,
        'Overridden':      overridden,
        'Kickoff':         ko_str,
        'Kickoff_Date':    ko_date,
        'Kickoff_Sort':    ko_sort,
        'Home':            home_name,
        'Away':            away_name,
        'Status':          status,
        'Result':          result,
        'Home_Difficulty': f.get('team_h_difficulty', ''),
        'Away_Difficulty': f.get('team_a_difficulty', ''),
        'Override_Note':   override_note,
    })

df_all = pd.DataFrame(rows)

# Sort: scheduled by GW then kickoff, unscheduled at bottom
df_sched   = df_all[df_all['GW'].notna()].copy()
df_unsched = df_all[df_all['GW'].isna()].copy()
df_sched   = df_sched.sort_values(['GW', 'Kickoff_Sort', 'Home'])
df_unsched = df_unsched.sort_values('Home')
df_all     = pd.concat([df_sched, df_unsched], ignore_index=True)
df_all['GW'] = pd.array(df_all['GW'], dtype='Int64')


# ============================================================
# 5. FLAG DGW / BGW TEAMS
# ============================================================

# Count fixtures per team per GW
team_gw_counts = {}
for _, row in df_all[df_all['GW'].notna()].iterrows():
    gw = int(row['GW'])
    for team in [row['Home'], row['Away']]:
        key = (team, gw)
        team_gw_counts[key] = team_gw_counts.get(key, 0) + 1

def fixture_type(row):
    if pd.isna(row['GW']):
        return 'Unscheduled'
    gw = int(row['GW'])
    if (team_gw_counts.get((row['Home'], gw), 0) > 1 or
        team_gw_counts.get((row['Away'], gw), 0) > 1):
        return 'DGW'
    return 'SGW'

df_all['Type'] = df_all.apply(fixture_type, axis=1)

# Drop sort helper before saving
df_export = df_all.drop(columns=['Kickoff_Sort']).copy()


# ============================================================
# 6. BUILD PER-GW TEAM SUMMARY
#    Team x GW grid — like Solio fixture ticker
# ============================================================

all_teams = sorted(teams.values())
gw_ids    = sorted([g['GW'] for g in gws])

summary_rows = []
for team in all_teams:
    row = {'Team': team}
    for gw in gw_ids:
        mask = (
            (df_all['GW'] == gw) &
            ((df_all['Home'] == team) | (df_all['Away'] == team))
        )
        team_fixtures = df_all[mask]

        if len(team_fixtures) == 0:
            row[f'GW{gw}'] = 'BGW'
        elif len(team_fixtures) == 1:
            r     = team_fixtures.iloc[0]
            opp   = r['Away'] if r['Home'] == team else r['Home']
            venue = 'H' if r['Home'] == team else 'A'
            abbr  = opp[:3].upper()
            cell  = f'{abbr}({venue})'
            if r['Overridden']:
                cell = cell + '*'
            row[f'GW{gw}'] = cell
        else:
            parts = []
            for _, r in team_fixtures.iterrows():
                opp   = r['Away'] if r['Home'] == team else r['Home']
                venue = 'H' if r['Home'] == team else 'A'
                abbr  = opp[:3].upper()
                cell  = f'{abbr}({venue})'
                if r['Overridden']:
                    cell = cell + '*'
                parts.append(cell)
            row[f'GW{gw}'] = ' + '.join(parts)

    summary_rows.append(row)

df_summary = pd.DataFrame(summary_rows)


# ============================================================
# 7. BUILD OVERRIDE AUDIT LOG
# ============================================================

override_rows = []
for (home, away), info in OVERRIDES.items():
    orig = df_export[
        (df_export['Home'] == home) & (df_export['Away'] == away)
    ]['Original_GW'].values
    orig_gw = int(orig[0]) if len(orig) > 0 and orig[0] else 'Unscheduled'

    override_rows.append({
        'Home':        home,
        'Away':        away,
        'Original_GW': orig_gw,
        'Assigned_GW': info['event'],
        'Reason':      info['reason'],
        'Date_Added':  datetime.now().strftime('%Y-%m-%d'),
    })

df_overrides = pd.DataFrame(override_rows) if override_rows else pd.DataFrame(
    columns=['Home','Away','Original_GW','Assigned_GW','Reason','Date_Added']
)


# ============================================================
# 8. PRINT SUMMARY TO CONSOLE
# ============================================================

# Overrides
if len(df_overrides) > 0:
    print(f'\n=== OVERRIDES APPLIED ({len(df_overrides)}) ===')
    for _, r in df_overrides.iterrows():
        print(f"  {r['Home']} vs {r['Away']}")
        print(f"    GW{r['Original_GW']} -> GW{r['Assigned_GW']}: {r['Reason']}")

# Unscheduled
unscheduled = df_export[df_export['Status'] == 'Unscheduled']
if len(unscheduled) > 0:
    print(f'\n=== UNSCHEDULED FIXTURES ({len(unscheduled)}) ===')
    print('  (Add to OVERRIDES when GW confirmed)')
    for _, r in unscheduled.iterrows():
        print(f"  {r['Home']} vs {r['Away']}")

# DGW summary
dgw_gws = sorted(df_all[df_all['Type']=='DGW']['GW'].dropna().unique())
if dgw_gws:
    print(f'\n=== DOUBLE GAMEWEEKS ===')
    for gw in dgw_gws:
        dgw_mask      = (df_all['GW'] == gw) & (df_all['Type'] == 'DGW')
        dgw_teams_set = set()
        for _, r in df_all[dgw_mask].iterrows():
            if team_gw_counts.get((r['Home'], int(gw)), 0) > 1:
                dgw_teams_set.add(r['Home'])
            if team_gw_counts.get((r['Away'], int(gw)), 0) > 1:
                dgw_teams_set.add(r['Away'])
        print(f'  GW{int(gw)}: {sorted(dgw_teams_set)}')

# Next GW fixtures
if next_gw:
    print(f'\n=== GW{next_gw} FIXTURES ===')
    nxt = df_all[df_all['GW'] == next_gw].sort_values('Kickoff_Sort')
    for _, r in nxt.iterrows():
        tags = []
        if r['Type'] == 'DGW':  tags.append('DGW')
        if r['Overridden']:     tags.append('Override*')
        tag_str = f"  [{', '.join(tags)}]" if tags else ''
        print(f"  {r['Home']} vs {r['Away']}  |  {r['Kickoff']}{tag_str}")


# ============================================================
# 9. SAVE CSVs
# ============================================================

print(f'\n=== SAVING FILES ===')

files = {
    'fixtures_all.csv':          df_export,
    'fixtures_gw_summary.csv':   df_summary,
    'fixtures_overrides.csv':    df_overrides,
    'fixtures_gw_deadlines.csv': df_gws,
}

for filename, df in files.items():
    path = f'{FIXTURE_DIR}/{filename}'
    df.to_csv(path, index=False)
    print(f'  {path}  ({len(df)} rows)')

print(f'''
All files saved to: {FIXTURE_DIR}

KEY:
  BGW = Blank gameweek (no fixture)
  *   = Manual override applied
  DGW = Double gameweek fixture

TO ADD A NEW OVERRIDE:
  Edit the OVERRIDES dict at the top and rerun.
''')
