import sqlite3, pandas as pd

with sqlite3.connect('outputs/projections_history.db') as conn:
    model_fix = pd.read_sql('''
        SELECT f.home_team, f.away_team, f.home_g, f.away_g, f.home_cs, f.away_cs
        FROM projections_fixtures f
        INNER JOIN (
            SELECT gw, home_team, away_team, MAX(timestamp) AS max_ts
            FROM projections_fixtures WHERE gw=36 GROUP BY home_team, away_team
        ) l ON f.gw=l.gw AND f.home_team=l.home_team AND f.away_team=l.away_team AND f.timestamp=l.max_ts
    ''', conn)

    solio = pd.read_sql('''
        SELECT s.team, s.g AS sol_g, s.gc AS sol_gc, s.cs AS sol_cs
        FROM solio_fixture_snapshots s
        INNER JOIN (
            SELECT team, gw, MAX(ingested_at) AS max_ia
            FROM solio_fixture_snapshots WHERE gw=36 GROUP BY team
        ) l ON s.team=l.team AND s.gw=l.gw AND s.ingested_at=l.max_ia
    ''', conn)

# Build team-level model from fixtures
rows = []
for _, r in model_fix.iterrows():
    rows.append({'team': r['home_team'], 'g': r['home_g'], 'gc': r['away_g'], 'cs': r['home_cs']})
    rows.append({'team': r['away_team'], 'g': r['away_g'], 'gc': r['home_g'], 'cs': r['away_cs']})

model_teams = (pd.DataFrame(rows)
    .groupby('team', as_index=False)
    .agg(mdl_g=('g','sum'), mdl_gc=('gc','sum'), mdl_cs=('cs','sum')))

cmp = model_teams.merge(solio, on='team').sort_values('team')
cmp['d_g']  = (cmp['mdl_g']  - cmp['sol_g']).round(3)
cmp['d_gc'] = (cmp['mdl_gc'] - cmp['sol_gc']).round(3)
cmp['d_cs'] = (cmp['mdl_cs'] - cmp['sol_cs']).round(3)
cmp = cmp.round(3)

print("Team             | Mdl G  Sol G   d_G  | Mdl GC Sol GC  d_GC | Mdl CS Sol CS  d_CS")
print("-" * 95)
for _, r in cmp.iterrows():
    flag = " <<<" if abs(r['d_g']) > 0.3 or abs(r['d_gc']) > 0.3 else ""
    print("{:<17}| {:5.2f}  {:5.2f}  {:+.2f} | {:5.2f}  {:5.2f}  {:+.2f} | {:5.3f}  {:5.3f}  {:+.3f}{}".format(
        r['team'], r['mdl_g'], r['sol_g'], r['d_g'],
        r['mdl_gc'], r['sol_gc'], r['d_gc'],
        r['mdl_cs'], r['sol_cs'], r['d_cs'], flag))

print()
print("MAE Goals: {:.3f}  |  MAE GC: {:.3f}  |  MAE CS: {:.3f}".format(
    cmp['d_g'].abs().mean(), cmp['d_gc'].abs().mean(), cmp['d_cs'].abs().mean()))
print()
print("Bias Goals: {:+.3f}  |  Bias GC: {:+.3f}  |  Bias CS: {:+.3f}".format(
    cmp['d_g'].mean(), cmp['d_gc'].mean(), cmp['d_cs'].mean()))
