import pandas as pd
import numpy as np

df = pd.read_csv("https://www.football-data.co.uk/mmz4281/2526/E0.csv")
df['FTHG'] = pd.to_numeric(df['FTHG'], errors='coerce')
df['FTAG'] = pd.to_numeric(df['FTAG'], errors='coerce')
df = df.dropna(subset=['FTHG', 'FTAG', 'AHh'])

n = len(df)
avg_home = df['FTHG'].mean()
avg_away = df['FTAG'].mean()

# Derive implied goals from AH line
df['imp_home_g'] = avg_home - (df['AHh'] / 2)
df['imp_away_g'] = avg_away + (df['AHh'] / 2)

cols = ['HomeTeam', 'AwayTeam', 'FTHG', 'FTAG', 'AHh', 'imp_home_g', 'imp_away_g']
print(df[cols].head(10).to_string())

# Check correlation between implied and actual
print(f"\nCorrelation imp_home_g vs FTHG: {df['imp_home_g'].corr(df['FTHG']):.3f}")
print(f"Correlation imp_away_g vs FTAG: {df['imp_away_g'].corr(df['FTAG']):.3f}")