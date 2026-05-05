ODDS_TO_SHORT = {
    "Arsenal": "Arsenal", "Aston Villa": "Aston Villa",
    "Bournemouth": "Bournemouth", "AFC Bournemouth": "Bournemouth",
    "Brentford": "Brentford",
    "Brighton and Hove Albion": "Brighton",
    "Brighton & Hove Albion": "Brighton",
    "Burnley": "Burnley", "Chelsea": "Chelsea",
    "Crystal Palace": "Crystal Palace", "Everton": "Everton",
    "Fulham": "Fulham", "Leeds United": "Leeds",
    "Liverpool": "Liverpool", "Manchester City": "Man City",
    "Manchester United": "Man Utd", "Newcastle United": "Newcastle",
    "Nottingham Forest": "Nott'm Forest", "Sunderland": "Sunderland",
    "Tottenham Hotspur": "Spurs", "West Ham United": "West Ham",
    "Wolverhampton Wanderers": "Wolves",
}

SOLIO_TO_SHORT = {
    "Arsenal": "Arsenal", "Aston Villa": "Aston Villa",
    "Aston Vill": "Aston Villa", "Bournemouth": "Bournemouth",
    "Bournemou": "Bournemouth", "Brentford": "Brentford",
    "Brighton": "Brighton", "Burnley": "Burnley",
    "Chelsea": "Chelsea", "Crystal Palace": "Crystal Palace",
    "Crystal Pa": "Crystal Palace", "Everton": "Everton",
    "Fulham": "Fulham", "Leeds United": "Leeds",
    "Leeds Utd": "Leeds", "Leeds Uni": "Leeds", "Leeds": "Leeds",
    "Liverpool": "Liverpool", "Man City": "Man City",
    "Man Utd": "Man Utd", "Manchester City": "Man City",
    "Manchester United": "Man Utd", "Manchester Utd": "Man Utd",
    "Newcastle": "Newcastle", "Newcastle United": "Newcastle",
    "Nott'm For": "Nott'm Forest", "Nott'm Forest": "Nott'm Forest",
    "Nottingham Forest": "Nott'm Forest", "Sunderland": "Sunderland",
    "Spurs": "Spurs", "Tottenham": "Spurs",
    "Tottenham Hotspur": "Spurs", "West Ham": "West Ham",
    "West Ham United": "West Ham", "Wolves": "Wolves",
    "Wolverhampton": "Wolves",
}

FDCO_TO_SHORT = {
    "Arsenal": "Arsenal", "Aston Villa": "Aston Villa",
    "Bournemouth": "Bournemouth", "Brentford": "Brentford",
    "Brighton": "Brighton", "Burnley": "Burnley",
    "Chelsea": "Chelsea", "Crystal Palace": "Crystal Palace",
    "Everton": "Everton", "Fulham": "Fulham",
    "Leeds United": "Leeds", "Leeds Utd": "Leeds",
    "Liverpool": "Liverpool", "Man City": "Man City",
    "Man United": "Man Utd", "Man Utd": "Man Utd",
    "Newcastle": "Newcastle", "Nott'm Forest": "Nott'm Forest",
    "Nottingham Forest": "Nott'm Forest",
    "Sunderland": "Sunderland", "Tottenham": "Spurs",
    "West Ham": "West Ham", "Wolves": "Wolves",
}

def to_short(name, mapping):
    return mapping.get(str(name).strip(), str(name).strip())
