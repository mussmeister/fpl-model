import os
import requests
import numpy as np
import pandas as pd
from utils.team_mappings import to_short, FDCO_TO_SHORT, SOLIO_TO_SHORT, ODDS_TO_SHORT


def load_season_results(season):
    """
    Load season results from football-data.co.uk.
    Includes AHh (Asian Handicap line) for use in AH-blended DC ratings.
    Returns df_results, all_teams, team_idx, LEAGUE_AVG_HOME, LEAGUE_AVG_AWAY.
    """
    url = f"https://www.football-data.co.uk/mmz4281/{season}/E0.csv"
    df = pd.read_csv(url)
    df["FTHG"] = pd.to_numeric(df["FTHG"], errors="coerce")
    df["FTAG"] = pd.to_numeric(df["FTAG"], errors="coerce")
    df = df.dropna(subset=["FTHG", "FTAG"]).copy()

    if "AHh" in df.columns:
        df["AHh"] = pd.to_numeric(df["AHh"], errors="coerce")

    df["HomeTeam"] = df["HomeTeam"].apply(lambda x: to_short(x, FDCO_TO_SHORT))
    df["AwayTeam"] = df["AwayTeam"].apply(lambda x: to_short(x, FDCO_TO_SHORT))

    for fmt in ["%d/%m/%Y", "%d/%m/%y", "%Y-%m-%d"]:
        try:
            df["Date"] = pd.to_datetime(df["Date"], format=fmt)
            break
        except Exception:
            continue

    n = len(df)
    avg_home = round(df["FTHG"].sum() / n, 4)
    avg_away = round(df["FTAG"].sum() / n, 4)
    ref_date = df["Date"].max()
    df["days_ago"] = (ref_date - df["Date"]).dt.days.fillna(0)

    all_teams = sorted(set(df["HomeTeam"].tolist() + df["AwayTeam"].tolist()))
    team_idx  = {t: i for i, t in enumerate(all_teams)}

    ah_available = "AHh" in df.columns and df["AHh"].notna().sum() > 0
    print(f"  Matches  : {n} | Home avg: {avg_home} | Away avg: {avg_away}")
    print(f"  Teams    : {len(all_teams)}")
    print(f"  AHh data : {'available (%d matches)' % df['AHh'].notna().sum() if ah_available else 'not found'}")

    return df, all_teams, team_idx, avg_home, avg_away


def fetch_odds(api_key, bookmakers=None, regions="uk", markets="h2h,totals", odds_format="decimal"):
    """
    Fetch current EPL h2h + totals odds from the-odds-api in one call.
    Returns odds_lookup dict keyed by (home, away).
    Each entry has home_odds/draw_odds/away_odds plus optional totals dict
    with 'line' and 'over_prob' for use in implied-total lambda rescaling.
    Supports optional bookmaker filters (e.g. ['pinnacle']).
    """
    params = {
        "apiKey": api_key,
        "regions": regions,
        "markets": markets,
        "oddsFormat": odds_format,
    }
    requested_bookies = None
    if bookmakers:
        if isinstance(bookmakers, (list, tuple)):
            requested_bookies = {str(b).strip().lower() for b in bookmakers}
            params["bookmakers"] = ",".join(requested_bookies)
        else:
            requested_bookies = {b.strip().lower() for b in str(bookmakers).split(",")}
            params["bookmakers"] = ",".join(requested_bookies)

    r = requests.get(
        "https://api.the-odds-api.com/v4/sports/soccer_epl/odds",
        params=params,
        timeout=15
    )
    r.raise_for_status()
    print(f"  Calls remaining: {r.headers.get('x-requests-remaining', '?')}")
    if requested_bookies:
        print(f"  Requested bookmakers: {params['bookmakers']}")

    def parse_match(event):
        home = to_short(event["home_team"], ODDS_TO_SHORT)
        away = to_short(event["away_team"], ODDS_TO_SHORT)
        h, d, a = [], [], []
        over_by_line  = {}   # line -> [over prices]
        under_by_line = {}   # line -> [under prices]

        bookies_to_parse = event.get("bookmakers", [])
        if requested_bookies:
            filtered = [bm for bm in bookies_to_parse
                        if bm.get("key", "").lower() in requested_bookies
                        or bm.get("title", "").lower() in requested_bookies]
            if filtered:
                bookies_to_parse = filtered

        for bm in bookies_to_parse:
            for mkt in bm.get("markets", []):
                if mkt["key"] == "h2h":
                    for o in mkt["outcomes"]:
                        n_name = to_short(o["name"], ODDS_TO_SHORT)
                        p = o["price"]
                        if n_name == home:
                            h.append(p)
                        elif o["name"] == "Draw":
                            d.append(p)
                        elif n_name == away:
                            a.append(p)
                elif mkt["key"] == "totals":
                    for o in mkt["outcomes"]:
                        line = o.get("point")
                        if line is None:
                            continue
                        if o["name"] == "Over":
                            over_by_line.setdefault(line, []).append(o["price"])
                        elif o["name"] == "Under":
                            under_by_line.setdefault(line, []).append(o["price"])

        if not h:
            return None

        totals = None
        if over_by_line:
            best_line = max(over_by_line, key=lambda l: len(over_by_line[l]))
            avg_over  = float(np.mean(over_by_line[best_line]))
            avg_under = float(np.mean(under_by_line.get(best_line, [avg_over])))
            inv_o = 1.0 / avg_over
            inv_u = 1.0 / avg_under
            over_prob = inv_o / (inv_o + inv_u)
            totals = {"line": best_line, "over_prob": round(over_prob, 4)}

        return {"home": home, "away": away,
                "home_odds": round(np.mean(h), 4),
                "draw_odds": round(np.mean(d), 4),
                "away_odds": round(np.mean(a), 4),
                "totals": totals}

    raw_odds = [parse_match(e) for e in r.json()]
    raw_odds = [o for o in raw_odds if o]
    if requested_bookies and len(raw_odds) == 0:
        print("  No requested bookmaker odds returned; retrying without bookmaker filter.")
        params.pop("bookmakers", None)
        r = requests.get(
            "https://api.the-odds-api.com/v4/sports/soccer_epl/odds",
            params=params,
            timeout=15
        )
        r.raise_for_status()
        raw_odds = [parse_match(e) for e in r.json()]
        raw_odds = [o for o in raw_odds if o]

    odds_lookup = {(o["home"], o["away"]): o for o in raw_odds}
    print(f"  Fixtures with odds: {len(raw_odds)}")
    return odds_lookup


def load_fixtures(fixture_dir, n_gws=4):
    """
    Load fixtures CSV and return target GW list and filtered DataFrame.
    """
    df = pd.read_csv(f"{fixture_dir}/fixtures_all.csv")
    df["GW"]   = pd.to_numeric(df["GW"], errors="coerce")
    df["Home"] = df["Home"].str.strip()
    df["Away"] = df["Away"].str.strip()

    upcoming   = df[df["Status"] == "Upcoming"]
    avail_gws  = sorted(upcoming["GW"].dropna().unique().astype(int))
    target_gws = avail_gws[:n_gws]
    df_target  = df[
        df["GW"].isin(target_gws) & (df["Status"] == "Upcoming")
    ].copy()

    print(f"  Target GWs: {target_gws}")
    return df_target, target_gws


def _extract_gw_from_solio(df, gameweek):
    """
    Extract a single GW's data from a Solio DataFrame.
    Returns DataFrame with Solio_G/GC/GD/CS columns or None.
    """
    col_g  = f"{gameweek}_G"
    col_gc = f"{gameweek}_GC"
    col_cs = f"{gameweek}_CS"

    if col_g not in df.columns:
        return None

    out = df[["Team"]].copy()
    out["Solio_G"]  = pd.to_numeric(df[col_g],  errors="coerce").round(2)
    out["Solio_GC"] = pd.to_numeric(df[col_gc], errors="coerce").round(2)
    out["Solio_CS"] = pd.to_numeric(df[col_cs], errors="coerce").round(2)
    out["Solio_GD"] = (out["Solio_G"] - out["Solio_GC"]).round(2)
    out["GW"]       = gameweek
    out["incomplete_dgw"] = False
    out = out.dropna(subset=["Solio_G"]).reset_index(drop=True)
    return out[["Team", "GW", "Solio_G", "Solio_GC", "Solio_GD", "Solio_CS", "incomplete_dgw"]]


def load_all_solio(solio_dir, target_gws):
    """
    Load Solio data for all target GWs.
    Searches across all CSV files in the solio directory automatically.
    Returns dict keyed by GW.
    """
    # Pre-load all CSV files once
    csv_files = [
        os.path.join(solio_dir, f)
        for f in os.listdir(solio_dir)
        if f.endswith(".csv")
    ]

    solio_frames = []
    for path in csv_files:
        try:
            df = pd.read_csv(path)
            if "Team" in df.columns:
                df["Team"] = df["Team"].apply(lambda x: to_short(x, SOLIO_TO_SHORT))
                solio_frames.append((path, df))
        except Exception:
            continue

    print(f"  Solio files found : {len(solio_frames)}")
    for path, df in solio_frames:
        # Identify which GWs each file contains
        gw_cols = [c.split("_")[0] for c in df.columns if c.endswith("_G")
                   and c.split("_")[0].isdigit()]
        print(f"    {os.path.basename(path)}: GWs {sorted(set(int(g) for g in gw_cols))}")

    solio_by_gw = {}
    for gw in target_gws:
        found = False
        for path, df in solio_frames:
            result = _extract_gw_from_solio(df, gw)
            if result is not None and len(result) > 0:
                solio_by_gw[gw] = result
                fname = os.path.basename(path)
                print(f"  GW{gw}: {len(result)} teams loaded (from {fname})")
                found = True
                break
        if not found:
            print(f"  GW{gw}: not found in any Solio file")

    return solio_by_gw


def _list_csv_files(directory):
    return [
        os.path.join(directory, f)
        for f in os.listdir(directory)
        if f.lower().endswith(".csv")
    ]


def _latest_csv_file(paths):
    if not paths:
        return None
    paths = [p for p in paths if os.path.isfile(p)]
    if not paths:
        return None
    return max(paths, key=os.path.getmtime)


def load_fplreview_player_data(fplreview_dir):
    """
    Load the latest FPLReview player projection CSV from a directory.
    Returns a DataFrame of raw player projections.
    """
    csv_files = _list_csv_files(fplreview_dir)
    latest = _latest_csv_file(csv_files)
    if latest is None:
        print(f"  No FPLReview player files loaded from {fplreview_dir}")
        return pd.DataFrame()

    try:
        df = pd.read_csv(latest)
    except Exception:
        print(f"  Could not read latest FPLReview file: {latest}")
        return pd.DataFrame()

    if "Name" not in df.columns or "Team" not in df.columns:
        print(f"  Latest FPLReview file is missing required columns: {latest}")
        return pd.DataFrame()

    df["source"] = "fplreview"
    df["source_file"] = os.path.basename(latest)
    print(f"  FPLReview file loaded: {os.path.basename(latest)} -> {len(df)} rows")
    return df


def load_solio_player_data(solio_dir):
    """
    Load the latest Solio player projection CSV from a directory.
    Returns a DataFrame of raw player projections.
    """
    csv_files = _list_csv_files(solio_dir)
    csv_files = [p for p in csv_files if "projection_all_metrics" in os.path.basename(p).lower()]
    latest = _latest_csv_file(csv_files)
    if latest is None:
        print(f"  No Solio player files loaded from {solio_dir}")
        return pd.DataFrame()

    try:
        df = pd.read_csv(latest)
    except Exception:
        print(f"  Could not read latest Solio file: {latest}")
        return pd.DataFrame()

    if "Name" not in df.columns or "Team" not in df.columns:
        print(f"  Latest Solio file is missing required columns: {latest}")
        return pd.DataFrame()

    df["Team"] = df["Team"].apply(lambda x: to_short(x, SOLIO_TO_SHORT))
    df["source"] = "solio"
    df["source_file"] = os.path.basename(latest)
    print(f"  Solio file loaded: {os.path.basename(latest)} -> {len(df)} rows")
    return df


def melt_player_projections(df, prefixes=None):
    """
    Convert wide player projection data with GW-specific columns into long format.
    Example column names: 29_xMins, 29_Pts, 29_goals, 29_assists, 29_CS, 29_bonus.
    Returns a DataFrame with one row per player per GW.
    """
    if df.empty:
        return df.copy()

    if prefixes is None:
        prefixes = ["xMins", "Pts", "goals", "assists", "CS", "bonus",
                    "cbit", "eo"]

    column_map = {}
    base_columns = [
        c for c in df.columns
        if not (c.split("_")[0].isdigit() and c.split("_")[1] in prefixes)
    ]

    for c in df.columns:
        parts = c.split("_")
        if len(parts) != 2:
            continue
        gw_part, metric = parts
        if not gw_part.isdigit() or metric not in prefixes:
            continue
        gw = int(gw_part)
        column_map.setdefault(gw, {})[metric] = c

    rows = []
    for gw, metric_cols in sorted(column_map.items()):
        subset = df[base_columns].copy()
        for metric in prefixes:
            if metric in metric_cols:
                subset[metric] = pd.to_numeric(df[metric_cols[metric]], errors="coerce")
            else:
                subset[metric] = pd.NA
        subset["GW"] = gw
        rows.append(subset)

    if not rows:
        return pd.DataFrame(columns=base_columns + prefixes + ["GW"])

    long_df = pd.concat(rows, ignore_index=True)
    keep_cols = [c for c in base_columns if c not in {"source", "source_file"}]
    keep_cols += ["source", "source_file"] + prefixes + ["GW"]
    long_df = long_df[keep_cols]
    return long_df
