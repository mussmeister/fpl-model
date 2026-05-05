import numpy as np
import pandas as pd
from utils.data_loaders import (
    load_fplreview_player_data,
    load_solio_player_data,
    melt_player_projections,
)

DEFAULT_MAX_MINUTES = 180.0
DEFAULT_BASE_MINUTES = {
    "G": 90.0,
    "D": 85.0,
    "M": 82.0,
    "F": 78.0,
}
BASE_GOAL_SHARE = {
    "G": 0.00,
    "D": 0.06,
    "M": 0.24,
    "F": 0.70,
}
BASE_ASSIST_SHARE = {
    "G": 0.01,
    "D": 0.08,
    "M": 0.40,
    "F": 0.18,
}
BASE_CS_SHARE = {
    "G": 1.00,
    "D": 1.00,
    "M": 0.50,
    "F": 0.15,
}


def _normalize_position(pos):
    if pd.isna(pos):
        return "M"
    pos = str(pos).strip().upper()
    if pos in {"GK", "G"}:
        return "G"
    if pos in {"DEF", "D"}:
        return "D"
    if pos in {"MID", "M"}:
        return "M"
    if pos in {"FWD", "FW", "F"}:
        return "F"
    return "M"


def _appearance_points(xmins):
    if xmins <= 0:
        return 0.0
    if xmins < 60:
        return 1.0
    if xmins < 120:
        return 2.0
    return 4.0


def _skill_factor(bv):
    try:
        bv = float(bv)
    except Exception:
        return 1.0
    return float(np.clip(0.9 + (bv - 5.0) / 10.0, 0.65, 1.35))


def _team_projection_df(team_projections):
    rows = []
    for gw, df in team_projections.items():
        if df is None or df.empty:
            continue
        temp = df.copy()
        temp["GW"] = gw
        rows.append(temp)
    if not rows:
        return pd.DataFrame(columns=["Team", "GW", "G", "GC", "CS"])
    return pd.concat(rows, ignore_index=True)


def _estimate_play_probability(pos, bv):
    pos = _normalize_position(pos)
    try:
        bv = float(bv)
    except Exception:
        return 0.0

    if pos == "G":
        if bv < 4.2:
            return 0.0
        if bv < 4.8:
            return 0.20
        if bv < 5.2:
            return 0.55
        if bv < 6.0:
            return 0.75
        return 0.90

    if pos == "D":
        if bv < 4.0:
            return 0.0
        if bv < 4.6:
            return 0.35
        if bv < 5.2:
            return 0.55
        if bv < 6.0:
            return 0.70
        return 0.82

    if pos == "M":
        if bv < 4.0:
            return 0.0
        if bv < 4.6:
            return 0.30
        if bv < 5.2:
            return 0.50
        if bv < 6.0:
            return 0.65
        return 0.78

    if pos == "F":
        if bv < 4.2:
            return 0.0
        if bv < 4.8:
            return 0.20
        if bv < 5.4:
            return 0.35
        if bv < 6.2:
            return 0.45
        return 0.60

    return 0.0


def _estimate_minutes(pos, bv, fixtures=1, max_minutes=DEFAULT_MAX_MINUTES):
    pos = _normalize_position(pos)
    base = DEFAULT_BASE_MINUTES.get(pos, DEFAULT_BASE_MINUTES["M"])
    factor = _skill_factor(bv)
    probability = _estimate_play_probability(pos, bv)

    fixtures = int(np.clip(fixtures, 1, 3))
    total_max = float(np.clip(fixtures * 90.0, 0.0, max_minutes))
    minutes = base * factor * probability * fixtures
    if probability < 0.06:
        return 0.0
    return float(np.clip(minutes, 0.0, total_max))


def combine_player_benchmark_data(fplreview_dir, solio_dir):
    raw_fplreview = load_fplreview_player_data(fplreview_dir)
    raw_solio = load_solio_player_data(solio_dir)

    fplreview_long = melt_player_projections(raw_fplreview)
    solio_long = melt_player_projections(raw_solio)

    if fplreview_long.empty and solio_long.empty:
        return pd.DataFrame()

    combined = pd.concat([fplreview_long, solio_long], ignore_index=True)
    return combined


def estimate_player_xpts(players, team_projections, fixture_counts=None, use_source_minutes=True, max_minutes=DEFAULT_MAX_MINUTES):
    if players.empty:
        return players.copy()

    players = players.copy()
    players["Pos"] = players["Pos"].apply(_normalize_position)
    players["source_xMins"] = pd.to_numeric(players["xMins"], errors="coerce").fillna(0.0)
    players["BV"] = pd.to_numeric(players.get("BV", 0), errors="coerce").fillna(0.0)

    if fixture_counts is None:
        fixture_counts = {}

    def _lookup_fixtures(row):
        return fixture_counts.get((row["GW"], row["Team"]), 1)

    players["fixture_count"] = players.apply(_lookup_fixtures, axis=1)

    if use_source_minutes:
        players["xMins"] = players["source_xMins"].copy()
    else:
        players["xMins"] = players.apply(
            lambda row: _estimate_minutes(row["Pos"], row["BV"], row["fixture_count"], max_minutes),
            axis=1,
        )

    players["mins_ratio"] = np.clip(players["xMins"] / (players["fixture_count"] * 90.0), 0.0, 1.0)
    players["appearance_pts"] = players["xMins"].apply(_appearance_points)
    players["skill_factor"] = players["BV"].apply(_skill_factor)
    players["goal_share"] = players["Pos"].map(BASE_GOAL_SHARE).fillna(0.0)
    players["assist_share"] = players["Pos"].map(BASE_ASSIST_SHARE).fillna(0.0)
    players["cs_share"] = players["Pos"].map(BASE_CS_SHARE).fillna(0.0)

    df_team = _team_projection_df(team_projections)
    team_merge = df_team[["Team", "GW", "G", "GC", "CS"]].copy()
    team_merge = team_merge.rename(columns={"G": "G_team", "GC": "GC_team", "CS": "CS_team"})
    players = players.merge(team_merge, on=["Team", "GW"], how="left")
    players[["G_team", "GC_team", "CS_team"]] = players[["G_team", "GC_team", "CS_team"]].fillna(0.0)

    players["goal_prob"] = (
        players["G_team"] * players["goal_share"] * players["mins_ratio"] * players["skill_factor"]
    )
    players["assist_prob"] = (
        players["G_team"] * players["assist_share"] * players["mins_ratio"] * players["skill_factor"]
    )
    players["cs_prob"] = players["CS_team"] * players["cs_share"] * players["mins_ratio"]
    players["bonus_prob"] = np.clip(players["G_team"] / 3.0, 0.0, 1.0) * players["mins_ratio"] * 0.55
    players["save_pts"] = np.where(
        players["Pos"] == "G",
        np.clip(1.2 - players["GC_team"], 0.0, 1.2) * players["mins_ratio"] * 0.35,
        0.0,
    )

    players["model_goal_pts"] = players["goal_prob"] * 6.0
    players["model_assist_pts"] = players["assist_prob"] * 3.0
    players["model_cs_pts"] = np.where(
        players["Pos"].isin(["G", "D"]),
        players["cs_prob"] * 4.0,
        np.where(players["Pos"] == "M", players["cs_prob"] * 1.0, 0.0),
    )
    players["model_bonus_pts"] = players["bonus_prob"] * 0.35
    players["model_xPts"] = (
        players["appearance_pts"]
        + players["model_goal_pts"]
        + players["model_assist_pts"]
        + players["model_cs_pts"]
        + players["model_bonus_pts"]
        + players["save_pts"]
    )

    players["model_xPts"] = players["model_xPts"].round(2)
    players["goal_prob"] = players["goal_prob"].round(4)
    players["assist_prob"] = players["assist_prob"].round(4)
    players["cs_prob"] = players["cs_prob"].round(4)
    players["bonus_prob"] = players["bonus_prob"].round(4)
    players["save_pts"] = players["save_pts"].round(3)

    return players
