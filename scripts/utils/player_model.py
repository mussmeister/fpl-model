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

# Minimum season minutes before switching to rate-based goal/xMins estimation.
# Below this threshold the BV/position fallback is used instead.
MIN_SEASON_MINS = 270   # ~3 full games

# Status discount applied to season-average minutes for non-available players.
STATUS_MINS_MULT = {"a": 1.0, "d": 0.50}   # injured/suspended → 0 (default)

# Position-average goals / assists per 90 for a typical starter.
# Used for shrinkage: actual rate × RATE_SHRINK + pos_avg × (1 − RATE_SHRINK).
# This approximates xG regression — elite scorers over-convert vs xG, so we
# blend their actual rate toward the positional mean.
POS_AVG_G_PER90  = {"G": 0.01, "D": 0.04, "M": 0.12, "F": 0.30}
POS_AVG_A_PER90  = {"G": 0.01, "D": 0.05, "M": 0.20, "F": 0.12}

RATE_SHRINK       = 0.60   # weight on actual goal/assist rate; 1−RATE_SHRINK on pos avg
XG_SHRINK         = 0.75   # higher weight for xG/xA rates — less noisy than actuals
MAX_TEAM_STRENGTH = 1.30   # cap on (team_G_per_fixture / league_avg_g) multiplier
FORM_WEIGHT       = 0.35   # weight on last-5-GW rate; 1−FORM_WEIGHT on full-season rate

# Set piece bonuses applied only to BV-fallback players (no season rate data).
# Rate-based players already have set piece contributions in their xG/xA.
# Values represent expected contribution per 90 mins from pens / corners+FKs.
PEN_BONUS_PER90    = 0.06  # ~0.08 pens/game × 0.76 xG
CORNER_BONUS_PER90 = 0.08  # ~5 corners/game × 60% primary × 0.027 xA/corner


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
        if bv < 4.2: return 0.0
        if bv < 4.8: return 0.20
        if bv < 5.2: return 0.55
        if bv < 6.0: return 0.75
        return 0.90

    if pos == "D":
        if bv < 4.0: return 0.0
        if bv < 4.6: return 0.35
        if bv < 5.2: return 0.55
        if bv < 6.0: return 0.70
        return 0.82

    if pos == "M":
        if bv < 4.0: return 0.0
        if bv < 4.6: return 0.30
        if bv < 5.2: return 0.50
        if bv < 6.0: return 0.65
        return 0.78

    if pos == "F":
        if bv < 4.2: return 0.0
        if bv < 4.8: return 0.20
        if bv < 5.4: return 0.35
        if bv < 6.2: return 0.45
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


def estimate_player_xpts(
    players,
    team_projections,
    fixture_counts=None,
    use_source_minutes=True,
    max_minutes=DEFAULT_MAX_MINUTES,
    gws_elapsed=34,
    league_avg_g=1.38,
):
    """Estimate player xPts.

    When use_source_minutes=False the model switches to two improved strategies
    for players that have FPL season stats (season_mins, season_goals,
    season_assists, status columns in the DataFrame):

      xMins  — season-average minutes per GW (season_mins / gws_elapsed),
                capped at position default, scaled by status availability.
                Falls back to BV/position curve for players with < MIN_SEASON_MINS.

      goal/assist prob — per-player scoring rate (goals_per_90, assists_per_90)
                         × (xMins/90) × team_strength_multiplier.
                         team_strength = team_G / (league_avg_g × fixture_count)
                         so DGW G_team (already doubled) is correctly normalised.
                         Falls back to position-share × skill_factor for players
                         without season data.

    Parameters
    ----------
    gws_elapsed : int
        Number of GWs completed so far this season (used to compute avg mins).
    league_avg_g : float
        Average goals scored per team per single fixture (used for team-strength
        multiplier).  Default 1.38 ≈ (1.52 home + 1.24 away) / 2.
    """
    if players.empty:
        return players.copy()

    players = players.copy()
    players["Pos"] = players["Pos"].apply(_normalize_position)
    players["source_xMins"] = pd.to_numeric(players["xMins"], errors="coerce").fillna(0.0)
    players["BV"] = pd.to_numeric(players.get("BV", 0), errors="coerce").fillna(0.0)

    if fixture_counts is None:
        fixture_counts = {}

    players["fixture_count"] = players.apply(
        lambda row: fixture_counts.get((row["GW"], row["Team"]), 1), axis=1
    )

    # ── Season stats (optional columns from FPL API) ──────────────────────────
    has_season = "season_mins" in players.columns
    if has_season:
        players["season_mins"]    = pd.to_numeric(players["season_mins"],    errors="coerce").fillna(0.0)
        players["season_goals"]   = pd.to_numeric(players.get("season_goals",   0), errors="coerce").fillna(0.0)
        players["season_assists"] = pd.to_numeric(players.get("season_assists", 0), errors="coerce").fillna(0.0)
        players["season_xg"]      = pd.to_numeric(players.get("season_xg",      0), errors="coerce").fillna(0.0)
        players["season_xa"]      = pd.to_numeric(players.get("season_xa",      0), errors="coerce").fillna(0.0)
        players["status"]         = players.get("status", "a").fillna("a")
        players["_use_rates"] = players["season_mins"] >= MIN_SEASON_MINS
    else:
        players["_use_rates"] = False

    # Resolve availability as a continuous 0–1 multiplier.
    # Prefer chance_of_playing (from FPL API %) over the categorical STATUS_MINS_MULT.
    if "chance_of_playing" in players.columns:
        players["_avail"] = pd.to_numeric(players["chance_of_playing"], errors="coerce")
        missing = players["_avail"].isna()
        if missing.any():
            players.loc[missing, "_avail"] = (
                players.loc[missing, "status"]
                .apply(lambda s: STATUS_MINS_MULT.get(str(s).lower(), 0.0))
            )
    else:
        players["_avail"] = players["status"].apply(
            lambda s: STATUS_MINS_MULT.get(str(s).lower(), 0.0)
        ) if has_season else 1.0

    # ── xMins ─────────────────────────────────────────────────────────────────
    if use_source_minutes:
        players["xMins"] = players["source_xMins"].copy()
    else:
        def _calc_xmins(row):
            fc    = int(row["fixture_count"])
            avail = float(row["_avail"])
            if row["_use_rates"]:
                avg_per_gw = row["season_mins"] / max(int(gws_elapsed), 1)
                raw = min(avg_per_gw, 90.0) * fc * avail
                return float(np.clip(raw, 0.0, fc * 90.0))
            base = _estimate_minutes(row["Pos"], row["BV"], fc, max_minutes)
            return float(base * avail)

        players["xMins"] = players.apply(_calc_xmins, axis=1)

    players["mins_ratio"]     = np.clip(players["xMins"] / (players["fixture_count"] * 90.0), 0.0, 1.0)
    players["appearance_pts"] = players["xMins"].apply(_appearance_points)
    players["skill_factor"]   = players["BV"].apply(_skill_factor)
    players["goal_share"]     = players["Pos"].map(BASE_GOAL_SHARE).fillna(0.0)
    players["assist_share"]   = players["Pos"].map(BASE_ASSIST_SHARE).fillna(0.0)
    players["cs_share"]       = players["Pos"].map(BASE_CS_SHARE).fillna(0.0)

    # ── Merge team projections ────────────────────────────────────────────────
    df_team   = _team_projection_df(team_projections)
    team_merge = df_team[["Team", "GW", "G", "GC", "CS"]].copy()
    team_merge = team_merge.rename(columns={"G": "G_team", "GC": "GC_team", "CS": "CS_team"})
    players = players.merge(team_merge, on=["Team", "GW"], how="left")
    players[["G_team", "GC_team", "CS_team"]] = players[["G_team", "GC_team", "CS_team"]].fillna(0.0)

    # ── Goal / assist probability ─────────────────────────────────────────────
    # Rate-based: uses player's own goals/assists per 90 this season,
    # adjusted for team attack strength relative to league average.
    # team_strength normalises for DGW (G_team is already summed across fixtures).
    use_rates = players["_use_rates"]
    fc_safe   = players["fixture_count"].clip(lower=1)

    # Initialise with position-share fallback for all rows
    players["goal_prob"]   = (players["G_team"] * players["goal_share"]   * players["mins_ratio"] * players["skill_factor"])
    players["assist_prob"] = (players["G_team"] * players["assist_share"] * players["mins_ratio"] * players["skill_factor"])

    if use_rates.any():
        s_mins = players["season_mins"].clip(lower=1)

        # Prefer xG/xA (Opta) when available — less noisy than actual goals.
        # A player with high xG but few actual goals is correctly rated higher.
        has_xg = "season_xg" in players.columns and players["season_xg"].any()
        if has_xg:
            g_per_90 = (players["season_xg"].clip(lower=0) / s_mins) * 90.0
            a_per_90 = (players["season_xa"].clip(lower=0) / s_mins) * 90.0
            g_shrink = XG_SHRINK
            a_shrink = XG_SHRINK
        else:
            g_per_90 = (players["season_goals"]   / s_mins) * 90.0
            a_per_90 = (players["season_assists"] / s_mins) * 90.0
            g_shrink = RATE_SHRINK
            a_shrink = RATE_SHRINK

        # Blend season rate with recent-form rate where available.
        # recent_xg_p90 / recent_xa_p90 come from last-5-GW FPL stats query.
        if "recent_xg_p90" in players.columns:
            has_form = players["recent_xg_p90"].notna() & use_rates
            if has_form.any():
                g_per_90 = g_per_90.copy()
                a_per_90 = a_per_90.copy()
                g_per_90[has_form] = (
                    (1 - FORM_WEIGHT) * g_per_90[has_form]
                    + FORM_WEIGHT * players.loc[has_form, "recent_xg_p90"]
                )
                a_per_90[has_form] = (
                    (1 - FORM_WEIGHT) * a_per_90[has_form]
                    + FORM_WEIGHT * players.loc[has_form, "recent_xa_p90"]
                )

        pos_avg_g = players["Pos"].map(POS_AVG_G_PER90).fillna(0.12)
        pos_avg_a = players["Pos"].map(POS_AVG_A_PER90).fillna(0.05)
        g_per_90_shrunk = g_shrink * g_per_90 + (1 - g_shrink) * pos_avg_g
        a_per_90_shrunk = a_shrink * a_per_90 + (1 - a_shrink) * pos_avg_a

        # Cap team strength multiplier to avoid extreme DGW/strong-team inflation
        t_str = (players["G_team"] / (league_avg_g * fc_safe)).clip(upper=MAX_TEAM_STRENGTH)

        players.loc[use_rates, "goal_prob"]   = (g_per_90_shrunk[use_rates] * (players.loc[use_rates, "xMins"] / 90.0) * t_str[use_rates]).clip(lower=0)
        players.loc[use_rates, "assist_prob"] = (a_per_90_shrunk[use_rates] * (players.loc[use_rates, "xMins"] / 90.0) * t_str[use_rates]).clip(lower=0)

    # ── Set piece bonuses (BV-fallback players only) ─────────────────────────
    # Rate-based players already have set piece contributions in their xG/xA.
    # For players with insufficient season data we add an explicit role bonus.
    if "pen_taker" in players.columns or "corner_taker" in players.columns:
        mins_per90 = players["xMins"] / 90.0
        non_rates  = ~use_rates
        if "pen_taker" in players.columns:
            pen_mask = non_rates & players["pen_taker"].fillna(False)
            players.loc[pen_mask, "goal_prob"] = (
                players.loc[pen_mask, "goal_prob"]
                + PEN_BONUS_PER90 * mins_per90[pen_mask]
            ).clip(lower=0)
        if "corner_taker" in players.columns:
            corner_mask = non_rates & players["corner_taker"].fillna(False)
            players.loc[corner_mask, "assist_prob"] = (
                players.loc[corner_mask, "assist_prob"]
                + CORNER_BONUS_PER90 * mins_per90[corner_mask]
            ).clip(lower=0)

    # ── CS / bonus / saves (unchanged) ───────────────────────────────────────
    players["cs_prob"]    = players["CS_team"] * players["cs_share"] * players["mins_ratio"]
    players["bonus_prob"] = np.clip(players["G_team"] / 3.0, 0.0, 1.0) * players["mins_ratio"] * 0.55
    players["save_pts"]   = np.where(
        players["Pos"] == "G",
        np.clip(1.2 - players["GC_team"], 0.0, 1.2) * players["mins_ratio"] * 0.35,
        0.0,
    )

    # ── Aggregate xPts ────────────────────────────────────────────────────────
    players["model_goal_pts"]   = players["goal_prob"]   * 6.0
    players["model_assist_pts"] = players["assist_prob"] * 3.0
    players["model_cs_pts"]     = np.where(
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

    players["model_xPts"]   = players["model_xPts"].round(2)
    players["goal_prob"]    = players["goal_prob"].round(4)
    players["assist_prob"]  = players["assist_prob"].round(4)
    players["cs_prob"]      = players["cs_prob"].round(4)
    players["bonus_prob"]   = players["bonus_prob"].round(4)
    players["save_pts"]     = players["save_pts"].round(3)

    players.drop(columns=["_use_rates", "_avail"], inplace=True, errors="ignore")
    return players
