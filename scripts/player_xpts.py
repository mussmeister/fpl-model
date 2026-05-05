import json
import sys
from pathlib import Path
import warnings
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
SYS_SCRIPTS = ROOT / "scripts"
if str(SYS_SCRIPTS) not in sys.path:
    sys.path.append(str(SYS_SCRIPTS))

from utils.data_loaders import (
    load_season_results,
    fetch_odds,
    load_fixtures,
    load_fplreview_player_data,
    load_solio_player_data,
    melt_player_projections,
)
from utils.dc_model import fit_dc_ratings, run_projections
from utils.player_model import estimate_player_xpts

warnings.filterwarnings("ignore")


def load_config(config_path):
    with open(config_path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_csv(df, path):
    try:
        df.to_csv(path, index=False)
        print(f"Saved {len(df)} rows to {path}")
    except PermissionError as exc:
        alt_path = path.with_name(path.stem + "_new" + path.suffix)
        print(f"Permission denied writing {path}. Saving to {alt_path} instead.")
        df.to_csv(alt_path, index=False)
        print(f"Saved {len(df)} rows to {alt_path}")


def main():
    config_path = ROOT / "config" / "config.json"
    config = load_config(config_path)

    fplreview_dir = Path(config.get("fplreview_dir", ROOT / "fplreview"))
    solio_dir = Path(config.get("solio_dir", ROOT / "solio"))
    output_dir = Path(config.get("player_outputs_dir", ROOT / "outputs"))
    output_dir.mkdir(parents=True, exist_ok=True)

    print("=== LOADING BENCHMARK PLAYER DATA ===")
    raw_fplreview = load_fplreview_player_data(fplreview_dir)
    raw_solio = load_solio_player_data(solio_dir)

    fplreview_long = melt_player_projections(raw_fplreview)
    solio_long = melt_player_projections(raw_solio)

    print(f"  FPLReview rows: {len(fplreview_long)}")
    print(f"  Solio rows: {len(solio_long)}")

    if fplreview_long.empty:
        print("No FPLReview benchmark player data found. Exiting.")
        return

    print("=== LOADING TEAM DATA ===")
    season = config.get("season", "2526")
    df_results, all_teams, team_idx, avg_home, avg_away = load_season_results(season)

    odds_lookup = {}
    if config.get("odds_api_key"):
        try:
            odds_lookup = fetch_odds(
                config["odds_api_key"],
                bookmakers=config.get("odds_bookmakers", ["pinnacle"]),
            )
        except Exception as exc:
            print(f"  Warning: could not fetch odds: {exc}")
            odds_lookup = {}

    fixture_dir = ROOT / "fixtures"
    df_target, target_gws = load_fixtures(fixture_dir, n_gws=12)
    if df_target.empty:
        print("No upcoming fixtures found in fixtures/fixtures_all.csv. Exiting.")
        return

    model_inputs = pd.concat([fplreview_long, solio_long], ignore_index=True)
    model_inputs = model_inputs[model_inputs["GW"].isin(target_gws)].copy()
    model_inputs = model_inputs.sort_values(["Name", "Team", "GW", "xMins"], ascending=[True, True, True, False])
    model_inputs = model_inputs.drop_duplicates(["Name", "Team", "GW"], keep="first")
    print(f"  Model inputs after GW filter and dedupe: {len(model_inputs)} rows")

    # Build team fixture counts used for independent minutes projection.
    fixture_counts = pd.concat([
        df_target[["GW", "Home"]].rename(columns={"Home": "Team"}),
        df_target[["GW", "Away"]].rename(columns={"Away": "Team"}),
    ], ignore_index=True)
    fixture_counts = (
        fixture_counts.groupby(["GW", "Team"]).size().rename("fixtures").to_dict()
    )

    fplreview_stats = fplreview_long[fplreview_long["GW"].isin(target_gws)].copy()
    solio_stats = solio_long[solio_long["GW"].isin(target_gws)].copy()
    print(f"  Validation rows loaded: {len(fplreview_stats) + len(solio_stats)}")

    print("=== FITTING DC RATINGS ===")
    ratings, home_adv = fit_dc_ratings(
        config.get("xi", 0.002),
        config.get("dc_rho", -0.073),
        df_results,
        all_teams,
        team_idx,
        avg_home,
        avg_away,
        ah_weight=config.get("ah_weight", 0.0),
    )

    print("=== GENERATING TEAM PROJECTIONS ===")
    team_projections = run_projections(
        ratings,
        home_adv,
        config.get("shrinkage_weight", 0.9357),
        config.get("blend_weight", 0.9322),
        config.get("dc_rho", -0.073),
        df_target,
        target_gws,
        odds_lookup,
        avg_home,
        avg_away,
    )

    print("=== ESTIMATING PLAYER xPts ===")
    model_players = estimate_player_xpts(
        model_inputs,
        team_projections,
        fixture_counts=fixture_counts,
        use_source_minutes=False,
    )

    # Keep only independent model fields; drop benchmark source artifacts from the model file.
    model_save = model_players.drop(columns=[
        c for c in model_players.columns
        if c in {
            "source", "source_file", "Pts", "goals", "assists",
            "CS", "bonus", "cbit", "eo", "source_xMins", "model_delta"
        }
    ], errors="ignore")

    output_path = output_dir / "player_xpts_model.csv"
    save_csv(model_save, output_path)

    print("=== BUILDING VALIDATION OUTPUT ===")
    fplreview_stats = fplreview_stats.rename(columns={
        "xMins": "fplreview_xMins",
        "Pts": "fplreview_Pts",
        "source_file": "fplreview_source_file",
    })
    solio_stats = solio_stats.rename(columns={
        "xMins": "solio_xMins",
        "Pts": "solio_Pts",
        "source_file": "solio_source_file",
    })

    fplreview_stats = fplreview_stats.sort_values(["Name", "Team", "GW", "fplreview_xMins"], ascending=[True, True, True, False])
    solio_stats = solio_stats.sort_values(["Name", "Team", "GW", "solio_xMins"], ascending=[True, True, True, False])
    fplreview_stats = fplreview_stats.drop_duplicates(["Name", "Team", "GW"], keep="first")
    solio_stats = solio_stats.drop_duplicates(["Name", "Team", "GW"], keep="first")

    validation = model_players.merge(
        fplreview_stats[["Name", "Team", "GW", "fplreview_xMins", "fplreview_Pts", "fplreview_source_file"]],
        on=["Name", "Team", "GW"],
        how="left",
    )
    validation = validation.merge(
        solio_stats[["Name", "Team", "GW", "solio_xMins", "solio_Pts", "solio_source_file"]],
        on=["Name", "Team", "GW"],
        how="left",
    )
    validation["fplreview_model_delta"] = (
        validation["model_xPts"] - pd.to_numeric(validation.get("fplreview_Pts", 0), errors="coerce").fillna(0.0)
    ).round(2)
    validation["solio_model_delta"] = (
        validation["model_xPts"] - pd.to_numeric(validation.get("solio_Pts", 0), errors="coerce").fillna(0.0)
    ).round(2)

    compare_columns = [
        "Name",
        "Team",
        "GW",
        "fixture_count",
        "xMins",
        "model_xPts",
        "model_delta",
        "fplreview_xMins",
        "fplreview_Pts",
        "fplreview_model_delta",
        "fplreview_source_file",
        "solio_xMins",
        "solio_Pts",
        "solio_model_delta",
        "solio_source_file",
        "goal_prob",
        "assist_prob",
        "cs_prob",
        "bonus_prob",
        "save_pts",
        "appearance_pts",
        "model_goal_pts",
        "model_assist_pts",
        "model_cs_pts",
        "model_bonus_pts",
    ]
    compare_columns = [c for c in compare_columns if c in validation.columns]
    save_csv(validation[compare_columns], output_dir / "player_xpts_model_comparison.csv")

    print("=== DONE ===")


if __name__ == "__main__":
    main()
