import json
import os
import sys
from pathlib import Path
import warnings
import pandas as pd

# Allow imports from scripts/utils
ROOT = Path(__file__).resolve().parents[1]
SYS_SCRIPTS = ROOT / "scripts"
if str(SYS_SCRIPTS) not in sys.path:
    sys.path.append(str(SYS_SCRIPTS))

from utils.data_loaders import (
    load_fplreview_player_data,
    load_solio_player_data,
    melt_player_projections,
)

warnings.filterwarnings("ignore")


def load_config(config_path):
    with open(config_path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_player_projections(df, path):
    df.to_csv(path, index=False)
    print(f"Saved {len(df)} rows to {path}")


def main():
    config_path = ROOT / "config" / "config.json"
    config = load_config(config_path)

    fplreview_dir = Path(config.get("fplreview_dir", ROOT / "fplreview"))
    solio_dir = Path(config.get("solio_dir", ROOT / "solio"))
    output_dir = Path(config.get("player_outputs_dir", ROOT / "outputs"))
    output_dir.mkdir(parents=True, exist_ok=True)

    print("=== LOADING BENCHMARK DATA ===")
    print(f"FPLReview dir: {fplreview_dir}")
    print(f"Solio dir    : {solio_dir}")

    raw_fplreview = load_fplreview_player_data(fplreview_dir)
    raw_solio = load_solio_player_data(solio_dir)

    print("=== TRANSFORMING TO LONG PLAYER PROJECTIONS ===")
    fplreview_long = melt_player_projections(raw_fplreview)
    solio_long = melt_player_projections(raw_solio)

    save_player_projections(
        fplreview_long,
        output_dir / "fplreview_player_projections.csv"
    )
    save_player_projections(
        solio_long,
        output_dir / "solio_player_projections.csv"
    )

    if not fplreview_long.empty and not solio_long.empty:
        merged = pd.concat([fplreview_long, solio_long], ignore_index=True)
        save_player_projections(
            merged,
            output_dir / "player_projection_benchmarks.csv"
        )

    print("=== DONE ===")


if __name__ == "__main__":
    main()
