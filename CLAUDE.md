# FPL Player xPts Model Project

## Overview
This project builds an independent player expected points (xPts) model for Fantasy Premier League (FPL), focusing on accurate projected minutes (xMins) without relying on benchmark source artifacts. The model aims to align with the average of FPLReview and Solio benchmarks while remaining fully independent.

## Tech Stack
- **Language**: Python 3.14
- **Core Libraries**:
  - pandas: Data manipulation and analysis
  - numpy: Numerical computations
  - joblib: Parallel processing for Monte Carlo simulations
- **Data Sources**:
  - FPLReview player projections (CSV)
  - Solio player projections (CSV)
  - Fixture data (CSV)
  - Historical match results for DC model training
- **Models**:
  - Dixon-Coles (DC) model for team ratings and projections
  - Custom player minute estimation based on buy value (BV) and position
  - xPts calculation incorporating goals, assists, clean sheets, bonuses, saves, and appearance points

## Architecture

### Directory Structure
```
config/
  config.json          # Model parameters (xi, dc_rho, shrinkage, etc.)
fixtures/
  fixtures_all.csv     # All upcoming fixtures
  fixtures_*.csv       # Filtered/summary fixtures
fplreview/
  *.csv                # FPLReview benchmark data
outputs/
  player_xpts_model.csv              # Independent model output
  player_xpts_model_comparison.csv   # Model vs benchmarks
scripts/
  player_xpts.py       # Main orchestrator
  utils/
    data_loaders.py    # Data loading and normalization
    dc_model.py        # Dixon-Coles team model
    player_model.py    # Player minute/xPts estimation
  notebooks/
    monte_carlo_*.py   # Parameter optimization scripts
solio/
  *.csv                # Solio benchmark data
```

### Data Flow
1. **Load Benchmarks**: FPLReview and Solio player data, normalize team names and positions
2. **Load Fixtures**: Upcoming GW fixtures, compute fixture counts per team
3. **Fit DC Ratings**: Train Dixon-Coles model on historical results
4. **Generate Team Projections**: Predict goals, clean sheets for each team/GW
5. **Estimate Player xPts**: For each player, estimate minutes based on BV/position, then calculate xPts
6. **Save Outputs**: Independent model file and comparison file with benchmarks

## Key Components

### Data Loaders (`utils/data_loaders.py`)
- `load_fplreview_player_data()`: Load and melt FPLReview wide-format projections
- `load_solio_player_data()`: Load and melt Solio projections
- `melt_player_projections()`: Convert wide GW columns to long format
- `load_fixtures()`: Load and filter fixtures for target GWs
- Team name normalization mappings (FDCO_TO_SHORT, SOLIO_TO_SHORT)

### DC Model (`utils/dc_model.py`)
- `fit_dc_ratings()`: Fit Dixon-Coles parameters using historical results
- `run_projections()`: Generate team-level projections (G, GC, CS) for upcoming fixtures
- Parameters: xi (variance), dc_rho (correlation), shrinkage_weight, blend_weight, ah_weight

### Player Model (`utils/player_model.py`)
- `_estimate_play_probability(pos, bv)`: Position-aware play probability based on BV thresholds
- `_estimate_minutes(pos, bv, fixtures)`: Calculate projected minutes using base minutes × skill factor × probability × fixtures
- `estimate_player_xpts()`: Full pipeline including appearance points, goal/assist/cs probabilities, xPts calculation
- Position codes: G (GK), D (DEF), M (MID), F (FWD)
- BV interpretation: Buy value as quality signal, not raw minutes

### Main Orchestrator (`player_xpts.py`)
- Loads config, data, fits DC model, generates projections, estimates players
- Saves independent model output (no benchmark columns)
- Saves comparison output with FPLReview/Solio for validation
- Uses fixture counts for DGW minute capping

## Conventions
- **Positions**: G/GK, D/DEF, M/MID, F/FWD/FW
- **Team Names**: Normalized to short forms (e.g., "Manchester City" → "Man City")
- **Duplicates**: Drop by Name/Team/GW, keep highest xMins
- **Numeric Handling**: Fill NaN with 0 for minutes/points
- **File Naming**: Outputs include timestamps for versioning
- **MAE Metric**: Mean Absolute Error for optimization against benchmarks
- **Independence**: Model must not carry benchmark artifacts; use_source_minutes=False for estimation

## Current Status

### Completed
- Independent xPts model with position-aware minute estimation
- Comparison output for validation against FPLReview/Solio
- Reduced zero-benchmark mismatches (410 → 261 rows with positive model minutes)
- BV-based play probability with position-specific curves

### In Progress
- Monte Carlo optimization for play probability multipliers
- Fine-tuning minute projections for flagged players (A.Becker, A.Jimenez, etc.)

### Outputs
- `player_xpts_model.csv`: Independent projections (Name, Team, GW, Pos, BV, fixture_count, xMins, model_xPts, probabilities)
- `player_xpts_model_comparison.csv`: Model + benchmark columns for analysis

## Decisions and Rationale

### Independence
- Model must be separate from benchmarks to avoid overfitting
- Use BV as quality proxy, not direct minute input
- Target: Average of FPLReview and Solio xMins as ground truth

### Minute Estimation
- BV represents buy value, not playing time
- Position-aware probabilities: GKs and DEFs have higher thresholds than MID/FWD
- Fixture count affects DGW minutes (capped at 90 × fixtures)
- Conservative for low-BV players to avoid overestimating bench players

### Optimization
- Monte Carlo for parameter tuning (multipliers on play probabilities)
- MAE minimization against average benchmark minutes
- Parallel processing for efficiency

### Data Handling
- Melt wide GW projections to long format for consistency
- Normalize team names across sources
- Handle missing data with fillna(0)

## Future Work
- Complete Monte Carlo optimization for best multipliers
- Validate against additional GWs
- Incorporate more features (form, injuries) if data available
- Extend to full FPL optimizer (team selection, captaincy)