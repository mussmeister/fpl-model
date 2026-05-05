import numpy as np
import pandas as pd
from scipy.stats import poisson
from scipy.optimize import minimize
from itertools import product


def fit_dc_ratings(xi, dc_rho, df_results, all_teams, team_idx,
                   league_avg_home, league_avg_away, ah_weight=0.0):
    """
    Fit Dixon-Coles attack/defence ratings using time-weighted MLE.

    ah_weight controls how much Asian Handicap implied goals are blended
    into the target variable alongside actual goals:
      0.0 = pure actual goals (original behaviour)
      1.0 = pure AH-implied goals
      0.5 = 50/50 blend

    AH-implied goals are only used if AHh column is present in df_results.
    Falls back to actual goals for any match with a missing AHh value.

    Returns (ratings dict, home_advantage float).
    """
    n_teams = len(all_teams)
    weights = np.exp(-xi * df_results["days_ago"].values)

    # Build target goals — blend actual with AH-implied if available
    has_ah = "AHh" in df_results.columns and ah_weight > 0.0

    if has_ah:
        ah_vals = df_results["AHh"].values.astype(float)

        # Where AHh is NaN, fall back to actual goals for that match
        imp_home_vals = np.where(
            np.isnan(ah_vals),
            df_results["FTHG"].values.astype(float),
            np.clip(league_avg_home - (ah_vals / 2), 0.3, 4.0)
        )
        imp_away_vals = np.where(
            np.isnan(ah_vals),
            df_results["FTAG"].values.astype(float),
            np.clip(league_avg_away + (ah_vals / 2), 0.3, 4.0)
        )

        target_home = ((1 - ah_weight) * df_results["FTHG"].values.astype(float)
                       + ah_weight * imp_home_vals)
        target_away = ((1 - ah_weight) * df_results["FTAG"].values.astype(float)
                       + ah_weight * imp_away_vals)
    else:
        target_home = df_results["FTHG"].values.astype(float)
        target_away = df_results["FTAG"].values.astype(float)

    def tau(i, j, lh, la):
        if   i == 0 and j == 0: return 1 - lh * la * dc_rho
        elif i == 0 and j == 1: return 1 + lh * dc_rho
        elif i == 1 and j == 0: return 1 + la * dc_rho
        elif i == 1 and j == 1: return 1 - dc_rho
        return 1.0

    def neg_ll(params):
        attack   = np.concatenate([[0.0], params[:n_teams - 1]])
        defence  = params[n_teams - 1: 2 * n_teams - 1]
        home_adv = params[2 * n_teams - 1]
        total    = 0.0
        for idx, row in enumerate(df_results.itertuples()):
            hi = team_idx.get(row.HomeTeam)
            ai = team_idx.get(row.AwayTeam)
            if hi is None or ai is None: continue
            lh = max(np.exp(attack[hi] + defence[ai] + home_adv) * league_avg_home, 1e-6)
            la = max(np.exp(attack[ai] + defence[hi]) * league_avg_away, 1e-6)

            hg = target_home[idx]
            ag = target_away[idx]

            # Fall back to actual goals if blended value is NaN
            if np.isnan(hg): hg = float(row.FTHG)
            if np.isnan(ag): ag = float(row.FTAG)

            hg_int = int(round(hg))
            ag_int = int(round(ag))

            ll = (poisson.logpmf(hg_int, lh) + poisson.logpmf(ag_int, la)
                  + np.log(max(tau(hg_int, ag_int, lh, la), 1e-10)))
            total += weights[idx] * ll
        return -total

    res = minimize(neg_ll, np.zeros(2 * n_teams),
                   method="L-BFGS-B",
                   options={"maxiter": 500, "ftol": 1e-8, "gtol": 1e-5})

    attack_p  = np.concatenate([[0.0], res.x[:n_teams - 1]])
    defence_p = res.x[n_teams - 1: 2 * n_teams - 1]
    home_adv  = res.x[2 * n_teams - 1]
    attack_p  -= attack_p.mean()
    defence_p -= defence_p.mean()

    ratings = {t: {"attack":  float(attack_p[team_idx[t]]),
                   "defence": float(defence_p[team_idx[t]])}
               for t in all_teams}
    return ratings, float(home_adv)


def remove_margin(h, d, a):
    """Remove bookmaker margin from h2h odds. Returns (ph, pd, pa)."""
    raw = np.array([1 / h, 1 / d, 1 / a])
    return raw / raw.sum()


def fit_odds_lambdas(ph, pd_m, pa, dc_rho):
    """
    Back out Poisson lambdas implied by market probabilities.
    Returns (lambda_home, lambda_away).
    """
    def tau_p(i, j, lh, la):
        if   i == 0 and j == 0: return 1 - lh * la * dc_rho
        elif i == 0 and j == 1: return 1 + lh * dc_rho
        elif i == 1 and j == 0: return 1 + la * dc_rho
        elif i == 1 and j == 1: return 1 - dc_rho
        return 1.0

    def mprobs(lh, la):
        ph_ = pd_ = pa_ = 0.0
        for i, j in product(range(11), repeat=2):
            p = poisson.pmf(i, lh) * poisson.pmf(j, la) * tau_p(i, j, lh, la)
            if i > j:    ph_ += p
            elif i == j: pd_ += p
            else:        pa_ += p
        return ph_, pd_, pa_

    def loss(x):
        lh, la = np.exp(x)
        p1, p2, p3 = mprobs(lh, la)
        return (p1 - ph) ** 2 + (p2 - pd_m) ** 2 + (p3 - pa) ** 2

    res = minimize(loss, [np.log(1.5), np.log(1.1)], method="Nelder-Mead",
                   options={"xatol": 1e-6, "fatol": 1e-8, "maxiter": 5000})
    return np.exp(res.x[0]), np.exp(res.x[1])


def project_fixture(home, away, ratings, home_adv,
                    shrinkage_weight, blend_weight, dc_rho,
                    league_avg_home, league_avg_away, odds=None):
    """
    Project expected goals for a single fixture.
    Returns (lambda_home, lambda_away, method).
    """
    hr = ratings.get(home, {"attack": 0., "defence": 0.})
    ar = ratings.get(away, {"attack": 0., "defence": 0.})
    lh_rat = np.exp(hr["attack"] + ar["defence"] + home_adv) * league_avg_home
    la_rat = np.exp(ar["attack"] + hr["defence"]) * league_avg_away

    if odds is not None:
        ph, pd_m, pa = remove_margin(odds["home_odds"], odds["draw_odds"], odds["away_odds"])
        lh_raw, la_raw = fit_odds_lambdas(ph, pd_m, pa, dc_rho)
        lh_odds = shrinkage_weight * lh_raw + (1 - shrinkage_weight) * league_avg_home
        la_odds = shrinkage_weight * la_raw + (1 - shrinkage_weight) * league_avg_away
        lh = blend_weight * lh_odds + (1 - blend_weight) * lh_rat
        la = blend_weight * la_odds + (1 - blend_weight) * la_rat
        method = "Blend"
    else:
        lh, la = lh_rat, la_rat
        method = "Ratings"

    return float(lh), float(la), method


def run_projections(ratings, home_adv, shrinkage_weight, blend_weight, dc_rho,
                    df_target, target_gws, odds_lookup,
                    league_avg_home, league_avg_away):
    """
    Generate per-team projected stats for all target GWs.
    Returns dict of DataFrames keyed by GW.
    """
    results = {}
    for GW in target_gws:
        gw_fixtures = df_target[df_target["GW"] == GW]
        rows = []
        for _, fix in gw_fixtures.iterrows():
            home, away = fix["Home"], fix["Away"]
            odds = odds_lookup.get((home, away))
            lh, la, method = project_fixture(
                home, away, ratings, home_adv,
                shrinkage_weight, blend_weight, dc_rho,
                league_avg_home, league_avg_away, odds)
            rows.append({"Team": home, "G": lh, "GC": la,
                         "CS": float(poisson.pmf(0, la)), "Method": method})
            rows.append({"Team": away, "G": la, "GC": lh,
                         "CS": float(poisson.pmf(0, lh)), "Method": method})

        if not rows: continue
        df_per = pd.DataFrame(rows)

        def agg(g):
            return pd.Series({
                "G":  g["G"].sum(),
                "GC": g["GC"].sum(),
                "GD": g["G"].sum() - g["GC"].sum(),
                "CS": g["CS"].prod(),
                "Method": g["Method"].iloc[0] if g["Method"].nunique() == 1 else "Mixed"
            })

        df_agg = (df_per.groupby("Team", sort=True)
                        .apply(agg, include_groups=False)
                        .reset_index())
        results[GW] = df_agg
    return results


def score_projections(results, solio_by_gw):
    """
    Score projections against Solio benchmarks.
    Returns dict with MAEs and combined score.
    """
    blend_errors   = {"G": [], "GC": [], "GD": [], "CS": []}
    ratings_errors = {"G": [], "GC": [], "GD": [], "CS": []}

    for GW, df_proj in results.items():
        if GW not in solio_by_gw: continue
        df_s    = solio_by_gw[GW]
        df_comp = df_proj[["Team", "G", "GC", "GD", "CS", "Method"]].merge(
            df_s, on="Team", how="inner")
        if len(df_comp) == 0: continue

        df_comp = df_comp[
            ~((df_comp["Solio_G"]  > df_comp["G"]  * 1.5) |
              (df_comp["Solio_GC"] > df_comp["GC"] * 1.5))
        ]

        for _, r in df_comp.iterrows():
            target = blend_errors if r["Method"] == "Blend" else ratings_errors
            for col in ["G", "GC", "GD", "CS"]:
                target[col].append(abs(r[col] - r[f"Solio_{col}"]))

    def mean_or_nan(lst):
        return float(np.mean(lst)) if lst else float("nan")

    blend_mae   = {c: mean_or_nan(blend_errors[c])   for c in ["G", "GC", "GD", "CS"]}
    ratings_mae = {c: mean_or_nan(ratings_errors[c]) for c in ["G", "GC", "GD", "CS"]}

    b_score = (blend_mae["GD"] * 0.5 + blend_mae["G"] * 0.3 + blend_mae["CS"] * 0.2
               if not any(np.isnan(v) for v in blend_mae.values()) else float("nan"))
    r_score = (ratings_mae["GD"] * 0.5 + ratings_mae["G"] * 0.3 + ratings_mae["CS"] * 0.2
               if not any(np.isnan(v) for v in ratings_mae.values()) else float("nan"))

    scores_available = [s for s in [b_score, r_score] if not np.isnan(s)]
    combined = float(np.mean(scores_available)) if scores_available else float("nan")

    return {
        "blend_mae":      blend_mae,
        "ratings_mae":    ratings_mae,
        "blend_score":    b_score,
        "ratings_score":  r_score,
        "combined_score": combined,
        "blend_n":   sum(len(v) for v in blend_errors.values()) // 4,
        "ratings_n": sum(len(v) for v in ratings_errors.values()) // 4,
    }