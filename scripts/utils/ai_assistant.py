"""
AI assistant utilities — FPL squad fetcher, prompt builder, provider calls.

Supports Gemini Flash (free) and Claude Sonnet (swap via config ai_provider).
"""
import sqlite3
import requests
import pandas as pd
from pathlib import Path

FPL_BASE       = "https://fantasy.premierleague.com/api"
BOOTSTRAP_URL  = f"{FPL_BASE}/bootstrap-static/"
PICKS_URL      = f"{FPL_BASE}/entry/{{team_id}}/event/{{gw}}/picks/"
HISTORY_URL    = f"{FPL_BASE}/entry/{{team_id}}/history/"
ELEMENT_URL    = f"{FPL_BASE}/element-summary/{{player_id}}/"

POS_MAP = {1: "GK", 2: "DEF", 3: "MID", 4: "FWD"}
_HEADERS = {"User-Agent": "FPL-Model/1.0"}

# ── FPL data fetchers ─────────────────────────────────────────────────────────

def fetch_bootstrap():
    r = requests.get(BOOTSTRAP_URL, headers=_HEADERS, timeout=30)
    r.raise_for_status()
    return r.json()


def fetch_picks(team_id: int, gw: int) -> list[dict]:
    url = PICKS_URL.format(team_id=team_id, gw=gw)
    r = requests.get(url, headers=_HEADERS, timeout=30)
    r.raise_for_status()
    data = r.json()
    picks = data.get("picks", [])
    chips = data.get("active_chip")
    return picks, chips


def fetch_team_info(team_id: int) -> dict:
    url = f"{FPL_BASE}/entry/{team_id}/"
    r = requests.get(url, headers=_HEADERS, timeout=30)
    r.raise_for_status()
    return r.json()


def fetch_element_history(player_id: int) -> list[dict]:
    url = ELEMENT_URL.format(player_id=player_id)
    r = requests.get(url, headers=_HEADERS, timeout=30)
    r.raise_for_status()
    return r.json().get("history", [])


def get_last_n_gw_points(player_id: int, n: int = 5) -> list[int]:
    history = fetch_element_history(player_id)
    recent = sorted(history, key=lambda x: x["round"])[-n:]
    return [h["total_points"] for h in recent]


# ── Squad builder ─────────────────────────────────────────────────────────────

def build_squad(team_id: int, gw: int, bootstrap: dict) -> tuple[list[dict], dict]:
    """Return (squad_rows, meta) where meta has free_transfers, bank, chip."""
    player_map = {p["id"]: p for p in bootstrap.get("elements", [])}
    team_map   = {t["id"]: t["short_name"] for t in bootstrap.get("teams", [])}

    picks, active_chip = fetch_picks(team_id, gw)

    squad = []
    for pick in picks:
        pid  = pick["element"]
        p    = player_map.get(pid, {})
        squad.append({
            "player_id":  pid,
            "name":       p.get("web_name", "Unknown"),
            "team_short": team_map.get(p.get("team"), "?"),
            "pos":        POS_MAP.get(p.get("element_type"), "?"),
            "bv":         p.get("now_cost", 0) / 10.0,
            "is_captain": pick.get("is_captain", False),
            "is_vice":    pick.get("is_vice_captain", False),
            "multiplier": pick.get("multiplier", 1),
            "position":   pick.get("position", 99),   # 1-11 starter, 12-15 bench
            "form":       float(p.get("form", 0) or 0),
            "season_pts": int(p.get("total_points", 0)),
        })

    try:
        info  = fetch_team_info(team_id)
        bank  = info.get("last_deadline_bank", 0) / 10.0
        ft    = info.get("last_deadline_free_transfers", 1)
    except Exception:
        bank, ft = 0.0, 1

    meta = {"free_transfers": ft, "bank": bank, "active_chip": active_chip}
    return squad, meta


# ── Projections from DB ───────────────────────────────────────────────────────

def get_squad_projections(squad: list[dict], db_path: Path, target_gws: list[int]) -> dict:
    """Return {player_name: {gw: {xpts, xmins}}} for squad members."""
    names = [p["name"] for p in squad]
    result = {}
    try:
        with sqlite3.connect(str(db_path)) as conn:
            for name in names:
                rows = pd.read_sql("""
                    WITH latest AS (
                        SELECT gw, MAX(timestamp) AS max_ts
                        FROM player_projection_model WHERE name = ?
                        GROUP BY gw
                    )
                    SELECT m.gw, m.xmins, m.xpts, m.goal_prob, m.cs_prob
                    FROM player_projection_model m
                    JOIN latest ON m.gw = latest.gw AND m.timestamp = latest.max_ts
                    WHERE m.name = ?
                    ORDER BY m.gw
                """, conn, params=(name, name))
                if not rows.empty:
                    result[name] = {
                        int(r["gw"]): {
                            "xpts":  round(float(r["xpts"]),  2),
                            "xmins": round(float(r["xmins"]), 1),
                        }
                        for _, r in rows.iterrows()
                        if int(r["gw"]) in target_gws
                    }
    except Exception:
        pass
    return result


def get_squad_benchmarks(squad: list[dict], db_path: Path, target_gws: list[int]) -> dict:
    """Return {player_name: {gw: avg_xpts}} averaging Solio + FPLReview latest snapshots."""
    names = [p["name"] for p in squad]
    result = {}
    try:
        with sqlite3.connect(str(db_path)) as conn:
            for name in names:
                rows = pd.read_sql("""
                    WITH latest AS (
                        SELECT source, MAX(ingested_at) AS max_ts
                        FROM player_projection_snapshots
                        WHERE name = ?
                        GROUP BY source
                    )
                    SELECT s.gw, AVG(CAST(s.pts AS REAL)) AS avg_xpts
                    FROM player_projection_snapshots s
                    JOIN latest ON s.source = latest.source
                         AND s.ingested_at = latest.max_ts
                    WHERE s.name = ?
                    GROUP BY s.gw
                """, conn, params=(name, name))
                if not rows.empty:
                    result[name] = {
                        int(r["gw"]): round(float(r["avg_xpts"]), 2)
                        for _, r in rows.iterrows()
                        if int(r["gw"]) in target_gws
                    }
    except Exception:
        pass
    return result


def get_squad_opponents(squad: list[dict], db_path: Path, target_gws: list[int]) -> dict:
    """Return {(player_name, gw): 'OPP (H/A)'} from the projections table."""
    names = [p["name"] for p in squad]
    result = {}
    gw_ph = ",".join("?" * len(target_gws))
    try:
        with sqlite3.connect(str(db_path)) as conn:
            ts_row = conn.execute(
                f"SELECT MAX(timestamp) FROM projections WHERE gw IN ({gw_ph})",
                target_gws,
            ).fetchone()
            if not ts_row or not ts_row[0]:
                return result
            latest_ts = ts_row[0]
            for name in names:
                team_row = conn.execute(
                    "SELECT team FROM player_projection_model WHERE name = ? LIMIT 1",
                    (name,),
                ).fetchone()
                if not team_row:
                    continue
                team = team_row[0]
                rows = conn.execute(
                    f"""SELECT gw, opponent, home_away FROM projections
                        WHERE team = ? AND gw IN ({gw_ph}) AND timestamp = ?
                        ORDER BY gw""",
                    (team, *target_gws, latest_ts),
                ).fetchall()
                for gw, opp, ha in rows:
                    result[(name, int(gw))] = f"{opp} ({ha})"
    except Exception:
        pass
    return result


def fetch_live_points(gw: int) -> dict:
    """Return {player_id: total_points} for a finished GW."""
    url = f"{FPL_BASE}/event/{gw}/live/"
    r = requests.get(url, headers=_HEADERS, timeout=30)
    r.raise_for_status()
    return {e["id"]: e["stats"]["total_points"] for e in r.json().get("elements", [])}


# ── Prompt builder ────────────────────────────────────────────────────────────

SYSTEM_PROMPT = """You are an expert Fantasy Premier League (FPL) assistant.
You have access to a custom Dixon-Coles statistical model that produces expected
points (xPts) projections grounded in team attack/defence ratings and each
player's season goal/assist rate.

Guidelines:
- Use xPts as your quantitative foundation. Higher xPts = stronger pick.
- Flag rotation risk for players whose clubs have upcoming FA Cup / Champions
  League / Europa League fixtures — even if the model looks good.
- Be concise and direct. Use markdown headers and bullet points.
- When uncertain (e.g. injury not confirmed), say so rather than guessing.
- Format currency as £Xm throughout."""


def build_prompt(squad: list[dict], meta: dict,
                 projections: dict, target_gws: list[int],
                 current_gw: int,
                 benchmarks: dict | None = None,
                 opponents: dict | None = None,
                 actual_points: dict | None = None,
                 squad_gw: int | None = None) -> str:

    benchmarks  = benchmarks  or {}
    opponents   = opponents   or {}
    actual_pts  = actual_points or {}
    squad_gw    = squad_gw or current_gw

    starters = sorted([p for p in squad if p["position"] <= 11], key=lambda x: x["position"])
    bench    = sorted([p for p in squad if p["position"] >  11], key=lambda x: x["position"])

    def fmt_player(p: dict) -> str:
        actual = actual_pts.get(p["player_id"])
        actual_str = f"  act={actual:>3}" if actual is not None else ""
        flag  = " [C]" if p["is_captain"] else (" [V]" if p["is_vice"] else "")
        proj  = projections.get(p["name"], {})
        bnch  = benchmarks.get(p["name"], {})
        cols  = []
        for g in target_gws:
            opp   = opponents.get((p["name"], g), "?")
            mdl   = f"{proj[g]['xpts']:5.2f}" if g in proj else "  —  "
            bench_v = f"{bnch[g]:5.2f}" if g in bnch else "  —  "
            cols.append(f"{opp:>8} {mdl} {bench_v}")
        return (f"  {p['pos']:3}  {p['name']:<22}{p['team_short']:>5}  "
                f"£{p['bv']:.1f}m{actual_str}  {'  '.join(cols)}{flag}")

    gw_header_line = (
        f"{'':5}{'Player':<22}{'Team':>5}  {'Price':>6}"
        + ("  Actual" if actual_pts else "")
        + "  "
        + "  ".join(f"GW{g}(Opp / Mdl / Bnch)" for g in target_gws)
    )

    starters_block = "\n".join(fmt_player(p) for p in starters)
    bench_block    = "\n".join(fmt_player(p) for p in bench)

    gw_note = f"GW{squad_gw} just finished." if actual_pts else f"GW{current_gw} in progress/upcoming."

    prompt = f"""**Squad GW: {squad_gw}** | {gw_note}
Free transfers: {meta['free_transfers']}  |  Bank: £{meta['bank']:.1f}m
{f"Active chip: {meta['active_chip']}" if meta['active_chip'] else ""}

Projecting ahead for: {', '.join(f'GW{g}' for g in target_gws)}
Mdl = Dixon-Coles model xPts  |  Bnch = avg of FPLReview & Solio xPts  |  Actual = GW{squad_gw} pts scored

---
## My Squad

{gw_header_line}
STARTERS:
{starters_block}

BENCH (position order):
{bench_block}

---

Please provide a structured analysis with these sections:

### Last GW Review
{f"Comment briefly on GW{squad_gw} actual scores vs expectations. Flag any overperformers or underperformers." if actual_pts else "N/A — GW in progress."}

### Captain & Vice-Captain (GW{target_gws[0] if target_gws else current_gw})
Recommend captain and vice with clear reasoning (xPts + opponent + risk factors).

### Bench Order
Recommended bench order 12→15 with brief justification.

### Rotation / Injury Risks
Flag any starters who carry rotation or injury risk, especially if their club has non-PL fixtures (FA Cup, CL, EL).

### Transfer Recommendations
Given the xPts data and fixture run, suggest 1–2 transfers if warranted. State player out, player in, and expected xPts gain over the next {len(target_gws)} GWs. If the squad looks solid, say so.

### Key Observations
DGW opportunities, fixture swings, differentials worth considering, or any model vs benchmark divergences worth noting.
"""
    return prompt


# ── AI provider calls ─────────────────────────────────────────────────────────

def call_gemini(system_prompt: str, user_prompt: str, api_key: str,
                model: str = "gemini-2.5-flash"):
    from google import genai
    from google.genai import types
    client = genai.Client(api_key=api_key)
    for chunk in client.models.generate_content_stream(
        model=model,
        contents=user_prompt,
        config=types.GenerateContentConfig(system_instruction=system_prompt),
    ):
        if chunk.text:
            yield chunk.text


def call_claude(system_prompt: str, user_prompt: str, api_key: str,
                model: str = "claude-sonnet-4-6"):
    import anthropic
    client = anthropic.Anthropic(api_key=api_key)
    with client.messages.stream(
        model=model,
        max_tokens=2048,
        system=system_prompt,
        messages=[{"role": "user", "content": user_prompt}],
    ) as stream:
        for text in stream.text_stream:
            yield text


def call_ai(system_prompt: str, user_prompt: str,
            provider: str, api_key: str) -> "generator":
    if provider == "claude":
        return call_claude(system_prompt, user_prompt, api_key)
    return call_gemini(system_prompt, user_prompt, api_key)
