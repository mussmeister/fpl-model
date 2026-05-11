"""
AI FPL Assistant — squad analysis, captaincy, bench, and transfer advice.

Supports Gemini Flash (free) and Claude Sonnet (via config ai_provider).
"""
import sys
import json
from pathlib import Path

import pandas as pd
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from utils.auth import require_auth, show_logout_button
from utils.ai_assistant import (
    fetch_bootstrap,
    build_squad,
    get_squad_projections,
    get_squad_benchmarks,
    get_squad_opponents,
    fetch_live_points,
    build_prompt,
    call_ai,
    SYSTEM_PROMPT,
)

ROOT    = Path(__file__).resolve().parents[2]
DB_PATH = ROOT / "outputs" / "projections_history.db"
CFG_PATH = ROOT / "config" / "config.json"

st.set_page_config(page_title="FPL AI Assistant", layout="wide", page_icon="🤖")

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Barlow+Condensed:wght@400;600;700;800&family=Barlow:wght@400;500;600&display=swap');
html, body, [class*="css"], .stApp { font-family: 'Barlow', sans-serif !important; }
h1, h2, h3 { font-family: 'Barlow Condensed', sans-serif !important; font-weight: 800 !important; }
.block-container { padding-left: 1rem !important; padding-right: 1rem !important; }
</style>
""", unsafe_allow_html=True)

require_auth()
show_logout_button()

if st.button("← Back to Fixtures"):
    st.switch_page("fpl_app.py")

st.title("🤖 FPL AI Assistant")
st.caption("Squad analysis powered by your Dixon-Coles xPts model + AI.")

# ── Config ────────────────────────────────────────────────────────────────────

@st.cache_data(ttl=60)
def load_config():
    return json.loads(CFG_PATH.read_text())

cfg = load_config()

# ── FPL ID gate ───────────────────────────────────────────────────────────────

if "fpl_id" not in st.session_state:
    st.session_state.fpl_id = None

if not st.session_state.fpl_id:
    try:
        from utils import user_db
        _email = st.session_state.get("auth_email", "")
        if _email:
            _profile = user_db.get_profile(_email)
            _stored_id = _profile.get("fpl_team_id")
            if _stored_id:
                st.session_state.fpl_id = str(_stored_id)
                st.session_state["_fpl_id_from_profile"] = True
    except Exception:
        pass

if not st.session_state.fpl_id:
    st.markdown("### Enter your FPL Manager ID")
    st.markdown("Find it in your FPL URL: `fantasy.premierleague.com/entry/`**123456**`/history`")
    col_a, col_b = st.columns([3, 1])
    with col_a:
        entered = st.text_input("FPL ID", placeholder="e.g. 1102131", label_visibility="collapsed")
    with col_b:
        go = st.button("Load", type="primary", use_container_width=True)
    if go:
        if entered.strip().isdigit():
            st.session_state.fpl_id = entered.strip()
            st.rerun()
        else:
            st.error("Please enter a valid numeric ID")
    st.stop()

fpl_id = int(st.session_state.fpl_id)

# ── Sidebar ───────────────────────────────────────────────────────────────────

with st.sidebar:
    if st.session_state.get("_fpl_id_from_profile"):
        try:
            from utils import user_db as _udb
            _pname = _udb.get_profile(st.session_state.get("auth_email", "")).get("fpl_team_name", "")
            st.caption(f"**{_pname or 'FPL ID'}** `{fpl_id}`")
        except Exception:
            st.caption(f"FPL ID: `{fpl_id}`")
    else:
        st.caption(f"FPL ID: `{fpl_id}`")
        if st.button("Change ID"):
            st.session_state.fpl_id = None
            st.session_state.pop("_fpl_id_from_profile", None)
            st.rerun()

    st.markdown("---")
    st.markdown("**AI Provider**")
    provider_default = cfg.get("ai_provider", "gemini")
    provider = st.radio(
        "Provider",
        options=["gemini", "claude"],
        index=0 if provider_default == "gemini" else 1,
        format_func=lambda x: "✨ Gemini Flash (free)" if x == "gemini" else "🧠 Claude Sonnet",
        label_visibility="collapsed",
    )

    if provider == "gemini":
        api_key = cfg.get("gemini_api_key", "")
        if not api_key:
            api_key = st.text_input("Gemini API key", type="password",
                                    help="Get free key at aistudio.google.com")
    else:
        api_key = cfg.get("claude_api_key", "")
        if not api_key:
            api_key = st.text_input("Anthropic API key", type="password",
                                    help="console.anthropic.com — billed per token")

    st.markdown("---")
    st.markdown("**Gameweeks ahead to analyse**")
    n_gws = st.slider("Next N gameweeks", min_value=1, max_value=5, value=3)

# ── Bootstrap + GW context ────────────────────────────────────────────────────

@st.cache_data(ttl=300, show_spinner="Fetching FPL data…")
def load_bootstrap():
    return fetch_bootstrap()

def get_gw_context(bootstrap: dict):
    """Return (squad_gw, gw_finished, first_upcoming_gw)."""
    events = bootstrap.get("events", [])
    current = next((ev for ev in events if ev.get("is_current")), None)
    nxt     = next((ev for ev in events if ev.get("is_next")), None)
    prev    = next((ev for ev in events if ev.get("is_previous")), None)

    if current:
        gw = current["id"]
        finished = bool(current.get("finished", False))
        first_upcoming = gw + 1 if finished else gw
        return gw, finished, first_upcoming
    if nxt:
        gw = nxt["id"] - 1
        return gw, True, nxt["id"]
    return 1, False, 1

try:
    bootstrap = load_bootstrap()
except Exception as e:
    st.error(f"Could not fetch FPL data: {e}")
    st.stop()

squad_gw, gw_finished, first_upcoming = get_gw_context(bootstrap)
target_gws = list(range(first_upcoming, first_upcoming + n_gws))

# ── Load squad ────────────────────────────────────────────────────────────────

@st.cache_data(ttl=300, show_spinner="Loading squad…")
def load_squad(team_id: int, gw: int, _bootstrap: dict):
    return build_squad(team_id, gw, _bootstrap)

try:
    squad, meta = load_squad(fpl_id, squad_gw, bootstrap)
except Exception:
    # GW picks not yet submitted — try previous GW
    try:
        squad, meta = load_squad(fpl_id, squad_gw - 1, bootstrap)
        squad_gw -= 1
        gw_finished = True
        target_gws = list(range(squad_gw + 1, squad_gw + 1 + n_gws))
    except Exception as e:
        st.error(f"Could not load squad: {e}")
        st.info("Make sure your FPL ID is correct and GW picks have been submitted.")
        st.stop()

squad_names = [p["name"] for p in squad]

# ── Load projections, benchmarks, opponents, actual points ────────────────────

@st.cache_data(ttl=300, show_spinner="Loading projections…")
def load_all_data(names, db, gws, finished_gw):
    projs   = get_squad_projections([{"name": n} for n in names], db, gws)
    benchs  = get_squad_benchmarks([{"name": n} for n in names], db, gws)
    opps    = get_squad_opponents([{"name": n} for n in names], db, gws)
    actuals = {}
    if finished_gw:
        try:
            actuals = fetch_live_points(finished_gw)
        except Exception:
            pass
    return projs, benchs, opps, actuals

projections, benchmarks, opponents, actual_pts = load_all_data(
    squad_names, str(DB_PATH), target_gws, squad_gw if gw_finished else None
)

# ── Squad display ─────────────────────────────────────────────────────────────

gw_label = f"GW{squad_gw}" + (" ✓ finished" if gw_finished else " — in progress")
st.markdown(f"### {gw_label} Squad")
st.caption(
    f"Free transfers: **{meta['free_transfers']}**  |  "
    f"Bank: **£{meta['bank']:.1f}m**"
    + (f"  |  Chip: **{meta['active_chip']}**" if meta.get("active_chip") else "")
)

POS_ORDER = {"GK": 0, "DEF": 1, "MID": 2, "FWD": 3}

starters = sorted([p for p in squad if p["position"] <= 11],
                  key=lambda x: (POS_ORDER.get(x["pos"], 9), x["position"]))
bench    = sorted([p for p in squad if p["position"] > 11],
                  key=lambda x: x["position"])


def build_squad_df(players):
    rows = []
    for p in players:
        proj  = projections.get(p["name"], {})
        bench_ = benchmarks.get(p["name"], {})
        flag  = "⚽" if p["is_captain"] else ("🔰" if p["is_vice"] else "")
        row   = {
            "Pos":    p["pos"],
            "Player": (flag + " " if flag else "") + p["name"],
            "Team":   p["team_short"],
            "£":      f"£{p['bv']:.1f}m",
        }
        if gw_finished:
            pts = actual_pts.get(p["player_id"])
            row[f"GW{squad_gw} Actual"] = float(pts) if pts is not None else None

        for g in target_gws:
            row[f"GW{g} Opp"]  = opponents.get((p["name"], g), "—")
            row[f"GW{g} Mdl"]  = proj[g]["xpts"]  if g in proj  else None
            row[f"GW{g} Bnch"] = bench_.get(g)
        rows.append(row)
    return pd.DataFrame(rows)


df_starters = build_squad_df(starters)
df_bench    = build_squad_df(bench)

# Column config — text for Opp/Player, number for everything numeric
col_cfg = {
    "Player": st.column_config.TextColumn("Player", width="large"),
    "Team":   st.column_config.TextColumn("Team",   width="small"),
}
if gw_finished:
    col_cfg[f"GW{squad_gw} Actual"] = st.column_config.NumberColumn(
        f"GW{squad_gw} Act", format="%.0f", width="small"
    )
for g in target_gws:
    col_cfg[f"GW{g} Opp"]  = st.column_config.TextColumn(f"GW{g} Opp",  width="small")
    col_cfg[f"GW{g} Mdl"]  = st.column_config.NumberColumn(f"GW{g} Mdl",  format="%.2f", width="small")
    col_cfg[f"GW{g} Bnch"] = st.column_config.NumberColumn(f"GW{g} Bnch", format="%.2f", width="small")

with st.expander("Starters", expanded=True):
    st.dataframe(df_starters, use_container_width=True, hide_index=True, column_config=col_cfg)

with st.expander("Bench", expanded=True):
    st.dataframe(df_bench, use_container_width=True, hide_index=True, column_config=col_cfg)

# ── AI analysis ───────────────────────────────────────────────────────────────

st.markdown("---")
st.markdown("### AI Analysis")

if not api_key:
    st.warning(
        "Add your API key in the sidebar to generate analysis.\n\n"
        "- **Gemini Flash**: free at [aistudio.google.com](https://aistudio.google.com)\n"
        "- **Claude Sonnet**: billed at [console.anthropic.com](https://console.anthropic.com)"
    )
    st.stop()

prompt = build_prompt(
    squad, meta, projections, target_gws, first_upcoming,
    benchmarks=benchmarks,
    opponents=opponents,
    actual_points=actual_pts if gw_finished else None,
    squad_gw=squad_gw,
)

with st.expander("View raw prompt sent to AI", expanded=False):
    st.code(prompt, language="markdown")

if st.button("🔍 Generate Analysis", type="primary"):
    st.session_state.pop("ai_response", None)
    try:
        gen = call_ai(SYSTEM_PROMPT, prompt, provider, api_key)
        response_text = st.write_stream(gen)
        st.session_state["ai_response"] = response_text
    except Exception as e:
        err = str(e)
        if "API_KEY" in err.upper() or "401" in err or "403" in err:
            st.error("Invalid API key — check it in the sidebar.")
        elif "quota" in err.lower() or "429" in err:
            st.error("Rate limit hit. Wait a moment and try again.")
        else:
            st.error(f"AI call failed: {e}")
elif "ai_response" in st.session_state:
    st.markdown(st.session_state["ai_response"])

st.caption(
    "Mdl = Dixon-Coles model xPts  |  Bnch = avg FPLReview & Solio xPts  |  "
    "AI responses are suggestions only — always apply your own judgement."
)
