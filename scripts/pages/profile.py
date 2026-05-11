"""
User profile page — available to all authenticated users.
"""
import sys
import requests
import streamlit as st
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from utils.auth import require_auth, show_logout_button
from utils import user_db

st.set_page_config(page_title="FPL – Profile", layout="wide", page_icon="👤")

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Barlow+Condensed:wght@400;600;700;800&family=Barlow:wght@400;500;600&display=swap');
html, body, [class*="css"], .stApp, .stMarkdown, .stButton > button {
    font-family: 'Barlow', sans-serif !important;
}
h1, h2, h3 { font-family: 'Barlow Condensed', sans-serif !important; font-weight: 800 !important; }
.block-container { padding-left: 1rem !important; padding-right: 1rem !important; }
</style>
""", unsafe_allow_html=True)

require_auth()
show_logout_button()

email = st.session_state.get('auth_email', '')
role  = st.session_state.get('auth_role', 'member')

st.title("👤 My Profile")

if st.button("← Back to Fixtures"):
    st.switch_page("fpl_app.py")

st.markdown("---")

profile = user_db.get_profile(email)

# ── Personal details ──────────────────────────────────────────────────────────

st.subheader("Personal Details")

with st.form("profile_form"):
    col1, col2 = st.columns(2)
    with col1:
        first_name = st.text_input("First Name", value=profile.get('first_name') or '')
    with col2:
        last_name = st.text_input("Last Name", value=profile.get('last_name') or '')

    st.text_input("Email", value=email, disabled=True)
    st.text_input("Role", value=role.title(), disabled=True)

    st.markdown("---")
    st.subheader("FPL Team")

    current_fpl_id = profile.get('fpl_team_id')
    current_team   = profile.get('fpl_team_name') or ''
    if current_team:
        st.caption(f"Current team: **{current_team}**")

    fpl_id_input = st.text_input(
        "FPL Team ID",
        value=str(current_fpl_id) if current_fpl_id else '',
        help="Your FPL manager ID — visible in the URL when viewing your team on the FPL website "
             "(e.g. fantasy.premierleague.com/entry/**123456**/event/1)",
    )

    submitted = st.form_submit_button("Save Profile", type="primary", use_container_width=True)

if submitted:
    fpl_team_id   = None
    fpl_team_name = current_team

    if fpl_id_input.strip():
        try:
            fpl_team_id = int(fpl_id_input.strip())
        except ValueError:
            st.error("FPL Team ID must be a whole number.")
            st.stop()

        if fpl_team_id != current_fpl_id:
            with st.spinner("Validating FPL Team ID…"):
                try:
                    resp = requests.get(
                        f"https://fantasy.premierleague.com/api/entry/{fpl_team_id}/",
                        timeout=10,
                        headers={"User-Agent": "FPL-Dashboard/1.0"},
                    )
                    if resp.status_code == 200:
                        fpl_team_name = resp.json().get('name', '')
                    elif resp.status_code == 404:
                        st.error(f"FPL Team ID **{fpl_team_id}** not found — check and try again.")
                        st.stop()
                    else:
                        st.error(f"FPL API returned status {resp.status_code} — try again later.")
                        st.stop()
                except requests.exceptions.Timeout:
                    st.error("FPL API timed out — try again in a moment.")
                    st.stop()
                except Exception:
                    st.error("Could not reach the FPL API — try again later.")
                    st.stop()

    user_db.update_profile(
        email,
        first_name.strip(),
        last_name.strip(),
        fpl_team_id,
        fpl_team_name,
    )

    msg = "Profile saved!"
    if fpl_team_name:
        msg += f" FPL team: **{fpl_team_name}**"
    st.success(msg)
    st.rerun()
