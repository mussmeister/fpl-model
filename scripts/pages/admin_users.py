"""
Admin user management page. Admin only.
"""
import sys
import pandas as pd
import streamlit as st
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from utils.auth import require_auth, is_admin, show_logout_button
from utils import user_db

st.set_page_config(page_title="FPL – Users", layout="wide", page_icon="👥")

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Barlow+Condensed:wght@400;600;700;800&family=Barlow:wght@400;500;600&display=swap');
html, body, [class*="css"], .stApp, .stMarkdown, .stButton > button {
    font-family: 'Barlow', sans-serif !important;
}
h1, h2, h3 { font-family: 'Barlow Condensed', sans-serif !important; font-weight: 800 !important; }
.block-container { max-width: 1100px !important; }
</style>
""", unsafe_allow_html=True)

require_auth()
show_logout_button()

if not is_admin():
    st.error("🔒 This page is restricted to admins.")
    if st.button("← Back to Fixtures"):
        st.switch_page("fpl_app.py")
    st.stop()

st.title("👥 User Management")

if st.button("← Back to Fixtures"):
    st.switch_page("fpl_app.py")

st.markdown("---")

users = user_db.get_all_users()

if not users:
    st.info("No users have logged in yet. They will appear here after their first sign-in.")
    st.stop()

current_email = st.session_state.get('auth_email', '')

df = pd.DataFrame(users)
df['disabled'] = df['disabled'].astype(bool)

display_cols = ['email', 'name', 'role', 'disabled', 'joined_at', 'last_login_at', 'fpl_team_name']
df_display = df[display_cols].rename(columns={
    'email':         'Email',
    'name':          'Name',
    'role':          'Role',
    'disabled':      'Disabled',
    'joined_at':     'Joined',
    'last_login_at': 'Last Login',
    'fpl_team_name': 'FPL Team',
})

# Mark own row so UI can indicate it
own_idx = df_display.index[df['email'] == current_email].tolist()

st.markdown(f"**{len(users)} registered user(s)**")

edited = st.data_editor(
    df_display,
    column_config={
        'Email':      st.column_config.TextColumn(disabled=True),
        'Name':       st.column_config.TextColumn(disabled=True),
        'Role':       st.column_config.SelectboxColumn(options=['admin', 'member']),
        'Disabled':   st.column_config.CheckboxColumn(),
        'Joined':     st.column_config.TextColumn(disabled=True),
        'Last Login': st.column_config.TextColumn(disabled=True),
        'FPL Team':   st.column_config.TextColumn(disabled=True),
    },
    hide_index=True,
    use_container_width=True,
)

col_save, col_note = st.columns([1, 3])
with col_save:
    save = st.button("Save Changes", type="primary", use_container_width=True)
with col_note:
    st.caption("Role changes take effect on the user's next login. Disable takes effect immediately.")

if save:
    changes = 0
    skipped = False
    for i in range(len(edited)):
        email_val = df['email'].iloc[i]
        if email_val == current_email:
            skipped = True
            continue
        orig_role     = df_display['Role'].iloc[i]
        orig_disabled = df_display['Disabled'].iloc[i]
        new_role      = edited['Role'].iloc[i]
        new_disabled  = edited['Disabled'].iloc[i]
        if new_role != orig_role:
            user_db.set_role(email_val, new_role)
            changes += 1
        if new_disabled != orig_disabled:
            user_db.set_disabled(email_val, bool(new_disabled))
            changes += 1

    if skipped:
        st.warning("Your own account was not modified (self-protection).")
    if changes:
        st.success(f"Saved {changes} change(s).")
        st.rerun()
    else:
        st.info("No changes detected.")
