"""
FPL Dashboard authentication.

Two sign-in methods:
  1. Google OAuth2 (preferred) — requires HTTPS + Google Cloud Console credentials
  2. Username / password form — checked directly against config/auth.yaml via bcrypt

Roles: 'admin' — full access including Solio data and upload page
       'member' — all read pages, no Solio comparison, no upload

Usage in every page (after set_page_config if present):
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).resolve().parents[N]))
    from utils.auth import require_auth, is_admin, show_logout_button

    user = require_auth()   # stops page if not authenticated
    show_logout_button()    # adds user name + logout to sidebar
"""
import urllib.parse
import yaml
import bcrypt
import requests
import streamlit as st
from pathlib import Path
from yaml.loader import SafeLoader

ROOT        = Path(__file__).resolve().parents[2]
CONFIG_PATH = ROOT / 'config' / 'auth.yaml'

_GOOGLE_AUTH  = 'https://accounts.google.com/o/oauth2/v2/auth'
_GOOGLE_TOKEN = 'https://oauth2.googleapis.com/token'
_GOOGLE_INFO  = 'https://www.googleapis.com/oauth2/v2/userinfo'

_OUR_KEYS = ('auth_status', 'auth_name', 'auth_email', 'auth_username', 'auth_role')


# ── Config ────────────────────────────────────────────────────────────────────

def _load_config():
    if not CONFIG_PATH.exists():
        return {}
    with open(CONFIG_PATH) as f:
        return yaml.load(f, SafeLoader) or {}


# ── Session helpers ───────────────────────────────────────────────────────────

def _set_session(name, email, username, role):
    st.session_state.update({
        'auth_status':   True,
        'auth_name':     name,
        'auth_email':    email,
        'auth_username': username,
        'auth_role':     role,
    })
    st.session_state.pop('_auth_logged_out', None)


def _clear_session():
    for k in _OUR_KEYS:
        st.session_state.pop(k, None)
    st.session_state['_auth_logged_out'] = True


def _get_user():
    return {
        'name':     st.session_state.get('auth_name', ''),
        'email':    st.session_state.get('auth_email', ''),
        'role':     st.session_state.get('auth_role', 'member'),
        'username': st.session_state.get('auth_username', ''),
    }


# ── Public helpers ────────────────────────────────────────────────────────────

def is_admin():
    """True if the current authenticated user has the admin role."""
    return st.session_state.get('auth_role') == 'admin'


def show_logout_button():
    """Render user badge + logout button in the sidebar. Call after require_auth()."""
    with st.sidebar:
        name = st.session_state.get('auth_name', '')
        role = st.session_state.get('auth_role', 'member')
        st.caption(f"👤 **{name}** · {role.title()}")
        if st.button("Logout", key="_sidebar_logout"):
            _clear_session()
            st.rerun()
        st.divider()


# ── Form login (bcrypt, no stauth dependency) ─────────────────────────────────

def _check_credentials(config, username, password):
    """Returns (name, role, email) if valid, else None."""
    ud = config.get('credentials', {}).get('usernames', {}).get(username)
    if not ud:
        return None
    stored = ud.get('password', '')
    if not stored:
        return None
    try:
        if bcrypt.checkpw(password.encode(), stored.encode()):
            return ud.get('name', username), ud.get('role', 'member'), ud.get('email', '')
    except Exception:
        pass
    return None


# ── Google OAuth ──────────────────────────────────────────────────────────────

def _google_auth_url(config):
    oauth = config.get('oauth2', {}).get('google', {})
    cid = oauth.get('client_id', '').strip()
    uri = oauth.get('redirect_uri', '').strip()
    if not cid or not uri:
        return None
    params = urllib.parse.urlencode({
        'client_id':     cid,
        'redirect_uri':  uri,
        'response_type': 'code',
        'scope':         'openid email profile',
        'access_type':   'online',
        'prompt':        'select_account',
    })
    return f"{_GOOGLE_AUTH}?{params}"


def _exchange_google_code(config, code):
    """Exchange OAuth code → (email, name). Returns None on failure."""
    oauth = config.get('oauth2', {}).get('google', {})
    try:
        tok = requests.post(_GOOGLE_TOKEN, data={
            'code':          code,
            'client_id':     oauth.get('client_id', ''),
            'client_secret': oauth.get('client_secret', ''),
            'redirect_uri':  oauth.get('redirect_uri', ''),
            'grant_type':    'authorization_code',
        }, timeout=10).json()
        at = tok.get('access_token')
        if not at:
            return None
        info = requests.get(
            _GOOGLE_INFO,
            headers={'Authorization': f'Bearer {at}'},
            timeout=10,
        ).json()
        return info.get('email'), info.get('name', info.get('email', ''))
    except Exception:
        return None


def _role_for_email(config, email):
    """Return 'admin', 'member', or None if not authorised."""
    el = email.lower()
    for ud in config.get('credentials', {}).get('usernames', {}).values():
        if ud.get('email', '').lower() == el:
            return ud.get('role', 'member')
    if el in [e.lower() for e in config.get('admin_emails', [])]:
        return 'admin'
    if el in [e.lower() for e in config.get('member_emails', [])]:
        return 'member'
    return None


# ── Login UI ──────────────────────────────────────────────────────────────────

def _render_login(config):
    """Full login page — Google button + username/password form fallback."""
    st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Barlow+Condensed:wght@800&family=Barlow:wght@400;500&display=swap');
    html, body, [class*="css"] { font-family: 'Barlow', sans-serif !important; }
    .login-title { font-family:'Barlow Condensed',sans-serif; font-size:34px; font-weight:800;
                   text-align:center; margin-bottom:4px; }
    .login-sub   { text-align:center; color:#888; font-size:13px; margin-bottom:24px; }
    </style>
    """, unsafe_allow_html=True)

    _, col, _ = st.columns([1, 2, 1])
    with col:
        st.markdown('<div class="login-title">⚽ FPL Dashboard</div>', unsafe_allow_html=True)
        st.markdown('<div class="login-sub">Sign in to continue</div>', unsafe_allow_html=True)

        google_url = _google_auth_url(config)
        if google_url:
            st.link_button(
                "Sign in with Google",
                google_url,
                use_container_width=True,
                type="primary",
            )
            st.markdown('<p style="text-align:center;color:#bbb;margin:10px 0;">── or ──</p>',
                        unsafe_allow_html=True)

        has_users = bool(config.get('credentials', {}).get('usernames'))
        if has_users:
            with st.form("_login_form"):
                username  = st.text_input("Username")
                password  = st.text_input("Password", type="password")
                submitted = st.form_submit_button("Sign In", use_container_width=True,
                                                  type="primary" if not google_url else "secondary")

            if submitted:
                result = _check_credentials(config, username, password)
                if result:
                    name, role, email = result
                    _set_session(name, email, username, role)
                    st.rerun()
                else:
                    st.error("Incorrect username or password")

        if not google_url and not has_users:
            st.error("No authentication methods configured — check `config/auth.yaml`.")


# ── Public API ────────────────────────────────────────────────────────────────

def require_auth():
    """
    Ensure user is authenticated. Call at the top of every page.
    Returns a dict with name, email, role, username.
    Stops page execution and shows login UI if not authenticated.
    """
    config = _load_config()

    # ── Google OAuth callback ─────────────────────────────────────────────────
    if 'code' in st.query_params:
        code = st.query_params.get('code')
        st.query_params.clear()
        result = _exchange_google_code(config, code)
        if result:
            email, name = result
            role = _role_for_email(config, email)
            if role:
                _set_session(name, email, '', role)
                st.rerun()
            else:
                st.error(
                    f"**Access denied.** {email} is not an authorised user.  \n"
                    "Ask the admin to add your email to `config/auth.yaml`."
                )
                st.stop()
        else:
            st.error("Google sign-in failed — please try again.")
            st.stop()

    # ── Already authenticated this session ────────────────────────────────────
    if st.session_state.get('auth_status'):
        return _get_user()

    # ── Show login UI ─────────────────────────────────────────────────────────
    _render_login(config)
    st.stop()
