"""
User management — tracks logins, profiles, and roles in projections_history.db.
"""
import sqlite3
from datetime import datetime
from pathlib import Path

DB_PATH = Path(__file__).resolve().parents[2] / 'outputs' / 'projections_history.db'

_DDL = """
CREATE TABLE IF NOT EXISTS users (
    email          TEXT PRIMARY KEY,
    name           TEXT    DEFAULT '',
    username       TEXT    DEFAULT '',
    role           TEXT    DEFAULT 'member',
    joined_at      TEXT,
    last_login_at  TEXT,
    disabled       INTEGER DEFAULT 0,
    first_name     TEXT    DEFAULT '',
    last_name      TEXT    DEFAULT '',
    fpl_team_id    INTEGER,
    fpl_team_name  TEXT    DEFAULT ''
)"""


def _connect():
    conn = sqlite3.connect(str(DB_PATH), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def _ensure():
    with _connect() as conn:
        conn.execute(_DDL)


def upsert_login(email: str, name: str, username: str, role: str):
    """Create user record on first login; update name + last_login on repeat logins.
    Role is only written on first login — admin changes via set_role() are preserved."""
    _ensure()
    now = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
    with _connect() as conn:
        if conn.execute("SELECT 1 FROM users WHERE email=?", (email,)).fetchone():
            conn.execute(
                "UPDATE users SET name=?, last_login_at=? WHERE email=?",
                (name, now, email),
            )
        else:
            conn.execute(
                "INSERT INTO users "
                "(email, name, username, role, joined_at, last_login_at) "
                "VALUES (?,?,?,?,?,?)",
                (email, name, username, role, now, now),
            )


def get_db_role(email: str):
    """Return DB-stored role for email, or None if user not yet registered."""
    _ensure()
    with _connect() as conn:
        row = conn.execute("SELECT role FROM users WHERE email=?", (email,)).fetchone()
    return row['role'] if row else None


def is_disabled(email: str) -> bool:
    _ensure()
    with _connect() as conn:
        row = conn.execute("SELECT disabled FROM users WHERE email=?", (email,)).fetchone()
    return bool(row and row['disabled'])


def get_all_users() -> list:
    _ensure()
    with _connect() as conn:
        rows = conn.execute(
            "SELECT email, name, role, joined_at, last_login_at, disabled, "
            "first_name, last_name, fpl_team_id, fpl_team_name "
            "FROM users ORDER BY joined_at DESC"
        ).fetchall()
    return [dict(r) for r in rows]


def set_disabled(email: str, disabled: bool):
    _ensure()
    with _connect() as conn:
        conn.execute("UPDATE users SET disabled=? WHERE email=?", (int(disabled), email))


def set_role(email: str, role: str):
    _ensure()
    with _connect() as conn:
        conn.execute("UPDATE users SET role=? WHERE email=?", (role, email))


def get_profile(email: str) -> dict:
    _ensure()
    with _connect() as conn:
        row = conn.execute(
            "SELECT email, name, first_name, last_name, fpl_team_id, fpl_team_name "
            "FROM users WHERE email=?",
            (email,),
        ).fetchone()
    return dict(row) if row else {}


def update_profile(email: str, first_name: str, last_name: str,
                   fpl_team_id, fpl_team_name: str):
    _ensure()
    with _connect() as conn:
        conn.execute(
            "UPDATE users SET first_name=?, last_name=?, "
            "fpl_team_id=?, fpl_team_name=? WHERE email=?",
            (first_name, last_name, fpl_team_id or None, fpl_team_name, email),
        )
