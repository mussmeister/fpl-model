"""
Upload Solio CSV files via the web interface. Admin only.
"""
import sys
import sqlite3
import shutil
import tempfile
from datetime import datetime
from pathlib import Path
import streamlit as st

DB_PATH = Path(__file__).resolve().parents[2] / 'outputs' / 'projections_history.db'
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ingest_solio import ensure_table, ingest_file
from utils.auth import require_auth, is_admin, show_logout_button

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Barlow+Condensed:wght@400;600;700;800&family=Barlow:wght@400;500;600&display=swap');
html, body, [class*="css"], .stApp, .stMarkdown, .stButton > button,
.stSelectbox label, .stSelectbox div[data-baseweb="select"] {
    font-family: 'Barlow', sans-serif !important;
}
h1, h2, h3 { font-family: 'Barlow Condensed', sans-serif !important; font-weight: 800 !important; }
</style>
""", unsafe_allow_html=True)

require_auth()
show_logout_button()

if not is_admin():
    st.error("🔒 This page is restricted to admins.")
    if st.button("← Back to Fixtures"):
        st.switch_page("fpl_app.py")
    st.stop()

st.title("📤 Upload Solio File")

if st.button("← Back to Fixtures"):
    st.switch_page("fpl_app.py")

st.markdown("---")

uploaded = st.file_uploader(
    "Choose a Solio CSV file",
    type="csv",
    help="Expects columns like 29_G, 29_CS, 30_G, 30_CS, etc. with a Team column"
)

if not uploaded:
    st.info("No file selected. Upload a `fixture_difficulty_all_metrics*.csv` file from Solio.")
    st.stop()

st.write(f"**File:** {uploaded.name}  |  **Size:** {len(uploaded.getvalue()) / 1024:.1f} KB")

force = st.checkbox(
    "Re-ingest if already loaded (force)",
    value=False,
    help="Deletes existing rows for this filename before re-ingesting"
)

if st.button("Ingest", type="primary", use_container_width=True):
    tmpdir = Path(tempfile.mkdtemp())
    try:
        named_path = tmpdir / uploaded.name
        named_path.write_bytes(uploaded.getvalue())

        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        with sqlite3.connect(str(DB_PATH)) as conn:
            ensure_table(conn)
            rows = ingest_file(conn, named_path, now, force=force)

        if rows > 0:
            st.success(f"Ingested **{rows} rows** from `{uploaded.name}` at `{now}`")
            st.cache_data.clear()
        else:
            st.warning(
                "No rows ingested — this file may already be in the database. "
                "Tick **Re-ingest** above to force a reload."
            )
    except Exception as e:
        st.error(f"Error: {e}")
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)

st.markdown("---")

# Show current DB status
try:
    with sqlite3.connect(str(DB_PATH), check_same_thread=False) as conn:
        cur = conn.execute(
            "SELECT COUNT(*) as rows, COUNT(DISTINCT source_file) as files, "
            "MAX(ingested_at) as latest FROM solio_fixture_snapshots"
        )
        row = cur.fetchone()
    if row and row[0]:
        st.caption(
            f"Database: **{row[0]:,} rows** across **{row[1]} files** — "
            f"latest ingestion: `{row[2]}`"
        )
except Exception:
    pass
