"""
Upload player projection CSVs (Solio + FPLReview). Admin only.
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
from ingest_player_projections import ensure_table, ingest_file, _detect_source
from utils.auth import require_auth, is_admin, show_logout_button

import pandas as pd

st.set_page_config(page_title="FPL – Player Projections Upload", layout="wide", page_icon="📤")

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

if not is_admin():
    st.error("🔒 This page is restricted to admins.")
    if st.button("← Back to Fixtures"):
        st.switch_page("fpl_app.py")
    st.stop()

st.title("📤 Upload Player Projections")

col_back, col_fixture = st.columns([1, 2])
with col_back:
    if st.button("← Back to Fixtures"):
        st.switch_page("fpl_app.py")
with col_fixture:
    if st.button("📤 Fixture Data Upload", use_container_width=True):
        st.switch_page("pages/upload_solio.py")

st.markdown("---")

st.markdown("""
**Accepted files:**
- **Solio** — `projection_all_metrics*.csv` — columns: `Pos, ID, Name, BV, SV, Team`, then `{GW}_xMins`, `{GW}_Pts`, `{GW}_goals`, `{GW}_assists`, `{GW}_CS`, `{GW}_bonus`, `{GW}_cbit`, `{GW}_eo`
- **FPLReview** — `fplreview_*.csv` — columns: `Pos, ID, Name, BV, SV, Team`, then `{GW}_xMins`, `{GW}_Pts` (interleaved), plus `Elite%`

Source is detected automatically from the column names.
""")

uploaded = st.file_uploader(
    "Choose a player projection CSV",
    type="csv",
    help="Solio projection_all_metrics*.csv  or  FPLReview fplreview_*.csv",
)

if not uploaded:
    st.info("No file selected.")
    st.stop()

raw_bytes = uploaded.getvalue()
st.write(f"**File:** {uploaded.name}  |  **Size:** {len(raw_bytes) / 1024:.1f} KB")

# Peek at columns to show detected source before ingesting
try:
    import io
    peek_df = pd.read_csv(io.BytesIO(raw_bytes), nrows=0, encoding='utf-8-sig')
    detected = _detect_source(peek_df, uploaded.name)
    gw_cols = [c for c in peek_df.columns if c.split('_')[0].isdigit()]
    gws = sorted({int(c.split('_')[0]) for c in gw_cols})
    st.info(
        f"Detected source: **{detected.upper()}**  |  "
        f"GWs in file: **{', '.join(str(g) for g in gws)}**"
    )
except Exception:
    pass

force = st.checkbox(
    "Re-ingest if already loaded (force)",
    value=False,
    help="Deletes existing rows for this filename before re-ingesting",
)

if st.button("Ingest", type="primary", use_container_width=True):
    tmpdir = Path(tempfile.mkdtemp())
    try:
        named_path = tmpdir / uploaded.name
        named_path.write_bytes(raw_bytes)

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

# DB status summary
try:
    with sqlite3.connect(str(DB_PATH), check_same_thread=False) as conn:
        ensure_table(conn)
        cur = conn.execute("""
            SELECT source,
                   COUNT(*) as rows,
                   COUNT(DISTINCT source_file) as files,
                   MIN(gw) as min_gw,
                   MAX(gw) as max_gw,
                   MAX(ingested_at) as latest
            FROM player_projection_snapshots
            GROUP BY source
            ORDER BY source
        """)
        summary = cur.fetchall()

    if summary:
        st.subheader("Database Status")
        for row in summary:
            source, rows, files, min_gw, max_gw, latest = row
            st.caption(
                f"**{source.upper()}** — {rows:,} rows across {files} file(s) "
                f"| GW {min_gw}–{max_gw} "
                f"| latest ingestion: `{latest}`"
            )

        # Recent files
        with sqlite3.connect(str(DB_PATH), check_same_thread=False) as conn:
            cur = conn.execute("""
                SELECT source, source_file, COUNT(*) as rows,
                       MIN(gw) as min_gw, MAX(gw) as max_gw, ingested_at
                FROM player_projection_snapshots
                GROUP BY source_file
                ORDER BY ingested_at DESC
                LIMIT 20
            """)
            files_df = cur.fetchall()

        import pandas as pd
        df = pd.DataFrame(files_df, columns=['Source', 'File', 'Rows', 'GW From', 'GW To', 'Ingested'])
        st.dataframe(df, hide_index=True, use_container_width=True)
    else:
        st.caption("No player projection data in database yet.")
except Exception:
    pass
