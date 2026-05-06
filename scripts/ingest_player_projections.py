"""
Ingest player projection CSVs (Solio + FPLReview) into SQLite.

Handles:
  solio/projection_all_metrics*.csv  — xMins, Pts, goals, assists, CS, bonus, cbit, eo per GW
  fplreview/fplreview_*.csv          — xMins, Pts per GW  + flat Elite%

Source is auto-detected from column names (Elite% → fplreview, _eo cols → solio).

Usage:
    # Backfill all files in solio/ and fplreview/:
    python scripts/ingest_player_projections.py --backfill

    # Single file:
    python scripts/ingest_player_projections.py solio/projection_all_metrics.csv

    # Force re-ingest:
    python scripts/ingest_player_projections.py fplreview/fplreview_1776437288.csv --force
"""
import sys
import sqlite3
import argparse
import pandas as pd
from pathlib import Path
from datetime import datetime

ROOT    = Path(__file__).resolve().parent.parent
DB_PATH = ROOT / 'outputs' / 'projections_history.db'

_DDL = """
CREATE TABLE IF NOT EXISTS player_projection_snapshots (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    ingested_at TEXT    NOT NULL,
    source_file TEXT    NOT NULL,
    source      TEXT    NOT NULL,
    player_id   INTEGER,
    name        TEXT    NOT NULL,
    team        TEXT    NOT NULL,
    pos         TEXT    NOT NULL,
    bv          REAL,
    sv          REAL,
    gw          INTEGER NOT NULL,
    xmins       REAL,
    pts         REAL,
    goals       REAL,
    assists     REAL,
    cs          REAL,
    bonus       REAL,
    cbit        REAL,
    eo          REAL,
    elite_pct   REAL
)
"""

_IDX = """
CREATE INDEX IF NOT EXISTS idx_pps_gw_name
ON player_projection_snapshots (gw, name)
"""


def ensure_table(conn):
    conn.execute(_DDL)
    conn.execute(_IDX)
    conn.commit()


def _already_ingested(conn, source_file):
    cur = conn.execute(
        "SELECT COUNT(*) FROM player_projection_snapshots WHERE source_file = ?",
        (source_file,),
    )
    return cur.fetchone()[0] > 0


def _detect_source(df: pd.DataFrame, filename: str) -> str:
    cols_lower = {c.lower() for c in df.columns}
    if 'elite%' in cols_lower:
        return 'fplreview'
    if any(c.lower().endswith('_eo') for c in df.columns):
        return 'solio'
    if filename.lower().startswith('fplreview'):
        return 'fplreview'
    if 'projection' in filename.lower():
        return 'solio'
    return 'unknown'


def _safe_float(val):
    v = pd.to_numeric(val, errors='coerce')
    return None if pd.isna(v) else float(v)


def ingest_file(conn, path: Path, ingested_at: str, force: bool = False) -> int:
    source_file = path.name

    if not force and _already_ingested(conn, source_file):
        print(f"  skip (already ingested): {source_file}")
        return 0

    if force and _already_ingested(conn, source_file):
        conn.execute(
            "DELETE FROM player_projection_snapshots WHERE source_file = ?",
            (source_file,),
        )
        conn.commit()
        print(f"  removed old rows for: {source_file}")

    try:
        df = pd.read_csv(path, encoding='utf-8-sig')
    except Exception as e:
        print(f"  error reading {path.name}: {e}")
        return 0

    required = {'Name', 'Team', 'Pos'}
    if not required.issubset(df.columns):
        print(f"  skip — missing required columns ({required - set(df.columns)}): {source_file}")
        return 0

    source = _detect_source(df, source_file)

    # Flat Elite% (FPLReview only) — same value for all GW rows for this player
    has_elite = 'Elite%' in df.columns

    # Discover GW → {metric_lower: col_name} from columns like "33_xMins", "29_goals"
    gw_metrics: dict[int, dict[str, str]] = {}
    for col in df.columns:
        parts = col.split('_', 1)
        if len(parts) == 2 and parts[0].isdigit():
            gw = int(parts[0])
            metric = parts[1].lower()
            gw_metrics.setdefault(gw, {})[metric] = col

    if not gw_metrics:
        print(f"  skip — no GW columns found: {source_file}")
        return 0

    rows = []
    for _, row in df.iterrows():
        name = str(row.get('Name', '')).strip()
        team = str(row.get('Team', '')).strip()
        pos  = str(row.get('Pos',  '')).strip()
        if not name or not team:
            continue

        pid       = int(row['ID']) if 'ID' in df.columns and pd.notna(row.get('ID')) else None
        bv        = _safe_float(row.get('BV'))
        sv        = _safe_float(row.get('SV'))
        elite_pct = _safe_float(row.get('Elite%')) if has_elite else None

        for gw, metrics in gw_metrics.items():
            def safe(m):
                col = metrics.get(m)
                return None if col is None else _safe_float(row.get(col))

            rows.append({
                'ingested_at': ingested_at,
                'source_file': source_file,
                'source':      source,
                'player_id':   pid,
                'name':        name,
                'team':        team,
                'pos':         pos,
                'bv':          bv,
                'sv':          sv,
                'gw':          gw,
                'xmins':       safe('xmins'),
                'pts':         safe('pts'),
                'goals':       safe('goals'),
                'assists':     safe('assists'),
                'cs':          safe('cs'),
                'bonus':       safe('bonus'),
                'cbit':        safe('cbit'),
                'eo':          safe('eo'),
                'elite_pct':   elite_pct,
            })

    if not rows:
        print(f"  no data rows found in {source_file}")
        return 0

    conn.executemany("""
        INSERT INTO player_projection_snapshots
            (ingested_at, source_file, source, player_id, name, team, pos, bv, sv,
             gw, xmins, pts, goals, assists, cs, bonus, cbit, eo, elite_pct)
        VALUES
            (:ingested_at, :source_file, :source, :player_id, :name, :team, :pos, :bv, :sv,
             :gw, :xmins, :pts, :goals, :assists, :cs, :bonus, :cbit, :eo, :elite_pct)
    """, rows)
    conn.commit()
    print(f"  ingested {len(rows)} rows  |  source={source}  |  file={source_file}  |  ts={ingested_at}")
    return len(rows)


def _file_timestamp(path: Path) -> str:
    """Return a sensible creation timestamp for a file.

    FPLReview filenames embed a Unix timestamp (fplreview_1772201674.csv).
    Use that so backfills on a new server aren't all stamped with today.
    Fall back to file mtime for everything else.
    """
    stem = path.stem
    if stem.startswith('fplreview_'):
        ts_part = stem.split('_', 1)[1]
        if ts_part.isdigit():
            try:
                return datetime.fromtimestamp(int(ts_part)).strftime('%Y-%m-%d %H:%M:%S')
            except (OSError, OverflowError):
                pass
    return datetime.fromtimestamp(path.stat().st_mtime).strftime('%Y-%m-%d %H:%M:%S')


def main():
    parser = argparse.ArgumentParser(description="Ingest player projection CSVs into SQLite")
    parser.add_argument('file', nargs='?', help='Path to a CSV file')
    parser.add_argument('--backfill', action='store_true',
                        help='Ingest all projection_all_metrics*.csv (solio/) and fplreview_*.csv (fplreview/)')
    parser.add_argument('--force', action='store_true',
                        help='Re-ingest even if already in DB')
    args = parser.parse_args()

    if not args.file and not args.backfill:
        parser.print_help()
        sys.exit(1)

    with sqlite3.connect(str(DB_PATH)) as conn:
        ensure_table(conn)

        if args.backfill:
            files = sorted((ROOT / 'solio').glob('projection_all_metrics*.csv'))
            files += sorted((ROOT / 'fplreview').glob('fplreview_*.csv'))
            if not files:
                print("No matching files found.")
                sys.exit(0)
            print(f"Backfilling {len(files)} files...")
            total = 0
            for path in files:
                mtime = _file_timestamp(path)
                total += ingest_file(conn, path, mtime, force=args.force)
            print(f"\nDone. {total} total rows ingested.")
        else:
            path = Path(args.file)
            if not path.is_absolute():
                path = ROOT / path
            if not path.exists():
                print(f"File not found: {path}")
                sys.exit(1)
            now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            ingest_file(conn, path, now, force=args.force)


if __name__ == '__main__':
    main()
