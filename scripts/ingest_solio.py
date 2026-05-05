"""
Ingest Solio fixture_difficulty_all_metrics CSV files into SQLite.

Usage:
    # Backfill all existing files in solio/ (uses each file's mtime as timestamp):
    python scripts/ingest_solio.py --backfill

    # Ingest a single new file (uses current time):
    python scripts/ingest_solio.py solio/fixture_difficulty_all_metrics.csv

    # Re-ingest a file even if it's already been loaded:
    python scripts/ingest_solio.py solio/fixture_difficulty_all_metrics.csv --force
"""
import sys
import sqlite3
import argparse
import pandas as pd
from pathlib import Path
from datetime import datetime

ROOT    = Path(__file__).resolve().parent.parent
DB_PATH = ROOT / 'outputs' / 'projections_history.db'

sys.path.insert(0, str(Path(__file__).resolve().parent))
from utils.team_mappings import to_short, SOLIO_TO_SHORT


def ensure_table(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS solio_fixture_snapshots (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            ingested_at TEXT    NOT NULL,
            source_file TEXT    NOT NULL,
            team        TEXT    NOT NULL,
            abbr        TEXT,
            gw          INTEGER NOT NULL,
            g           REAL,
            gc          REAL,
            cs          REAL,
            gd          REAL,
            opp         TEXT
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_solio_gw_team
        ON solio_fixture_snapshots (gw, team)
    """)
    conn.commit()


def already_ingested(conn, source_file):
    cur = conn.execute(
        "SELECT COUNT(*) FROM solio_fixture_snapshots WHERE source_file = ?",
        (source_file,)
    )
    return cur.fetchone()[0] > 0


def ingest_file(conn, path: Path, ingested_at: str, force: bool = False):
    source_file = path.name

    if not force and already_ingested(conn, source_file):
        print(f"  skip (already ingested): {source_file}")
        return 0

    if force and already_ingested(conn, source_file):
        conn.execute(
            "DELETE FROM solio_fixture_snapshots WHERE source_file = ?", (source_file,)
        )
        conn.commit()
        print(f"  removed old rows for: {source_file}")

    try:
        df = pd.read_csv(path, encoding='utf-8-sig')
    except Exception as e:
        print(f"  error reading {path.name}: {e}")
        return 0

    if 'Team' not in df.columns:
        print(f"  skip (no Team column): {source_file}")
        return 0

    df['team_short'] = df['Team'].apply(lambda x: to_short(str(x), SOLIO_TO_SHORT))

    # Discover which GWs are present from column names like 29_G, 30_CS, etc.
    gw_metrics: dict[int, dict[str, str]] = {}
    for col in df.columns:
        parts = col.split('_', 1)
        if len(parts) == 2 and parts[0].isdigit():
            gw = int(parts[0])
            metric = parts[1]
            gw_metrics.setdefault(gw, {})[metric] = col

    rows = []
    for _, row in df.iterrows():
        team = row['team_short']
        abbr = str(row.get('Abbr', '')).strip() if 'Abbr' in df.columns else ''
        for gw, metrics in gw_metrics.items():
            def safe(m):
                col = metrics.get(m)
                if col is None:
                    return None
                val = pd.to_numeric(row.get(col), errors='coerce')
                return None if pd.isna(val) else float(val)

            rows.append({
                'ingested_at': ingested_at,
                'source_file': source_file,
                'team':        team,
                'abbr':        abbr,
                'gw':          gw,
                'g':           safe('G'),
                'gc':          safe('GC'),
                'cs':          safe('CS'),
                'gd':          safe('GD'),
                'opp':         str(row[metrics['OPP']]) if 'OPP' in metrics else None,
            })

    if not rows:
        print(f"  no data rows found in {source_file}")
        return 0

    conn.executemany("""
        INSERT INTO solio_fixture_snapshots
            (ingested_at, source_file, team, abbr, gw, g, gc, cs, gd, opp)
        VALUES
            (:ingested_at, :source_file, :team, :abbr, :gw, :g, :gc, :cs, :gd, :opp)
    """, rows)
    conn.commit()
    print(f"  ingested {len(rows)} rows  |  file: {source_file}  |  ts: {ingested_at}")
    return len(rows)


def main():
    parser = argparse.ArgumentParser(description="Ingest Solio fixture CSVs into SQLite")
    parser.add_argument('file', nargs='?', help='Path to a Solio CSV file')
    parser.add_argument('--backfill', action='store_true',
                        help='Ingest all fixture_difficulty_all_metrics*.csv files in solio/')
    parser.add_argument('--force', action='store_true',
                        help='Re-ingest even if source_file already exists in DB')
    args = parser.parse_args()

    if not args.file and not args.backfill:
        parser.print_help()
        sys.exit(1)

    with sqlite3.connect(str(DB_PATH)) as conn:
        ensure_table(conn)

        if args.backfill:
            solio_dir = ROOT / 'solio'
            files = sorted(solio_dir.glob('fixture_difficulty_all_metrics*.csv'))
            if not files:
                print("No matching files found in solio/")
                sys.exit(0)
            print(f"Backfilling {len(files)} files...")
            total = 0
            for path in files:
                # Use file mtime so historical timeline is preserved
                mtime = datetime.fromtimestamp(path.stat().st_mtime).strftime('%Y-%m-%d %H:%M:%S')
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
