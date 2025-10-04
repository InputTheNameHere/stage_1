"""
Public function to store metadata into SQLite by reusing metadata_extractor.gather_metadata.
"""

import sqlite3
from pathlib import Path
from typing import Optional
from metadata_extractor import gather_metadata


def store_metadata_sqlite(datalake_root: Optional[str] = None, db_path: Optional[str] = None, dry_run: bool = False) -> int:
    """
    Gather metadata with gather_metadata() and store into SQLite.

    Parameters:
      - datalake_root: path to datalake root (optional)
      - db_path: path to sqlite file (optional). Defaults to datamarts/SQLite/metadata.db relative to repo root.
      - dry_run: if True, only print what would be inserted (no DB writes).

    Returns:
      - number of processed records (int)
    """

    # Determine default paths
    try:
        base = Path(__file__).resolve().parents[2]
    except NameError:
        base = Path.cwd()

    dl_root = Path(datalake_root) if datalake_root else base / "data storage" / "datalake"
    db_file = Path(db_path) if db_path else base / "datamarts" / "SQLite" / "metadata.db"

    print(f"[INFO] SQLite store invoked: datalake={dl_root} db={db_file} dry_run={dry_run}")

    # Gather metadata
    rows = gather_metadata(str(dl_root) if dl_root else None)
    if not rows:
        print("[INFO] No metadata rows found.")
        return 0

    # Dry run: print a sample and exit
    if dry_run:
        print("[DRY RUN] Sample of metadata (first 10):")
        for r in rows[:10]:
            print(r)
        return len(rows)

    # Prepare DB and insert
    db_file.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_file))
    cur = conn.cursor()

    # Create table (simple schema) and indexes
    cur.execute("""
        CREATE TABLE IF NOT EXISTS books (
            book_id INTEGER PRIMARY KEY,
            title TEXT,
            author TEXT,
            language TEXT,
            body_path TEXT,
            extracted_at TEXT
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_books_author ON books(author)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_books_title ON books(title)")
    conn.commit()

    processed = 0
    for (book_id, title, author, language, body_path, extracted_at) in rows:
        try:
            cur.execute("""
                INSERT OR REPLACE INTO books(book_id, title, author, language, body_path, extracted_at)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (book_id, title, author, language, body_path, extracted_at))
            processed += 1
        except Exception as e:
            print(f"[WARN] SQLite insert failed for book_id={book_id}: {e}")

    conn.commit()
    conn.close()
    print(f"[OK] SQLite: stored {processed} records into {db_file}")
    return processed
