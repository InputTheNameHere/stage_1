"""
It scans the datalake for files matching "*_header.txt",
extracts metadata (Title, Author, Language) using line-based regex heuristics
and writes the results to a SQLite database (datamarts/metadata.db by default).


    from metadata_sqlite import build_metadata_sqlite
    build_metadata_sqlite(datalake_root="path/to/datalake", db_path="path/to/metadata.db", dry_run=False)
"""

import re
import sqlite3
from pathlib import Path
from datetime import datetime
import sys

def build_metadata_sqlite(datalake_root: str = None, db_path: str = None, dry_run: bool = False):
    """
    Scan datalake for *_header.txt files, extract metadata and store into SQLite.

    Parameters:
    - datalake_root: (str) optional path to datalake root. If None, defaults to "<repo_root>/data storage/datalake".
    - db_path: (str) optional path to sqlite file. If None, defaults to "<repo_root>/datamarts/metadata.db".
    - dry_run: (bool) if True, do not write to DB; only print what would be written.

    Returns:
    - int: number of processed header files (inserted or updated)
    """

    # ---------- Setup default paths:

    # Determine repository base path.
    # Using Path(__file__).resolve().parents[2] assumes this file sits in
    # a subfolders of the subfolders of the repo. If __file__ is missing
    # fall back to current working directory.
    try:
        base = Path(__file__).resolve().parents[2]
    except NameError:
        # __file__ is not defined in some interactive environments
        base = Path.cwd()

    # Default datalake location
    if datalake_root:
        dl_root = Path(datalake_root)
    else:
        dl_root = base / "data storage" / "datalake"

    # Default SQLite path
    if db_path:
        db_file = Path(db_path)
    else:
        db_file = base / "datamarts" / "metadata.db"

    # Print starting info for clarity
    print(f"[INFO] datalake_root = {dl_root}")
    print(f"[INFO] db_file = {db_file}")
    if dry_run:
        print("[INFO] Running in dry_run mode: no DB writes will be performed.")

    # Prepare simple regexes for header parsing
    # We look for lines beginning with Title:, Author:, Language:
    RE_TITLE = re.compile(r"^\s*Title:\s*(.+)$", re.IGNORECASE)
    RE_AUTHOR = re.compile(r"^\s*Author:\s*(.+)$", re.IGNORECASE)
    RE_LANGUAGE = re.compile(r"^\s*Language:\s*(.+)$", re.IGNORECASE)

    def extract_meta_from_text(text: str):
        """
        Scan header text line-by-line and try to find title/author/language.
        Stop early if all three are found.
        Returns a tuple (title, author, language) where missing values are None.
        """
        title = author = language = None
        for line in text.splitlines():
            if title is None:
                m = RE_TITLE.match(line)
                if m:
                    title = m.group(1).strip()
            if author is None:
                m = RE_AUTHOR.match(line)
                if m:
                    author = m.group(1).strip()
            if language is None:
                m = RE_LANGUAGE.match(line)
                if m:
                    language = m.group(1).strip()
            if title and author and language:
                break
        return title, author, language

    def parse_book_id_from_path(p: Path):
        """
        Expect header filenames like '12345_header.txt' or '12345_header'.
        Extract leading integer as book_id, return None if format unknown.
        """
        stem = p.stem
        parts = stem.split("_")
        try:
            return int(parts[0])
        except Exception:
            return None

    # Find header files
    if not dl_root.exists():
        print(f"[ERROR] datalake root does not exist: {dl_root}", file=sys.stderr)
        return 0

    header_files = sorted(dl_root.rglob("*_header.txt"))
    print(f"[INFO] Found {len(header_files)} header files under datalake root.")

    # Prepare SQLite DB (unless dry_run)
    conn = None
    cur = None
    if not dry_run:
        # Ensure parent folder exists
        db_file.parent.mkdir(parents=True, exist_ok=True)
        # Connect to sqlite (file will be created if missing)
        conn = sqlite3.connect(str(db_file))
        cur = conn.cursor()

        # Create table if not exists.
        # We include body_path so we can quickly find the corresponding body file.
        # extracted_at stores the UTC timestamp when the metadata was recorded.
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
        # Helpful indexes for common queries (filter by author/title)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_books_author ON books(author)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_books_title ON books(title)")
        conn.commit()
        print(f"[INFO] Initialized SQLite DB at {db_file}")

    # Process each header file
    processed = 0
    for hf in header_files:
        book_id = parse_book_id_from_path(hf)
        if book_id is None:
            # Skip files that don't follow the expected naming convention.
            print(f"[WARN] Skipping header file with unexpected name format: {hf}")
            continue

        # Try reading header content. Use errors='ignore' to be robust against encoding issues.
        try:
            header_text = hf.read_text(encoding="utf-8", errors="ignore")
        except Exception as e:
            print(f"[WARN] Could not read header file {hf}: {e}")
            continue

        # Extract metadata fields using the helper above
        title, author, language = extract_meta_from_text(header_text)

        # Compute the expected path to the corresponding body file.
        # We expect a file named "<book_id>_body.txt" in the same directory as the header.
        body_path = str(hf.with_name(f"{book_id}_body.txt"))

        # Timestamp when we extracted the metadata (UTC ISO format)
        extracted_at = datetime.utcnow().isoformat()

        if dry_run:
            # In dry run mode, just print what we would insert.
            print(f"[DRY RUN] book_id={book_id}, title={title!r}, author={author!r}, language={language!r}, body_path={body_path}")
            processed += 1
            continue

        # Insert or replace to make function idempotent: running it multiple times will update records.
        try:
            cur.execute("""
                INSERT OR REPLACE INTO books(book_id, title, author, language, body_path, extracted_at)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (book_id, title, author, language, body_path, extracted_at))
            processed += 1
        except Exception as e:
            print(f"[ERROR] DB insert failed for book_id={book_id}: {e}", file=sys.stderr)

    # Commit and close DB connection
    if conn:
        conn.commit()
        conn.close()
        print(f"[OK] Wrote/updated {processed} records into SQLite database: {db_file}")
    else:
        print(f"[DRY RUN] Processed {processed} header files (no DB writes).")

    return processed

if __name__ == "__main__":
    build_metadata_sqlite(dry_run=False)
