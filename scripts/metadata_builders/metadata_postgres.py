"""
Public function to store metadata into PostgreSQL by reusing metadata_extractor.gather_metadata.
"""

import os
from typing import Optional
from pathlib import Path
from metadata_extractor import gather_metadata
import logging
from psycopg2 import OperationalError

# psycopg2 for Postgres operations
try:
    import psycopg2
    import psycopg2.extras
except Exception:
    psycopg2 = None


def store_metadata_postgres(datalake_root: Optional[str] = None, dsn: Optional[str] = None, dry_run: bool = False) -> int:
    """
    Gather metadata with gather_metadata() and store into PostgreSQL.

    Parameters:
      - datalake_root: path to datalake root (optional)
      - dsn: optional DSN string (if not provided will read DATABASE_URL or PG* env vars)
      - dry_run: if True, only print actions and do not write

    Returns:
      - number of processed records (int)
    """
    if gather_metadata is None:
        raise RuntimeError("gather_metadata not available (metadata_extractor missing)")

    if psycopg2 is None:
        raise RuntimeError("psycopg2 not installed. Install with: pip install psycopg2-binary")

    # Determine connection DSN
    if dsn:
        conn_dsn = dsn
    else:
        conn_dsn = os.environ.get("DATABASE_URL")

    print(f"[INFO] Postgres store invoked. DSN preview: {str(conn_dsn)[:80]}... dry_run={dry_run}")

    # Gather metadata
    rows = gather_metadata(datalake_root)
    if not rows:
        print("[INFO] No metadata rows found.")
        return 0

    if dry_run:
        print("[DRY RUN] Sample rows (first 10):")
        for r in rows[:10]:
            print(r)
        return len(rows)

    # Connect and upsert
    try:
        conn = psycopg2.connect(conn_dsn)
    except OperationalError as e:
        raise RuntimeError(f"Could not connect to Postgres: {e}")

    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("""
                CREATE TABLE IF NOT EXISTS books (
                    book_id INTEGER PRIMARY KEY,
                    title TEXT,
                    author TEXT,
                    language TEXT,
                    body_path TEXT,
                    extracted_at TIMESTAMP WITH TIME ZONE
                );
                """)
                cur.execute("CREATE INDEX IF NOT EXISTS idx_books_author ON books(author);")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_books_title ON books(title);")

                insert_sql = """
                INSERT INTO books (book_id, title, author, language, body_path, extracted_at)
                VALUES %s
                ON CONFLICT (book_id) DO UPDATE
                  SET title = EXCLUDED.title,
                      author = EXCLUDED.author,
                      language = EXCLUDED.language,
                      body_path = EXCLUDED.body_path,
                      extracted_at = EXCLUDED.extracted_at;
                """

                unique = {}
                for r in rows:
                    unique[r[0]] = (r[0], r[1], r[2], r[3], r[4], r[5])
                values = list(unique.values())

                psycopg2.extras.execute_values(cur, insert_sql, values, page_size=1000)

        print(f"[OK] Postgres: upserted {len(values)} records.")
    except Exception as e:
        print(f"[ERROR] Postgres upsert failed: {e}")
        raise
    finally:
        conn.close()

    return len(values)
