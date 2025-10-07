"""
Build SQLite metadata DB by calling a public API (Gutendex) for each book ID
we find in the datalake (from *_body.txt files). This is a fallback when
*_header.txt files are not present.

Usage:
  python3 -m scripts.metadata_builders.metadata_from_api \
    --datalake-root "data storage/datalake" \
    --out "datamarts/SQLite/metadata.db"
"""
from __future__ import annotations

from pathlib import Path
import argparse
import sqlite3
import time
import requests

GUTENDEX_URL = "https://gutendex.com/books/{}"  # e.g. /books/1342


def discover_ids(datalake_root: Path) -> list[int]:
    """Find all *_body.txt files and return their numeric IDs."""
    ids: set[int] = set()
    for p in datalake_root.rglob("*_body.txt"):
        try:
            ids.add(int(p.stem.split("_")[0]))
        except Exception:
            pass
    return sorted(ids)


def fetch_meta(book_id: int) -> dict[str, str | None]:
    """
    Call Gutendex for a single ID.
    Returns dict with title/author/lang (None if missing).
    """
    try:
        r = requests.get(GUTENDEX_URL.format(book_id), timeout=15)
        if r.status_code != 200:
            return {"title": None, "author": None, "lang": None}
        data = r.json()
    except Exception:
        return {"title": None, "author": None, "lang": None}

    title = data.get("title")
    # authors is a list of dicts with "name"
    authors = data.get("authors") or []
    author = authors[0]["name"] if authors else None
    # languages is a list like ["en"]
    langs = data.get("languages") or []
    lang = langs[0] if langs else None
    return {"title": title, "author": author, "lang": lang}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--datalake-root", required=True)
    ap.add_argument("--out", default="datamarts/SQLite/metadata.db")
    ap.add_argument("--sleep", type=float, default=0.2,
                    help="Small pause between requests (sec), to be nice to API.")
    args = ap.parse_args()

    dl = Path(args.datalake_root)
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)

    ids = discover_ids(dl)
    print(f"[INFO] Found {len(ids)} books to fetch metadata for.")

    # Create / clear DB
    con = sqlite3.connect(str(out))
    cur = con.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS books (
            book_id INTEGER PRIMARY KEY,
            title TEXT,
            author TEXT,
            language TEXT
        )""")
    cur.execute("DELETE FROM books")
    con.commit()

    ok = 0
    start = time.time()
    for i, bid in enumerate(ids, 1):
        meta = fetch_meta(bid)
        cur.execute(
            "INSERT OR REPLACE INTO books(book_id,title,author,language) VALUES (?,?,?,?)",
            (bid, meta["title"], meta["author"], meta["lang"])
        )
        if i % 50 == 0:
            con.commit()
            elapsed = time.time() - start
            print(f"[INFO] {i}/{len(ids)} rows written... ({elapsed:.1f}s)")

        if args.sleep:
            time.sleep(args.sleep)

    con.commit()
    con.close()
    print(f"[OK] Wrote {len(ids)} rows to {out}")


if __name__ == "__main__":
    import time
    main()
