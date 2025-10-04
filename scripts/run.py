#!/usr/bin/env python3
"""
Controller script to:
- create control files: control/to_download.txt, control/downloaded.txt, control/failed.txt
- iterate book ids and call your existing download_book(book_id)
- optionally call metadata storage functions after downloads
"""

import sys
import time
import random
import argparse
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent.resolve()
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

MB_DIR = SCRIPT_DIR / "metadata_builders"
if str(MB_DIR) not in sys.path:
    sys.path.insert(0, str(MB_DIR))

try:
    from download_book import download_book
except Exception:
    download_book = None

try:
    from metadata_builders.metadata_sqlite import store_metadata_sqlite
except Exception:
    store_metadata_sqlite = None

try:
    from metadata_builders.metadata_postgres import store_metadata_postgres
except Exception:
    store_metadata_postgres = None

try:
    from metadata_builders.metadata_mongo import store_metadata_mongo
except Exception:
    store_metadata_mongo = None


# Control-file helpers
def ensure_control_files(to_path: Path, downloaded_path: Path, failed_path: Path, start: int = None, end: int = None, overwrite_to_download: bool = True):
    """
    Create/touch control files. If start/end provided, write sequential ids to to_download.
    """
    to_path.parent.mkdir(parents=True, exist_ok=True)

    if start is not None and end is not None:
        if start > end:
            raise ValueError("start must be <= end")
        if overwrite_to_download or not to_path.exists():
            ids = [str(i) for i in range(start, end + 1)]
            to_path.write_text("\n".join(ids), encoding="utf-8")
            print(f"[INFO] Wrote {len(ids)} ids to {to_path}")

    # touch other files
    downloaded_path.parent.mkdir(parents=True, exist_ok=True)
    failed_path.parent.mkdir(parents=True, exist_ok=True)
    downloaded_path.touch(exist_ok=True)
    failed_path.touch(exist_ok=True)
    print(f"[INFO] Ensured control files: {to_path}, {downloaded_path}, {failed_path}")


def read_set(path: Path) -> set:
    """Read non-empty, stripped lines from file into a set."""
    if not path.exists():
        return set()
    return set([ln.strip() for ln in path.read_text(encoding="utf-8").splitlines() if ln.strip()])


def append_line(path: Path, line: str):
    """Append a line safely to a file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(str(line).strip() + "\n")


# Download loop
def process_to_download(to_file: Path, downloaded_file: Path, failed_file: Path, retries: int = 2, sleep_seconds: float = 0.1):
    """
    Iterate over to_download list and call download_book(book_id).
    Update downloaded_file and failed_file accordingly.
    """
    if download_book is None:
        raise RuntimeError("download_book function not found. Ensure scripts/download_book.py exists and defines download_book(book_id).")

    to_list = to_file.read_text(encoding="utf-8").splitlines() if to_file.exists() else []
    downloaded = read_set(downloaded_file)
    failed = read_set(failed_file)

    print(f"[INFO] Starting downloads: total={len(to_list)}, already_downloaded={len(downloaded)}, failed={len(failed)}")

    for sid in to_list:
        sid = sid.strip()
        if not sid:
            continue
        if sid in downloaded:
            continue
        if sid in failed:
            continue

        try:
            book_id = int(sid)
        except ValueError:
            print(f"[WARN] invalid id in to_download: {sid} -> marking failed")
            append_line(failed_file, sid)
            continue

        success = False
        for attempt in range(retries + 1):
            try:
                result = download_book(book_id)
                # Normalize result: accept bool or (bool, body_path, header_path)
                if isinstance(result, tuple):
                    ok = bool(result[0])
                else:
                    ok = bool(result)
                if ok:
                    append_line(downloaded_file, str(book_id))
                    print(f"[OK] Downloaded {book_id}")
                    success = True
                    break
                else:
                    print(f"[WARN] download_book returned False for {book_id} (attempt {attempt + 1})")
            except Exception as e:
                print(f"[WARN] Exception while downloading {book_id} (attempt {attempt + 1}): {e}")

            # backoff with jitter
            wait = sleep_seconds * (1 + attempt) + random.random() * 0.5
            time.sleep(wait)

        if not success:
            append_line(failed_file, str(book_id))
            print(f"[ERROR] All attempts failed for {book_id}. Marked as failed.")

        # polite pause between different book downloads
        time.sleep(sleep_seconds)


def main():
    # Default range of ids to download
    DEFAULT_START_ID = 1
    DEFAULT_END_ID = 10

    parser = argparse.ArgumentParser(description="Create control files and run download loop.")
    parser.add_argument("--start", type=int, default=DEFAULT_START_ID, help="Start book id (inclusive) to generate to_download list")
    parser.add_argument("--end", type=int, default=DEFAULT_END_ID, help="End book id (inclusive)")
    parser.add_argument("--to-download", type=str, default=str(Path("control_files") / "ids_to_download.txt"))
    parser.add_argument("--downloaded", type=str, default=str(Path("control_files") / "downloaded_books.txt"))
    parser.add_argument("--failed", type=str, default=str(Path("control_files") / "failed_downloads.txt"))
    parser.add_argument("--retries", type=int, default=2, help="Number of retries per id")
    parser.add_argument("--sleep", type=float, default=0.1, help="Seconds between downloads (base)")
    parser.add_argument("--generate-only", action="store_true", help="Only generate to_download and exit")
    parser.add_argument("--sqlite", action="store_true", help="After downloads run SQLite store")
    parser.add_argument("--sqlite-dry", action="store_true", help="Run SQLite store in dry mode")
    parser.add_argument("--postgres", action="store_true", help="After downloads run Postgres store")
    parser.add_argument("--postgres-dry", action="store_true", help="Run Postgres store in dry mode")
    parser.add_argument("--mongo", action="store_true", help="After downloads run Mongo store")
    parser.add_argument("--mongo-dry", action="store_true", help="Run Mongo store in dry mode")

    # Here we can set default database for metadata
    """
    if "--sqlite" or "--sqlite-dry" or "--postgres" or "--postgres-dry" or "--mongo" or "--mongo-dry" not in sys.argv:
        sys.argv += ["--sqlite"]
    """

    args = parser.parse_args()

    to_file = Path(args.to_download)
    downloaded_file = Path(args.downloaded)
    failed_file = Path(args.failed)

    # Ensure control files
    ensure_control_files(to_file, downloaded_file, failed_file, start=args.start, end=args.end, overwrite_to_download=True)

    if args.generate_only:
        print("[INFO] Generation of to_download finished (generate-only). Exiting.")
        return

    # Run download loop
    process_to_download(to_file, downloaded_file, failed_file, retries=args.retries, sleep_seconds=args.sleep)

    # Optionally store metadata to DBs
    repo_base = SCRIPT_DIR.parent.resolve()
    datalake_default = repo_base / "data storage" / "datalake"

    if args.sqlite or args.sqlite_dry:
        if store_metadata_sqlite is None:
            print("[WARN] store_metadata_sqlite not available (module import failed). Skipping.")
        else:
            print("[INFO] Running SQLite metadata store...")
            store_metadata_sqlite(datalake_root=str(datalake_default), dry_run=args.sqlite_dry)

    if args.postgres or args.postgres_dry:
        if store_metadata_postgres is None:
            print("[WARN] store_metadata_postgres not available (module import failed). Skipping.")
        else:
            print("[INFO] Running Postgres metadata store...")
            store_metadata_postgres(datalake_root=str(datalake_default), dry_run=args.postgres_dry)

    if args.mongo or args.mongo_dry:
        if store_metadata_mongo is None:
            print("[WARN] store_metadata_mongo not available (module import failed). Skipping.")
        else:
            print("[INFO] Running Mongo metadata store...")
            store_metadata_mongo(datalake_root=str(datalake_default), dry_run=args.mongo_dry)

    print("[INFO] run.py finished.")


if __name__ == "__main__":
    main()
