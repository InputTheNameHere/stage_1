"""
Public function to store metadata into MongoDB by reusing metadata_extractor.gather_metadata.
"""

from typing import Optional, List, Tuple
from pathlib import Path
import os
from metadata_extractor import gather_metadata

try:
    from pymongo import MongoClient, UpdateOne
    from pymongo.errors import PyMongoError
except Exception:
    MongoClient = None
    UpdateOne = None
    PyMongoError = Exception


def store_metadata_mongo(datalake_root: Optional[str] = None, mongo_uri: Optional[str] = None, db_name: Optional[str] = None, dry_run: bool = False) -> int:
    """
    Gather metadata and store in MongoDB.

    Parameters:
      - datalake_root: optional path to datalake root (passed to gather_metadata)
      - mongo_uri: optional MongoDB URI (e.g. "mongodb://user:pass@localhost:27017")
                   If not set, will use MONGO_URI env var or default "mongodb://localhost:27017"
      - db_name: optional database name (defaults to env MONGO_DB or "search_engine")
      - dry_run: if True, only print sample docs and do not write to MongoDB

    Returns:
      - number of documents upserted (int)
    """
    # Guard: ensure parser available
    if gather_metadata is None:
        raise RuntimeError("metadata_extractor.gather_metadata not found. Add metadata_extractor.py to repo.")

    # Guard: pymongo available
    if MongoClient is None:
        raise RuntimeError("pymongo not installed. Install with: pip install pymongo")

    # Resolve connection params
    uri = mongo_uri or os.environ.get("MONGO_URI") or os.environ.get("MONGODB_URI") or "mongodb://localhost:27017"
    database_name = db_name or os.environ.get("MONGO_DB") or "search_engine"

    print(f"[INFO] Mongo store invoked. URI preview: {uri[:80]}... DB: {database_name} dry_run={dry_run}")

    # Gather metadata
    rows = gather_metadata(datalake_root)
    if not rows:
        print("[INFO] No metadata rows found.")
        return 0

    # Dry run: print sample and exit
    if dry_run:
        print("[DRY RUN] Sample documents (first 10):")
        for r in rows[:10]:
            book_id, title, author, language, body_path, extracted_at = r
            doc = {
                "book_id": book_id,
                "title": title,
                "author": author,
                "language": language,
                "body_path": body_path,
                "extracted_at": extracted_at
            }
            print(doc)
        return len(rows)

    # Build bulk operations (UpdateOne with upsert)
    ops = []
    unique_seen = set()
    for book_id, title, author, language, body_path, extracted_at in rows:
        # if same book_id appears multiple times in 'rows', keep the last occurrence
        if book_id in unique_seen:
            pass
        unique_seen.add(book_id)
        doc = {
            "book_id": book_id,
            "title": title,
            "author": author,
            "language": language,
            "body_path": body_path,
            "extracted_at": extracted_at
        }
        ops.append(UpdateOne({"book_id": book_id}, {"$set": doc}, upsert=True))

    # Connect to MongoDB and execute bulk write
    client = MongoClient(uri)
    db = client[database_name]
    coll = db["books"]

    try:
        # Ensure indexes: unique on book_id, and non-unique indexes for author/title
        coll.create_index("book_id", unique=True)
        coll.create_index("author")
        coll.create_index("title")
    except PyMongoError as e:
        print(f"[WARN] Could not create indexes (non-fatal): {e}")

    try:
        if not ops:
            print("[INFO] No operations prepared for MongoDB.")
            return 0
        result = coll.bulk_write(ops, ordered=False)
        upserted = (result.upserted_count if hasattr(result, "upserted_count") else 0)
        modified = (result.modified_count if hasattr(result, "modified_count") else 0)
        matched = (result.matched_count if hasattr(result, "matched_count") else 0)
        total = len(ops)
        print(f"[OK] MongoDB: bulk_write finished. ops={total} upserted={upserted} modified={modified} matched={matched}")
        return total
    except PyMongoError as e:
        print(f"[ERROR] MongoDB bulk_write failed: {e}")
        raise
    finally:
        client.close()
