from __future__ import annotations
import os, time
from collections import defaultdict
from pathlib import Path
from typing import Dict, Tuple
from .common import get_repo_root, guess_datalake_root, tokenize, iter_bodies, write_indexed_list

# Lazy import: pymongo is optional (only needed for the Mongo strategy)
try:
    from pymongo import MongoClient, UpdateOne
except Exception:
    MongoClient = None
    UpdateOne = None

def build_index_mongo(
    datalake_root: str | None = None,
    mongo_uri: str | None = None,
    db_name: str = "search_engine",
    coll_name: str = "inverted_index",
    update_control: bool = True
) -> Tuple[str, int, float]:
    """
    Build an inverted index directly into MongoDB.

    Document shape in the collection:
      { term: <str>, postings: { "<doc_id>": <tf>, ... } }

    Notes:
      * Mongo requires string keys in embedded documents, so doc IDs
        are casted to strings inside 'postings'.
    """
    if MongoClient is None:
        raise RuntimeError("pymongo is not installed. pip install pymongo")

    t0 = time.time()
    repo = get_repo_root()
    dl = Path(datalake_root) if datalake_root else guess_datalake_root(repo)

    # Connection string: prefer explicit arg, else env var
    uri = mongo_uri or os.getenv("MONGO_URI", "mongodb://localhost:27017")
    client = MongoClient(uri)
    db = client[db_name]
    coll = db[coll_name]
    coll.create_index("term", unique=True)

    inv: Dict[str, Dict[int, int]] = defaultdict(lambda: defaultdict(int))
    processed: list[int] = []

    # Build postings in memory
    for bid, body_path, txt in iter_bodies(dl):
        for tok in tokenize(txt):
            inv[tok][bid] += 1
        processed.append(bid)

    # Prepare bulk upserts:
    #  - postings keys MUST be strings for MongoDB
    ops = []
    for term, postings in inv.items():
        postings_dict = {str(int(k)): int(v) for k, v in postings.items()}
        ops.append(
            UpdateOne(
                {"term": term},
                {"$set": {"term": term, "postings": postings_dict}},
                upsert=True
            )
        )

    if ops:
        coll.bulk_write(ops, ordered=False)

    if update_control:
        write_indexed_list(processed, repo / "control")

    elapsed = time.time() - t0
    client.close()
    return f"{uri}::{db_name}.{coll_name}", len(processed), elapsed
