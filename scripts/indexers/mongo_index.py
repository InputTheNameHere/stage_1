"""
Strategy 3: MongoDB collection.
Collection schema: { term: "<str>", postings: { "<doc_id>": <tf>, ... } }

Note: Mongo requires dictionary keys to be strings -> we cast doc IDs to str.
"""
from __future__ import annotations

import os
import time
from pathlib import Path
from typing import Dict, List

from .common import get_repo_root, guess_datalake_root, tokenize, iter_bodies, write_indexed_list

try:
    from pymongo import MongoClient, UpdateOne
except Exception:  # pragma: no cover
    MongoClient = None
    UpdateOne = None


def build_index_mongo(datalake_root: str | None = None,
                      mongo_uri: str | None = None,
                      db_name: str = "search_engine",
                      coll_name: str = "inverted_index",
                      update_control: bool = True) -> tuple[str, int, float]:
    if MongoClient is None:
        raise RuntimeError("pymongo is not installed. Run: pip install pymongo")

    t0 = time.time()

    repo = get_repo_root()
    dl = Path(datalake_root) if datalake_root else guess_datalake_root(repo)
    uri = mongo_uri or os.getenv("MONGO_URI", "mongodb://localhost:27017")
    client = MongoClient(uri)

    coll = client[db_name][coll_name]
    coll.create_index("term", unique=True)

    index: Dict[str, Dict[int, int]] = {}
    processed: List[int] = []

    for book_id, _path, text in iter_bodies(dl):
        for term in tokenize(text):
            d = index.setdefault(term, {})
            d[book_id] = d.get(book_id, 0) + 1
        processed.append(book_id)

    ops = []
    for term, postings in index.items():
        postings_str = {str(int(k)): int(v) for k, v in postings.items()}
        ops.append(UpdateOne({"term": term},
                             {"$set": {"term": term, "postings": postings_str}},
                             upsert=True))
    if ops:
        coll.bulk_write(ops, ordered=False)

    if update_control:
        write_indexed_list(processed, repo / "control")

    client.close()
    return f"{uri}::{db_name}.{coll_name}", len(processed), time.time() - t0
