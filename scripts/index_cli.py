#!/usr/bin/env python3
from __future__ import annotations
import argparse
from pathlib import Path

# Import the three index builders
from scripts.indexers.monolith_json import build_index_monolith
from scripts.indexers.files_hierarchy import build_index_fs
from scripts.indexers.mongo_index import build_index_mongo

def main():
    """
    Command-line entry point to build the inverted index using one of 3 strategies:
      - monolith : single JSON file
      - fs       : one file per term under a folder hierarchy
      - mongo    : documents in MongoDB (requires a running mongod)
    """
    p = argparse.ArgumentParser(description="Build inverted index (Stage 1).")
    p.add_argument(
        "--strategy",
        choices=["monolith", "fs", "mongo"],
        required=True,
        help="Which index backend to build."
    )
    p.add_argument("--datalake-root", default=None, help="Optional override of datalake path.")
    p.add_argument("--output-root", default=None, help="Optional override for output path (monolith/fs).")
    p.add_argument("--no-update-control", action="store_true", help="Do not write control/indexed.txt")
    args = p.parse_args()

    update_control = not args.no_update_control

    # Dispatch by strategy
    if args.strategy == "monolith":
        out, n, t = build_index_monolith(args.datalake_root, args.output_root, update_control)
        print(f"[OK] Monolith index written to {out}. processed_docs={n} time={t:.2f}s")
    elif args.strategy == "fs":
        out, n, t = build_index_fs(args.datalake_root, args.output_root, update_control)
        print(f"[OK] File-system index written under {out}. processed_docs={n} time={t:.2f}s")
    else:
        loc, n, t = build_index_mongo(args.datalake_root, None, update_control=update_control)
        print(f"[OK] Mongo index upserted in {loc}. processed_docs={n} time={t:.2f}s")

if __name__ == "__main__":
    main()
