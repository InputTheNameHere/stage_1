#!/usr/bin/env python3
"""
CLI for building the inverted index with one of 3 strategies.

Examples:
  python3 -m scripts.index_cli --strategy monolith
  python3 -m scripts.index_cli --strategy fs
  MONGO_URI="mongodb://localhost:27017" python3 -m scripts.index_cli --strategy mongo
"""

import argparse
from scripts.indexers.monolith_json import build_index_monolith
from scripts.indexers.files_hierarchy import build_index_fs
from scripts.indexers.mongo_index import build_index_mongo


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--strategy", choices=["monolith", "fs", "mongo"], required=True)
    p.add_argument("--datalake-root", default=None, help="Optional path to datalake.")
    p.add_argument("--output-root", default=None, help="Optional output root (monolith/fs).")
    p.add_argument("--no-update-control", action="store_true", help="Skip control/indexed.txt update.")
    args = p.parse_args()

    update_control = not args.no_update_control

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
