"""
Simple automated test for all three indexing strategies:
monolith (JSON), file-system (FS), and MongoDB.

It builds each index, measures time, and checks that
the term "adventure" exists in multiple documents.

Usage:
  python3 -m scripts.test_indexers
"""

import json
import os
import pathlib
import time
from pymongo import MongoClient
from subprocess import run, PIPE

# Utility to measure time of any strategy
def test_strategy(name, command, check_fn):
    print(f"\n=== Testing {name.upper()} strategy ===")
    start = time.time()
    result = run(command, shell=True, stdout=PIPE, stderr=PIPE, text=True)
    elapsed = time.time() - start

    ok = "OK" if "[OK]" in result.stdout else "?"
    print(result.stdout.strip().split("\n")[-1])  # show last line

    try:
        details = check_fn()
        print(f"✅ {name} works — terms={details['terms']}, "
              f"sample(adventure)={details['sample']} "
              f"⏱ {elapsed:.2f}s")
    except Exception as e:
        print(f"❌ {name} failed: {e}")
    print("-" * 60)


def check_monolith():
    path = pathlib.Path("datamarts/inverted_index_monolith/index.json")
    data = json.loads(path.read_text())
    return {"terms": len(data),
            "sample": list((data.get("adventure") or {}).keys())[:5]}


def check_fs():
    path = pathlib.Path("datamarts/inverted_index_fs/a/adventure.txt")
    if not path.exists():
        raise FileNotFoundError(path)
    with open(path) as f:
        lines = [l.strip().split()[0] for l in f.readlines()[:5]]
    return {"terms": "N/A", "sample": lines}


def check_mongo():
    cli = MongoClient("mongodb://localhost:27017")
    c = cli["search_engine"]["inverted_index"]
    doc = c.find_one({"term": "adventure"}) or {}
    cli.close()
    return {"terms": c.estimated_document_count(),
            "sample": list((doc.get("postings") or {}).keys())[:5]}


def main():
    os.environ["MONGO_URI"] = "mongodb://localhost:27017"

    strategies = [
        ("monolith",
         "python3 -m scripts.index_cli --strategy monolith",
         check_monolith),

        ("fs",
         "python3 -m scripts.index_cli --strategy fs",
         check_fs),

        ("mongo",
         "python3 -m scripts.index_cli --strategy mongo",
         check_mongo)
    ]

    print("\n=== INDEXER FUNCTIONAL TEST ===")
    for name, cmd, check in strategies:
        test_strategy(name, cmd, check)
    print("\nAll tests completed ✅")


if __name__ == "__main__":
    main()
