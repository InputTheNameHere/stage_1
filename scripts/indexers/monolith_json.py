"""
Strategy 1: monolithic JSON file.
Output: <repo>/datamarts/inverted_index_monolith/index.json

Format:
{
  "adventure": {"10": 2, "1342": 5, ...},
  "sea": {"1340": 1, ...},
  ...
}
"""
from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Dict, List, Tuple

from .common import get_repo_root, guess_datalake_root, tokenize, iter_bodies, write_indexed_list


def build_index_monolith(datalake_root: str | None = None,
                         output_path: str | None = None,
                         update_control: bool = True) -> tuple[Path, int, float]:
    t0 = time.time()

    repo = get_repo_root()
    dl = Path(datalake_root) if datalake_root else guess_datalake_root(repo)
    out_dir = Path(output_path) if output_path else (repo / "datamarts" / "inverted_index_monolith")
    out_dir.mkdir(parents=True, exist_ok=True)
    index_file = out_dir / "index.json"

    index: Dict[str, Dict[int, int]] = {}
    processed: List[int] = []

    for book_id, _path, text in iter_bodies(dl):
        for term in tokenize(text):
            d = index.setdefault(term, {})
            d[book_id] = d.get(book_id, 0) + 1
        processed.append(book_id)

    # Convert inner keys to plain ints for JSON (we'll read both str/int later)
    serializable = {term: {int(k): int(v) for k, v in postings.items()}
                    for term, postings in index.items()}
    index_file.write_text(json.dumps(serializable))

    if update_control:
        write_indexed_list(processed, repo / "control")

    return index_file, len(processed), time.time() - t0
