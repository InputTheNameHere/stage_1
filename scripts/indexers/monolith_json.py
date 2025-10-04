from __future__ import annotations
import json, time
from collections import defaultdict
from pathlib import Path
from typing import Dict, Tuple
from .common import get_repo_root, guess_datalake_root, tokenize, iter_bodies, write_indexed_list

def build_index_monolith(
    datalake_root: str | None = None,
    output_path: str | None = None,
    update_control: bool = True
) -> Tuple[Path, int, float]:
    """
    Build a single-file inverted index:
      index.json : { term: { doc_id: tf, ... }, ... }

    Args:
      datalake_root: optional override for datalake path
      output_path: optional override for output folder (will create index.json inside)
      update_control: if True, writes control/indexed.txt with processed IDs

    Returns:
      (index_file_path, number_of_processed_docs, elapsed_seconds)
    """
    t0 = time.time()
    repo = get_repo_root()
    dl = Path(datalake_root) if datalake_root else guess_datalake_root(repo)
    out_dir = Path(output_path) if output_path else (repo / "datamarts" / "inverted_index_monolith")
    out_dir.mkdir(parents=True, exist_ok=True)
    index_file = out_dir / "index.json"

    # term -> {doc_id: tf}
    inv: Dict[str, Dict[int, int]] = defaultdict(lambda: defaultdict(int))
    processed: list[int] = []

    # Build postings in memory
    for bid, body_path, txt in iter_bodies(dl):
        for tok in tokenize(txt):
            inv[tok][bid] += 1
        processed.append(bid)

    # Convert to plain dicts for JSON serialization
    plain = {term: dict(post) for term, post in inv.items()}
    index_file.write_text(json.dumps(plain))

    if update_control:
        write_indexed_list(processed, repo / "control")

    elapsed = time.time() - t0
    return index_file, len(processed), elapsed
