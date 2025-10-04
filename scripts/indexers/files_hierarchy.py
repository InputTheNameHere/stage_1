from __future__ import annotations
import time
from collections import defaultdict
from pathlib import Path
from typing import Dict, Tuple
from .common import get_repo_root, guess_datalake_root, tokenize, iter_bodies, write_indexed_list

def build_index_fs(
    datalake_root: str | None = None,
    output_root: str | None = None,
    update_control: bool = True
) -> Tuple[Path, int, float]:
    """
    Build a filesystem-based inverted index:
      datamarts/inverted_index_fs/<first-letter>/<term>.txt
    Each file contains lines: "doc_id tf"

    Args:
      datalake_root: optional override for datalake path
      output_root: optional override for output root directory
      update_control: if True, writes control/indexed.txt

    Returns:
      (root_output_dir, number_of_processed_docs, elapsed_seconds)
    """
    t0 = time.time()
    repo = get_repo_root()
    dl = Path(datalake_root) if datalake_root else guess_datalake_root(repo)
    out_dir = Path(output_root) if output_root else (repo / "datamarts" / "inverted_index_fs")
    out_dir.mkdir(parents=True, exist_ok=True)

    inv: Dict[str, Dict[int, int]] = defaultdict(lambda: defaultdict(int))
    processed: list[int] = []

    # Build postings in memory
    for bid, body_path, txt in iter_bodies(dl):
        for tok in tokenize(txt):
            inv[tok][bid] += 1
        processed.append(bid)

    # Persist postings to one file per term
    for term, postings in inv.items():
        first = term[0]
        folder = out_dir / first  # optionally normalize to "_" for non-letters
        folder.mkdir(parents=True, exist_ok=True)
        fp = folder / f"{term}.txt"
        with fp.open("w", encoding="utf-8") as f:
            for doc_id, tf in sorted(postings.items()):
                f.write(f"{doc_id} {tf}\n")

    if update_control:
        write_indexed_list(processed, repo / "control")

    elapsed = time.time() - t0
    return out_dir, len(processed), elapsed
