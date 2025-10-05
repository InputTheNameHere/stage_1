"""
Strategy 2: file-system hierarchy.
Output tree: <repo>/datamarts/inverted_index_fs/<first-letter>/<term>.txt

Each file contains lines "doc_id tf", sorted by doc_id:
  10 2
  1342 5
"""
from __future__ import annotations

import time
from pathlib import Path
from typing import Dict, List

from .common import get_repo_root, guess_datalake_root, tokenize, iter_bodies, write_indexed_list


def build_index_fs(datalake_root: str | None = None,
                   output_root: str | None = None,
                   update_control: bool = True) -> tuple[Path, int, float]:
    t0 = time.time()

    repo = get_repo_root()
    dl = Path(datalake_root) if datalake_root else guess_datalake_root(repo)
    out_dir = Path(output_root) if output_root else (repo / "datamarts" / "inverted_index_fs")
    out_dir.mkdir(parents=True, exist_ok=True)

    index: Dict[str, Dict[int, int]] = {}
    processed: List[int] = []

    for book_id, _path, text in iter_bodies(dl):
        for term in tokenize(text):
            d = index.setdefault(term, {})
            d[book_id] = d.get(book_id, 0) + 1
        processed.append(book_id)

    for term, postings in index.items():
        folder = out_dir / term[0]
        folder.mkdir(parents=True, exist_ok=True)
        fp = folder / f"{term}.txt"
        lines = [f"{doc} {tf}" for doc, tf in sorted(postings.items())]
        fp.write_text("\n".join(lines) + ("\n" if lines else ""))

    if update_control:
        write_indexed_list(processed, repo / "control")

    return out_dir, len(processed), time.time() - t0
