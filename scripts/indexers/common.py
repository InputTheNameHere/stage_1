"""
Common helpers shared by all index builders (monolith, FS, Mongo).

We keep things simple:
- detect repo root and datalake path (supports "data storage/datalake" and "data_storage/datalake")
- tokenize text (lowercase, remove short tokens and simple stopwords)
- iterate over all *_body.txt files and yield (book_id, path, text)
- write control/indexed.txt with processed book IDs
"""
from __future__ import annotations

import re
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Tuple

# A small stopword list is enough for Stage 1 demos
STOPWORDS = {
    "the","and","to","of","a","in","that","is","it","for","on","as","with","was","at","by","an",
    "be","this","are","from","or","but","not","have","had","has","were","which",
    "i","you","he","she","we","they","his","her","their","its","my","me","our","us"
}

TOKEN_RE = re.compile(r"[A-Za-z0-9']+")


def get_repo_root(start: Path | None = None) -> Path:
    """Go up from this file: scripts/indexers/common.py -> indexers -> scripts -> <repo>"""
    start = start or Path(__file__).resolve()
    return start.parents[2]


def guess_datalake_root(repo_root: Path) -> Path:
    """
    Return first existing path among:
      <repo>/data storage/datalake
      <repo>/data_storage/datalake
    """
    p1 = repo_root / "data storage" / "datalake"
    if p1.exists():
        return p1
    return repo_root / "data_storage" / "datalake"


def tokenize(text: str) -> List[str]:
    """Lowercase, split by regex, remove short tokens and stopwords."""
    raw = TOKEN_RE.findall(text)
    lowered = (t.lower() for t in raw)
    return [t for t in lowered if len(t) > 1 and t not in STOPWORDS]


def iter_bodies(datalake_root: Path) -> Iterator[Tuple[int, Path, str]]:
    """
    Yield all docs as (book_id, path, text) for files like: 1342_body.txt
    We silently skip unreadable or malformed files.
    """
    for body_path in datalake_root.rglob("*_body.txt"):
        name = body_path.stem  # "1342_body"
        try:
            book_id = int(name.split("_")[0])
        except Exception:
            continue
        try:
            text = body_path.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            continue
        yield book_id, body_path, text


def write_indexed_list(processed_ids: Iterable[int], control_dir: Path) -> None:
    """
    Create control/indexed.txt with one doc ID per line.
    This is the "control layer" that tells us which documents are already indexed.
    """
    control_dir.mkdir(parents=True, exist_ok=True)
    out = control_dir / "indexed.txt"
    ids_sorted = sorted(set(int(x) for x in processed_ids))
    out.write_text("\n".join(map(str, ids_sorted)) + ("\n" if ids_sorted else ""))
