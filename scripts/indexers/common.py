from __future__ import annotations
import os, re, json, time
from collections import defaultdict
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Tuple

# A small stopword set to keep the example simple.
STOPWORDS = {
    "the","and","to","of","a","in","that","is","it","for","on","as","with","was","at","by","an",
    "be","this","are","from","or","but","not","have","had","has","were","which","you","your",
    "i","he","she","they","we","his","her","their","its","my","me","our","us"
}

def get_repo_root(start: Path | None = None) -> Path:
    """
    Resolve the repository root based on this file location.

    File layout:
      <repo_root>/scripts/indexers/common.py
    parents:
      common.py (0) -> indexers (1) -> scripts (2) -> <repo_root> (3)
    We want <repo_root>, hence parents[2].
    """
    start = start or Path(__file__).resolve()
    return start.parents[2]

def guess_datalake_root(repo_root: Path) -> Path:
    """
    Try to guess where the datalake lives.
    The original project used a folder with a space: "data storage/datalake".
    We also support an underscored variant "data_storage/datalake".
    """
    p1 = repo_root / "data storage" / "datalake"
    if p1.exists():
        return p1
    p2 = repo_root / "data_storage" / "datalake"
    return p2

# Simple token regex: letters, digits, and apostrophes (keeps "don't")
_word = re.compile(r"[A-Za-z0-9']+")

def tokenize(text: str) -> List[str]:
    """
    Lowercase tokenization with stopword removal and min length = 2.
    Returns a list of terms to be indexed.
    """
    toks = [t.lower() for t in _word.findall(text)]
    return [t for t in toks if t not in STOPWORDS and len(t) > 1]

def iter_bodies(datalake_root: Path) -> Iterator[Tuple[int, Path, str]]:
    """
    Iterate over all *_body.txt files under datalake and yield:
      (book_id, file_path, text)
    book_id is extracted from the filename prefix, e.g. '1342_body.txt' -> 1342.
    """
    for body in datalake_root.rglob("*_body.txt"):
        try:
            bid = int(body.stem.split("_")[0])
        except Exception:
            # Skip files that do not follow the <id>_body.txt naming convention
            continue
        try:
            txt = body.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            # Skip unreadable files
            continue
        yield bid, body, txt

def write_indexed_list(processed_ids: Iterable[int], control_dir: Path) -> None:
    """
    Write the sorted unique list of processed doc IDs into control/indexed.txt.
    This serves as a simple control/trace of what has been indexed.
    """
    control_dir.mkdir(parents=True, exist_ok=True)
    out = control_dir / "indexed.txt"
    uniq = sorted(set(processed_ids))
    out.write_text("\n".join(map(str, uniq)) + ("\n" if uniq else ""))
