"""
Centralize the header parsing logic so that each database backend (SQLite, Postgres, MongoDB)
can reuse the same reliable extraction code.

Return format (list of tuples):
    (book_id:int,
     title: str | None,
     author: str | None,
     language: str | None,
     body_path: str,
     extracted_at: str)
"""

import re
from pathlib import Path
from datetime import datetime
from typing import List, Tuple, Optional

def gather_metadata(datalake_root: Optional[str] = None) -> List[Tuple[int, Optional[str], Optional[str], Optional[str], str, str]]:
    """
    Scan datalake for '*_header.txt' files and extract metadata fields.

    Parameters:
    - datalake_root: Optional[str] - path to datalake root. If None, defaults to
      "<repo_root>/data storage/datalake".

    Returns:
    - list of tuples: (book_id, title, author, language, body_path, extracted_at)
    """

    # Determine default base path (repo root). Use Path(__file__).parents[2] if available,
    # otherwise fall back to current working directory.
    try:
        base = Path(__file__).resolve().parents[2]
    except NameError:
        base = Path.cwd()

    if datalake_root:
        dl_root = Path(datalake_root)
    else:
        dl_root = base / "data storage" / "datalake"

    # Simple, line-based regex heuristics for Title / Author / Language.
    RE_TITLE = re.compile(r"^\s*Title:\s*(.+)$", re.IGNORECASE)
    RE_AUTHOR = re.compile(r"^\s*Author:\s*(.+)$", re.IGNORECASE)
    RE_LANGUAGE = re.compile(r"^\s*Language:\s*(.+)$", re.IGNORECASE)

    def extract_meta_from_text(text: str):
        """
        Scan header text line-by-line and try to find title/author/language.
        Stop early if all three are found.
        Returns a tuple (title, author, language) where missing values are None.
        """
        title = author = language = None
        for line in text.splitlines():
            if title is None:
                m = RE_TITLE.match(line)
                if m:
                    title = m.group(1).strip()
            if author is None:
                m = RE_AUTHOR.match(line)
                if m:
                    author = m.group(1).strip()
            if language is None:
                m = RE_LANGUAGE.match(line)
                if m:
                    language = m.group(1).strip()
            if title and author and language:
                break
        return title, author, language

    def parse_book_id_from_path(p: Path) -> Optional[int]:
        """
        Expect filename like '<book_id>_header.txt' and return integer book_id.
        Return None when filename doesn't match the expected pattern.
        """
        stem = p.stem
        parts = stem.split("_")
        try:
            return int(parts[0])
        except Exception:
            return None

    results: List[Tuple[int, Optional[str], Optional[str], Optional[str], str, str]] = []

    if not dl_root.exists():
        # Nothing to do - return empty list
        return results

    header_files = sorted(dl_root.rglob("*_header.txt"))

    for hf in header_files:
        bid = parse_book_id_from_path(hf)
        if bid is None:
            # skip unexpected filenames
            continue
        try:
            txt = hf.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            # skip unreadable files
            continue
        title, author, language = extract_meta_from_text(txt)
        body_path = str(hf.with_name(f"{bid}_body.txt"))
        extracted_at = datetime.utcnow().isoformat()
        results.append((bid, title, author, language, body_path, extracted_at))

    return results
