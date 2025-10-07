"""
metadata_extractor.py
---------------------
Tiny helper used by scripts/metadata_builders/metadata_sqlite.py.

It scans the datalake for *_header.txt files and extracts three fields:
- title
- author
- language

Returns a dict: { book_id: {"title": ..., "author": ..., "lang": ...}, ... }

We keep regexes and parsing intentionally simple for Stage 1.
"""

from __future__ import annotations
from pathlib import Path
import re
from typing import Dict, Tuple, Optional

# Simple regexes for Gutenberg headers (case-insensitive)
TITLE_RE = re.compile(r"^Title:\s*(.*)", re.IGNORECASE)
AUTHOR_RE = re.compile(r"^Author:\s*(.*)", re.IGNORECASE)
LANG_RE = re.compile(r"^Language:\s*(.*)", re.IGNORECASE)


def _parse_header_text(text: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Extract title/author/language from top part of header text.
    If a field is missing, return None for it.
    """
    # We only need the first ~200 lines (headers live at the top)
    lines = text.splitlines()[:200]
    title = author = lang = None

    for line in lines:
        if title is None:
            m = TITLE_RE.match(line)
            if m:
                val = m.group(1).strip()
                title = val or None

        if author is None:
            m = AUTHOR_RE.match(line)
            if m:
                val = m.group(1).strip()
                author = val or None

        if lang is None:
            m = LANG_RE.match(line)
            if m:
                val = m.group(1).strip()
                lang = val or None

        if title and author and lang:
            break

    return title, author, lang


def gather_metadata(datalake_root: str | Path) -> Dict[int, Dict[str, str | None]]:
    """
    Walk through <datalake_root> and read every *_header.txt.
    Build a mapping: {book_id: {"title":..., "author":..., "lang":...}}
    """
    root = Path(datalake_root)
    out: Dict[int, Dict[str, str | None]] = {}

    # Look for files like: 1342_header.txt
    for header in root.rglob("*_header.txt"):
        name = header.stem  # e.g., "1342_header"
        try:
            book_id = int(name.split("_")[0])
        except Exception:
            continue  # skip unexpected names

        try:
            text = header.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            continue  # unreadable file -> skip

        title, author, lang = _parse_header_text(text)
        out[book_id] = {"title": title, "author": author, "lang": lang}

    return out
