#!/usr/bin/env python3
from __future__ import annotations
import json, os, sqlite3
from pathlib import Path
from typing import Dict, List, Set
from flask import Flask, request, jsonify

# Resolve repo paths relative to this file
REPO = Path(__file__).resolve().parents[1]

# Default to the monolith index for the HTTP API
INDEX_FILE = REPO / "datamarts" / "inverted_index_monolith" / "index.json"

# Optional SQLite metadata database: contains title/author/lang, etc.
SQLITE_DB = REPO / "datamarts" / "SQLite" / "metadata.db"

app = Flask(__name__)

def load_index() -> Dict[str, Dict[int,int]]:
    """
    Load the monolith inverted index from JSON.
    Shape: { term: { doc_id: tf, ... }, ... }
    """
    if not INDEX_FILE.exists():
        return {}
    try:
        return json.loads(INDEX_FILE.read_text())
    except Exception:
        # Return empty index if file is malformed
        return {}

def get_meta(doc_ids: List[int], author: str | None, lang: str | None) -> List[int]:
    """
    Filter a list of document IDs using metadata constraints (author/lang).
    Requires SQLite DB with a table 'books(book_id, author, language, ...)'.
    If DB or filters are not present, returns the input list unchanged.
    """
    if not SQLITE_DB.exists() or (author is None and lang is None) or not doc_ids:
        return doc_ids

    # Build a simple WHERE ... IN (...) filter
    q = "SELECT book_id FROM books WHERE book_id IN ({})".format(",".join("?"*len(doc_ids)))
    params: list = list(map(int, doc_ids))
    if author is not None:
        q += " AND author LIKE ?"
        params.append(f"%{author}%")
    if lang is not None:
        q += " AND language LIKE ?"
        params.append(f"%{lang}%")

    conn = sqlite3.connect(str(SQLITE_DB))
    cur = conn.cursor()
    cur.execute(q, params)
    rows = [r[0] for r in cur.fetchall()]
    conn.close()
    return rows

def get_titles_for(doc_ids: List[int]) -> dict[int, dict]:
    """
    Fetch (title, author, language) for given document IDs from SQLite.
    Returns a dict: { doc_id: {'title': ..., 'author': ..., 'lang': ...}, ... }
    If DB is missing, returns empty mapping.
    """
    if not SQLITE_DB.exists() or not doc_ids:
        return {}
    q = "SELECT book_id, title, author, language FROM books WHERE book_id IN ({})".format(
        ",".join("?" * len(doc_ids))
    )
    conn = sqlite3.connect(str(SQLITE_DB))
    cur = conn.cursor()
    cur.execute(q, list(map(int, doc_ids)))
    out = {
        int(r[0]): {"title": r[1], "author": r[2], "lang": r[3]}
        for r in cur.fetchall()
    }
    conn.close()
    return out

# Load the index file once at startup (simple demo setup)
INDEX = load_index()

@app.get("/")
def home():
    """
    Minimal landing page to document the API usage.
    """
    return (
        "<h1>Mini Search API</h1>"
        "<p>Use <code>/search?q=term1+term2&op=and|or&author=...&lang=...</code></p>"
        "<ul>"
        "<li><a href='/search?q=adventure'>/search?q=adventure</a></li>"
        "<li><a href='/search?q=sherlock+holmes'>/search?q=sherlock+holmes</a></li>"
        "<li><a href='/search?q=sea+ocean&op=or'>/search?q=sea+ocean&op=or</a></li>"
        "</ul>"
    ), 200

@app.get("/search")
def search():
    """
    Query endpoint.
    Query params:
      q  : space-separated terms (required)
      op : "and" (default) or "or"
      author : optional metadata filter (substring match)
      lang   : optional metadata filter (substring match)
    Response:
      {
        query, op, filters, count,
        docs: [ { id, title?, author?, lang? }, ... ]
      }
    """
    q = request.args.get("q", "").strip()
    op = request.args.get("op", "and").lower()
    author = request.args.get("author")
    lang = request.args.get("lang")

    if not q:
        return jsonify({"error": "q is required"}), 400

    # Split into terms and collect postings sets
    terms = [t for t in q.split() if t]
    postings: List[Set[int]] = []
    for t in terms:
        # Values in JSON might be string keys or int keys depending on how index was built.
        # We normalize to int IDs here.
        term_map = INDEX.get(t.lower(), {}) or {}
        ids = set(map(int, term_map.keys()))
        postings.append(ids)

    # Compute result set based on op
    if not postings:
        result: List[int] = []
    else:
        if op == "or":
            s: Set[int] = set().union(*postings)
        else:
            s = set(postings[0])
            for p in postings[1:]:
                s &= p
        result = sorted(s)

    # Optional metadata filtering (author/lang)
    if author or lang:
        result = get_meta(result, author, lang)

    # Naive scoring: sum of term frequencies across query terms
    scores = {
        doc: sum(
            (INDEX.get(t.lower(), {}) or {}).get(str(doc), 0)  # tf under string key
            or (INDEX.get(t.lower(), {}) or {}).get(doc, 0)   # or tf under int key
            for t in terms
        )
        for doc in result
    }
    result_sorted = sorted(result, key=lambda d: scores.get(d, 0), reverse=True)[:50]

    # Enrich with metadata if available
    meta_map = get_titles_for(result_sorted)
    items = [
        {
            "id": doc,
            **(
                {
                    "title": meta_map[doc]["title"],
                    "author": meta_map[doc]["author"],
                    "lang": meta_map[doc]["lang"],
                }
                if doc in meta_map
                else {}
            ),
        }
        for doc in result_sorted
    ]

    return jsonify({
        "query": q,
        "op": op,
        "filters": {"author": author, "lang": lang},
        "count": len(items),
        "docs": items,
    })

if __name__ == "__main__":
    # Debug server for local testing
    app.run(debug=True)
