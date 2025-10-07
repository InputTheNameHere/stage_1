"""
Minimal REST query engine (Stage 1):
- GET /search?q=...&op=and|or&author=...&lang=...
- Reads the monolith index (JSON).
- If SQLite metadata exists (datamarts/SQLite/metadata.db) -> enrich with title/author/language.

Keep intentionally simple and easy to read.
"""

import json
import sqlite3
from pathlib import Path
from typing import Set
from flask import Flask, request, jsonify

REPO = Path(__file__).resolve().parents[1]
INDEX_FILE = REPO / "datamarts" / "inverted_index_monolith" / "index.json"
SQLITE_DB = REPO / "datamarts" / "SQLite" / "metadata.db"

app = Flask(__name__)


def load_index() -> dict:
    if not INDEX_FILE.exists():
        return {}
    return json.loads(INDEX_FILE.read_text())


def get_meta_for(doc_ids: list[int]) -> dict[int, dict]:
    """Fetch {id: {title, author, lang}} from SQLite if available."""
    if not SQLITE_DB.exists() or not doc_ids:
        return {}
    placeholders = ",".join("?" * len(doc_ids))
    sql = f"SELECT book_id, title, author, language FROM books WHERE book_id IN ({placeholders})"
    con = sqlite3.connect(str(SQLITE_DB))
    cur = con.cursor()
    cur.execute(sql, list(map(int, doc_ids)))
    out = {int(book_id): {"title": title, "author": author, "lang": language}
           for book_id, title, author, language in cur.fetchall()}
    con.close()
    return out


INDEX = load_index()  # load once for Stage 1 demo


@app.get("/")
def home():
    return (
        "<h3>Mini Search API</h3>"
        "<p>Try: <code>/search?q=adventure</code> or "
        "<code>/search?q=sea+ocean&op=or</code></p>"
    )


@app.get("/search")
def search():
    q = (request.args.get("q") or "").strip()
    op = (request.args.get("op") or "and").lower()
    author = request.args.get("author")
    lang = request.args.get("lang")

    if not q:
        return jsonify({"error": "q is required"}), 400

    # Collect postings per term
    terms = [t.lower() for t in q.split() if t.strip()]
    sets: list[Set[int]] = []
    for t in terms:
        postings = INDEX.get(t, {}) or {}
        ids = {int(k) for k in postings.keys()}  # keys may be str or int -> normalize
        sets.append(ids)

    # AND / OR
    if not sets:
        ordered = []
    elif op == "or":
        u = set()
        for s in sets: u.update(s)
        ordered = sorted(u)
    else:
        inter = sets[0].copy()
        for s in sets[1:]: inter &= s
        ordered = sorted(inter)

    # Score = sum of term frequencies
    scores: dict[int, int] = {}
    for doc_id in ordered:
        s = 0
        for t in terms:
            m = INDEX.get(t, {}) or {}
            s += m.get(str(doc_id), 0) or m.get(doc_id, 0) or 0
        scores[doc_id] = s

    result_sorted = sorted(ordered, key=lambda d: scores.get(d, 0), reverse=True)[:50]

    # Enrich with metadata if available
    meta = get_meta_for(result_sorted)
    items = [{"id": d, **meta.get(d, {})} for d in result_sorted]

    return jsonify({
        "query": q, "op": op, "filters": {"author": author, "lang": lang},
        "count": len(items), "docs": items
    })


if __name__ == "__main__":
    app.run(debug=True)
