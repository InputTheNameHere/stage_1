"""
Benchmarking script for metadata storage performance.
Measures insertion speed, query response times, and scalability.
"""

import time
import sqlite3
import psycopg2
from pymongo import MongoClient

from metadata_sqlite import store_metadata_sqlite
from metadata_postgres import store_metadata_postgres
from metadata_mongo import store_metadata_mongo


# ------------------------------------------
# 1. INSERTION SPEED
# ------------------------------------------
def benchmark_insertion():
    print("\n=== BENCHMARK: INSERTION SPEED ===")
    for name, func in [
        ("SQLite", store_metadata_sqlite),
        ("PostgreSQL", store_metadata_postgres),
        ("MongoDB", store_metadata_mongo),
    ]:
        print(f"\nTesting {name}...")
        start = time.perf_counter()
        rows = func(dry_run=False)  # Actually insert metadata
        end = time.perf_counter()
        print(f"{name}: Inserted {rows} rows in {end - start:.2f} seconds "
              f"→ {rows / (end - start):.2f} rows/sec")


# ------------------------------------------
# 2. QUERY PERFORMANCE
# ------------------------------------------
def timeit_query(func, n=10):
    times = []
    for _ in range(n):
        start = time.perf_counter()
        func()
        times.append(time.perf_counter() - start)
    return sum(times) / len(times)


def benchmark_queries():
    print("\n=== BENCHMARK: QUERY PERFORMANCE ===")

    # SQLite
    with sqlite3.connect("datamarts/SQLite/metadata.db") as conn:
        def q1():
            conn.execute("SELECT * FROM books WHERE author LIKE '%Shakespeare%'").fetchall()
        def q2():
            conn.execute("SELECT body_path FROM books WHERE title LIKE '%Hamlet%'").fetchall()
        print(f"SQLite author query avg: {timeit_query(q1):.5f}s")
        print(f"SQLite title query avg: {timeit_query(q2):.5f}s")

    # PostgreSQL
    with psycopg2.connect("postgresql://user:pass@localhost:5432/yourdb") as conn:
        with conn.cursor() as cur:
            def q1():
                cur.execute("SELECT * FROM books WHERE author ILIKE '%Shakespeare%'")
                cur.fetchall()
            def q2():
                cur.execute("SELECT body_path FROM books WHERE title ILIKE '%Hamlet%'")
                cur.fetchall()
            print(f"Postgres author query avg: {timeit_query(q1):.5f}s")
            print(f"Postgres title query avg: {timeit_query(q2):.5f}s")

    # MongoDB
    client = MongoClient("mongodb://localhost:27017")
    db = client["search_engine"]
    coll = db["books"]
    def q1():
        list(coll.find({"author": {"$regex": "Shakespeare", "$options": "i"}}))
    def q2():
        list(coll.find({"title": {"$regex": "Hamlet", "$options": "i"}}, {"body_path": 1}))
    print(f"Mongo author query avg: {timeit_query(q1):.5f}s")
    print(f"Mongo title query avg: {timeit_query(q2):.5f}s")


# ------------------------------------------
# 3. MAIN
# ------------------------------------------
if __name__ == "__main__":
    benchmark_insertion()
    benchmark_queries()
