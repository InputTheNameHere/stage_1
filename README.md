# Search_Engine_Project

## Project Overview

This project represents Stage 1 of a search engine implementation, focusing on building the **Data Layer**. The main goals include:

- Downloading public domain books from [Project Gutenberg](https://www.gutenberg.org/)
- Structuring unstructured book data in a **datalake**
- Extracting and storing structured metadata in **datamarts**
- Evaluating database backends: SQLite, PostgreSQL, and MongoDB
- Coordinating ingestion with a **control layer**

This foundation supports future indexing and search stages (Stage 2+).

---

## Project Structure

```

project-root/
├── data storage/
│   └── datalake/                    # Raw and cleaned book text (header/body)
│       └── YYYYMMDD/HH/
│           ├── <book_id>_header.txt
│           └── <book_id>_body.txt
├── scripts/
|   └──control/
│           ├── downloaded_books.txt        
│           └── failed_downloads.txt
|           └── ids_to_download.txt          
|    └──metadata_builders/
│             ├──metadata_sqlite.pyc             # Extracts and stores metadata in SQLite
|             └──metadata_postgres.pyc           # Extracts and stores metadata in PostgreSQL
|             └──metadata_mongo.pyc              # Extracts and stores metadata in MongoDB
├── download_book.py                 # Downloads and splits books
└── README.md


```
---

## Features

### Book Downloader (`download_book.py`)

- Downloads books from Project Gutenberg using a book ID (e.g., 1342 = *Pride and Prejudice*)
- Identifies header and body using standard markers:
  - `*** START OF THE PROJECT GUTENBERG EBOOK`
  - `*** END OF THE PROJECT GUTENBERG EBOOK`
- Saves `header.txt` and `body.txt` to a structured folder based on **date/hour**

Example output path:
```

data storage/datalake/20251007/14/
├── 1342_header.txt
└── 1342_body.txt

````
---

### Datalake

The **datalake** is organized by ingestion date and hour:

```
datalake/
└── YYYYMMDD/
    └── HH/
        ├── <book_id>_header.txt
        └── <book_id>_body.txt
```

Benefits:

* Traceability
* Incremental processing
* Scalable structure 

---

### Metadata Extractors (Datamarts)

Metadata is parsed from the `header.txt` and stored in:

* **SQLite** (embedded file-based DB)
* **PostgreSQL** (traditional relational DB)
* **MongoDB** (document-based NoSQL)

Expected metadata fields:

* `book_id`
* `title`
* `author`
* `language`

Expected DB schema:

```sql
-- For SQLite/Postgres
CREATE TABLE books (
    book_id INTEGER PRIMARY KEY,
    title TEXT,
    author TEXT,
    language TEXT
);
```

**Files**:

* `metadata_sqlite.cpython-311.pyc`
* `metadata_postgres.cpython-311.pyc`
* `metadata_mongo.cpython-311.pyc`

---

### Control Layer

Tracks ingestion and indexing status using simple flat files:

```
control/
  ├── downloaded_books.txt        
  └── failed_downloads.txt
  └── ids_to_download.txt   
```

Logic:

* Avoid duplicate downloads
* Index only downloaded files
* Update status after each step


---

## Setup & Usage

### Requirements

* Python 3.11+
* pip
* Databases:

  * SQLite (built-in)
  * PostgreSQL (optional)
  * MongoDB (optional)

### Install dependencies

```bash
pip install requests pymongo psycopg2
```

> You can skip `pymongo` and `psycopg2` if you're only testing SQLite.

---

### Run Downloader

```bash
python download_book.py
```

---

## Benchmarking Plan (Optional/Extra Credit)

We aim to test:

* **Insertion Speed**: For 1000s of book records
* **Query Performance**: E.g., find all books by "Jane Austen"
* **Scalability**: As dataset grows from 100s to 10,000+ books

For each database backend (SQLite, PostgreSQL, MongoDB)

---

## Future Work (Stage 2+)

* Full **inverted index** implementation
* Support for keyword search across books
* Microservices in Java (indexer, query engine)
* Web frontend (search interface)
* Parallel processing (Spark, multiprocessing)

---

## Sample Test

```bash
# Test with book ID 1342 (Pride and Prejudice)
python download_book.py
# Then inspect: data storage/datalake/<date>/<hour>/
```

---

## Authors

* Leonoor Antje Barton
* Adrian Budzich
* Martyna Chmielińska 
* Angela López Dorta
* Pablo Mendoza Rodriguez

```
