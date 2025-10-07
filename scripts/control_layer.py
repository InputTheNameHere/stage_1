from pathlib import Path
import random
CONTROL_PATH = Path("control")
DOWNLOADS = CONTROL_PATH / "downloaded_books.txt"
INDEXINGS = CONTROL_PATH / "indexed_books.txt"
TOTAL_BOOKS = 70000
def control_pipeline_step():
CONTROL_PATH.mkdir(parents=True, exist_ok=True)
downloaded = set(DOWNLOADS.read_text().splitlines()) if DOWNLOADS.exists() else set()
indexed = set(INDEXINGS.read_text().splitlines()) if INDEXINGS.exists() else set()
ready_to_index = downloaded - indexed
if ready_to_index:
book_id = ready_to_index.pop()
print(f"[CONTROL] Scheduling book {book_id} for indexing...")
# Here you would call the indexer
with open(INDEXINGS, "a", encoding="utf-8") as f:
f.write(f"{book_id}\n")
print(f"[CONTROL] Book {book_id} successfully indexed.")
else:
for _ in range(10): # Retry up to 10 times to find a new book
candidate_id = str(random.randint(1, TOTAL_BOOKS))
if candidate_id not in downloaded:
print(f"[CONTROL] Downloading new book with ID {candidate_id}...")
# Here you would call the downloader
with open(DOWNLOADS, "a", encoding="utf-8") as f:
f.write(f"{candidate_id}\n")
print(f"[CONTROL] Book {candidate_id} successfully downloaded.")
break