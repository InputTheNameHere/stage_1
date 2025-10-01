import requests
from pathlib import Path
from datetime import datetime

START_MARKER="*** START OF THE PROJECT GUTENBERG EBOOK"
END_MARKER="*** END OF THE PROJECT GUTENBERG EBOOK"

def download_book(book_id:int):

    base_path = Path(__file__).resolve().parents[1]
    datalake_path = base_path / "data storage" / "datalake"

    now = datetime.now()
    date_str = now.strftime("%Y%m%d")
    hour_str = now.strftime("%H")
    output_path= datalake_path / date_str / hour_str

    output_path.mkdir(parents=True,exist_ok=True)

    url=f"https://www.gutenberg.org/cache/epub/{book_id}/pg{book_id}.txt"
    response=requests.get(url)
    response.raise_for_status()
    text=response.text

    if START_MARKER not in text or END_MARKER not in text:
        return False

    header,body_and_footer=text.split(START_MARKER,1)
    body,footer=body_and_footer.split(END_MARKER,1)

    body_path=output_path/f"{book_id}_body.txt"
    header_path=output_path/f"{book_id}_header.txt"

    with open(body_path,"w",encoding="utf-8")as f:
        f.write(body.strip())
    with open(header_path,"w",encoding="utf-8")as f:
        f.write(header.strip())
    return True

download_book(1342)