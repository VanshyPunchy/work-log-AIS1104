import csv
import io
import os
import hashlib
import requests

NOTION_TOKEN = os.environ["NOTION_TOKEN"]
NOTION_DATA_SOURCE_ID = os.environ["NOTION_DATA_SOURCE_ID"]
WORKLOG_CSV_URL = os.environ["WORKLOG_CSV_URL"]

NOTION_VERSION = "2026-03-11"

HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": NOTION_VERSION,
    "Content-Type": "application/json",
}

def rich_text(value: str):
    value = (value or "").strip()
    if not value:
        return []
    return [{"text": {"content": value[:2000]}}]

def title_text(value: str):
    value = (value or "").strip() or "Untitled"
    return [{"text": {"content": value[:2000]}}]

def fetch_csv_text() -> str:
    resp = requests.get(WORKLOG_CSV_URL, timeout=60)
    resp.raise_for_status()
    return resp.text

def query_existing_keys():
    """
    Reads existing rows and returns a set of SyncKey values already in Notion.
    Assumes your database has a text property called SyncKey.
    """
    url = f"https://api.notion.com/v1/data_sources/{NOTION_DATA_SOURCE_ID}/query"
    existing = set()
    payload = {"page_size": 100}

    while True:
        r = requests.post(url, headers=HEADERS, json=payload, timeout=60)
        r.raise_for_status()
        data = r.json()

        for row in data.get("results", []):
            props = row.get("properties", {})
            sync_prop = props.get("SyncKey", {})
            if sync_prop.get("type") == "rich_text":
                parts = sync_prop.get("rich_text", [])
                existing_value = "".join(
                    p.get("plain_text", "") for p in parts
                ).strip()
                if existing_value:
                    existing.add(existing_value)

        if not data.get("has_more"):
            break
        payload["start_cursor"] = data["next_cursor"]

    return existing

def make_sync_key(row: dict) -> str:
    raw = "|".join([
        row.get("Task", "").strip(),
        row.get("Date", "").strip(),
        row.get("Duration", "").strip(),
        row.get("Project", "").strip(),
        row.get("Notes", "").strip(),
    ])
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()

def create_row(row: dict):
    sync_key = make_sync_key(row)

    payload = {
        "parent": {
            "type": "data_source_id",
            "data_source_id": NOTION_DATA_SOURCE_ID,
        },
        "properties": {
            "Task": {
                "title": title_text(row.get("Task", ""))
            },
            "Date": {
                "date": {"start": row.get("Date", "").strip()} if row.get("Date", "").strip() else None
            },
            "Duration": {
                "number": float(row["Duration"]) if row.get("Duration", "").strip() else None
            },
            "Project": {
                "rich_text": rich_text(row.get("Project", ""))
            },
            "Notes": {
                "rich_text": rich_text(row.get("Notes", ""))
            },
            "SyncKey": {
                "rich_text": rich_text(sync_key)
            }
        }
    }

    r = requests.post("https://api.notion.com/v1/pages", headers=HEADERS, json=payload, timeout=60)
    r.raise_for_status()

def main():
    csv_text = fetch_csv_text()
    reader = csv.DictReader(io.StringIO(csv_text))
    existing_keys = query_existing_keys()

    created = 0
    skipped = 0

    for row in reader:
        key = make_sync_key(row)
        if key in existing_keys:
            skipped += 1
            continue
        create_row(row)
        created += 1

    print(f"Done. Created {created} rows, skipped {skipped} duplicates.")

if __name__ == "__main__":
    main()
