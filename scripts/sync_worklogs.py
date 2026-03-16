import csv
import hashlib
import os
from pathlib import Path

import requests
from playwright.sync_api import sync_playwright

NOTION_TOKEN = os.environ["NOTION_TOKEN"]
NOTION_DATA_SOURCE_ID = os.environ["NOTION_DATA_SOURCE_ID"]
WORKLOG_URL = os.environ["WORKLOG_URL"]

HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": "2025-09-03",
    "Content-Type": "application/json",
}

DOWNLOAD_DIR = Path("downloads")
CSV_PATH = DOWNLOAD_DIR / "worklog.csv"


def rich_text(value: str):
    value = (value or "").strip()
    if not value:
        return []
    return [{"text": {"content": value[:2000]}}]


def title_text(value: str):
    value = (value or "").strip() or "Untitled"
    return [{"text": {"content": value[:2000]}}]


def download_csv():
    DOWNLOAD_DIR.mkdir(exist_ok=True)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page(accept_downloads=True)

        page.goto(WORKLOG_URL, wait_until="networkidle")

        # Change this if your button text is different
        with page.expect_download(timeout=30000) as download_info:
            page.get_by_role("button", name="Export CSV").click()

        download = download_info.value
        download.save_as(str(CSV_PATH))
        browser.close()

    if not CSV_PATH.exists():
        raise FileNotFoundError("CSV was not downloaded.")


def notion_query_existing_sync_keys():
    url = f"https://api.notion.com/v1/data_sources/{NOTION_DATA_SOURCE_ID}/query"
    payload = {"page_size": 100}
    keys = set()

    while True:
        r = requests.post(url, headers=HEADERS, json=payload, timeout=60)
        r.raise_for_status()
        data = r.json()

        for row in data.get("results", []):
            props = row.get("properties", {})
            sync_prop = props.get("SyncKey", {})
            if sync_prop.get("type") == "rich_text":
                value = "".join(part.get("plain_text", "") for part in sync_prop.get("rich_text", []))
                value = value.strip()
                if value:
                    keys.add(value)

        if not data.get("has_more"):
            break

        payload["start_cursor"] = data["next_cursor"]

    return keys


def make_sync_key(row: dict) -> str:
    raw = "|".join([
        row.get("Task", "").strip(),
        row.get("Date", "").strip(),
        row.get("Duration", "").strip(),
        row.get("Project", "").strip(),
        row.get("Notes", "").strip(),
    ])
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def create_notion_row(row: dict):
    sync_key = make_sync_key(row)

    payload = {
        "parent": {
            "type": "data_source_id",
            "data_source_id": NOTION_DATA_SOURCE_ID
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


def import_csv_to_notion():
    existing = notion_query_existing_sync_keys()
    created = 0
    skipped = 0

    with open(CSV_PATH, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            key = make_sync_key(row)
            if key in existing:
                skipped += 1
                continue
            create_notion_row(row)
            created += 1

    print(f"Created {created} rows, skipped {skipped} duplicates.")


def main():
    download_csv()
    import_csv_to_notion()


if __name__ == "__main__":
    main()
