import argparse
import csv
import os
from datetime import datetime
from typing import Any

from _api.config import load_config
from _sigma.api import SigmaAPI

BASE_FIELDS = ["workspace_name", "owner_name", "workbookname", "urlid", "published", "published_date"]
TAIL_FIELDS = ["last_updated"]


def _build_row(
    workbook: dict[str, Any],
    sigma: SigmaAPI,
) -> dict[str, Any] | None:
    now_iso = datetime.now().isoformat(timespec="seconds")
    name = workbook.get("name")
    latest_version = workbook.get("latestVersion")
    urlid = workbook.get("workbookUrlId")
    tags = workbook.get("tags", [])
    
    row = {
        "workbookname": name,
        "urlid": urlid,
        "published": latest_version,
        "tag_version": [tag.get("sourceWorkbookVersion") for tag in tags],
        "tagged_date": [tag.get("workbookTaggedAt") for tag in tags],
        "last_updated": now_iso,
    }

def _fieldnames(tag_names: list[str]) -> list[str]:
    tag_fields: list[str] = []
    for tag_name in tag_names:
        tag_fields.append(tag_name)
        tag_fields.append(f"{tag_name} tagged_date")
    return BASE_FIELDS + tag_fields + TAIL_FIELDS


def _write_csv(path: str, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description="Sigma workbook latest-version tag report")
    parser.add_argument("--config", help="Path to configuration file")
    parser.add_argument(
        "--output-csv",
        default="results/Sigma_Workbook_Version_Snapshot.csv",
        help="Output CSV path.",
    )
    args = parser.parse_args()

    config = load_config(args.config)
    sigma = SigmaAPI(config["base_url"], config["client_id"], config["client_secret"])

    if not sigma.authenticate():
        print("Authentication failed.")
        return

    workbooks = sigma.get_all_workbooks()
    if not isinstance(workbooks, list):
        print("Failed to retrieve workbooks.")
        return

    print(f"Total workbooks: {len(workbooks)}")
    
    rows: list[dict[str, Any]] = []
    for workbook in workbooks:
        row = _build_row(workbook, sigma)
        if row:
            rows.append(row)

    _write_csv(args.output_csv, rows, _fieldnames(tag_names))
    
    print(f"Wrote {len(rows)} rows to {args.output_csv}")


if __name__ == "__main__":
    main()
