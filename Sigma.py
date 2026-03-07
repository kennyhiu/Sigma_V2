import argparse
import csv
import os
from datetime import datetime
from typing import Any

from _api.config import load_config
from _sigma.api import SigmaAPI

TAG_COLUMN_MAP = [
    ("INT", "INT Tag (Y/N)"),
    ("Ready for UAT", "Ready for UAT Tag (Y/N)"),
    ("UAT", "UAT Tag (Y/N)"),
    ("VP_PROD_US", "VP_PROD_US Tag (Y/N)"),
    ("VP_PROD_EU", "VP_PROD_EU Tag (Y/N)"),
    ("VP_PROD_AU", "VP_PROD_AU Tag (Y/N)"),
]
BASE_COLUMNS = ["workbookname", "urlid", "latest_version_number"]
TAIL_COLUMNS = ["last_updated"]


def _version_sort_key(value: Any) -> Any:
    if value is None:
        return -1
    if isinstance(value, (int, float)):
        return value
    text = str(value).strip()
    if not text:
        return -1
    try:
        return int(text)
    except ValueError:
        try:
            return float(text)
        except ValueError:
            return text


def _latest_version(versions: list[dict[str, Any]] | None) -> dict[str, Any] | None:
    latest: dict[str, Any] | None = None
    for version in versions or []:
        version_number = version.get("version")
        if latest is None or _version_sort_key(version_number) > _version_sort_key(latest.get("version")):
            latest = version
    return latest


def _extract_tag_names(tag_name_by_id: dict[str, str], latest_version: dict[str, Any] | None) -> set[str]:
    names = set()
    for tag in (latest_version or {}).get("tags", []) or []:
        tag_id = tag.get("versionTagId") or tag.get("tagId") or tag.get("id") or ""
        tag_name = tag_name_by_id.get(str(tag_id))
        if tag_name:
            names.add(str(tag_name).strip())
    return names


def _build_row(
    workbook: dict[str, Any],
    sigma: SigmaAPI,
    tag_name_by_id: dict[str, str],
    now_iso: str,
) -> dict[str, Any] | None:
    workbook_name = workbook.get("name", "(unnamed)")
    workbook_urlid = workbook.get("workbookUrlId") or ""
    if not workbook_urlid:
        return None

    versions = sigma.get_workbook_version_history(workbook_urlid) or []
    latest = _latest_version(versions)
    latest_version_number = latest.get("version") if latest else "N/A"
    latest_tag_names = _extract_tag_names(tag_name_by_id, latest)

    row = {
        "workbookname": workbook_name,
        "urlid": workbook_urlid,
        "latest_version_number": latest_version_number,
        "last_updated": now_iso,
    }
    for tag_name, column_name in TAG_COLUMN_MAP:
        row[column_name] = "Y" if tag_name in latest_tag_names else "N"
    return row


def _fieldnames() -> list[str]:
    return BASE_COLUMNS + [column for _, column in TAG_COLUMN_MAP] + TAIL_COLUMNS


def _write_csv(path: str, rows: list[dict[str, Any]]) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=_fieldnames())
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description="Sigma workbook latest-version tag report")
    parser.add_argument("--config", help="Path to configuration file")
    parser.add_argument(
        "--output-csv",
        default="results/Sigma_Workbook_LatestVersion_Tags.csv",
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
    all_tags = sigma.get_all_version_tags() or []
    tag_name_by_id = {
        str(tag.get("id")): str(tag.get("name") or "")
        for tag in all_tags
        if isinstance(tag, dict) and tag.get("id")
    }
    now_iso = datetime.now().isoformat(timespec="seconds")
    rows: list[dict[str, Any]] = []
    for workbook in workbooks:
        row = _build_row(workbook, sigma, tag_name_by_id, now_iso)
        if row:
            rows.append(row)

    _write_csv(args.output_csv, rows)

    print(f"Wrote {len(rows)} rows to {args.output_csv}")


if __name__ == "__main__":
    main()
