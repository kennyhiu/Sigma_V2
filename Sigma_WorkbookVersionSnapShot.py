import argparse
import csv
import os
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from _core.config import load_config
from _sigma.api import SigmaClient

FIELDNAMES = [
    "path",
    "owner_name",
    "workbookname",
    "urlid",
    "tagname",
    "tag_by",
    "version_number",
    "published_date",
    "release",
    "notes",
]


@dataclass
class SnapshotContext:
    sigma: SigmaClient
    member_name_by_id: dict[str, str]
    now_iso: str


def _build_fieldnames() -> list[str]:
    return FIELDNAMES


def _member_display_name(member: dict[str, Any]) -> str:
    first_name = str(member.get("firstName") or "").strip()
    last_name = str(member.get("lastName") or "").strip()
    full_name = f"{first_name} {last_name}".strip()
    if full_name:
        return full_name
    return str(member.get("email") or "").strip()


def _build_member_name_by_id(sigma: SigmaClient) -> dict[str, str]:
    out: dict[str, str] = {}
    members = sigma.get_all_members() or []
    for member in members:
        if not isinstance(member, dict):
            continue
        member_id = member.get("memberId")
        if not member_id:
            continue
        out[str(member_id)] = _member_display_name(member)
    return out


def _resolve_owner_name(workbook: dict[str, Any], ctx: SnapshotContext) -> str:
    owner_id = workbook.get("ownerId")
    if owner_id:
        return ctx.member_name_by_id.get(str(owner_id), "")
    return ""


def _build_rows(workbook: dict[str, Any], ctx: SnapshotContext) -> list[dict[str, Any]]:
    urlid = workbook.get("workbookUrlId")
    if not urlid:
        return []

    path = workbook.get("path", "")
    owner_name = _resolve_owner_name(workbook, ctx)
    workbookname = workbook.get("name", "")
    published_version = workbook.get("latestVersion", 0) or 0
    published_date = workbook.get("updatedAt", "") or ""

    rows: list[dict[str, Any]] = []

    # Skip the published version row - only include tagged versions
    # Add row for published version
    # rows.append({
    #     "path": path,
    #     "owner_name": owner_name,
    #     "workbookname": workbookname,
    #     "urlid": urlid,
    #     "tagname": "",
    #     "tag_by": "",
    #     "version_number": published_version,
    #     "published_date": published_date,
    #     "release": "",
    #     "notes": "",
    # })

    # Get version history for tag lookups
    version_history = ctx.sigma.get_workbook_version_history(urlid) or []
    
    # Build a map of taggedAt timestamp -> taggedBy member ID for quick lookup
    # We'll match by finding the closest timestamp
    timestamp_to_tagged_by: list[tuple[str, str]] = []
    for version_entry in version_history:
        if not isinstance(version_entry, dict):
            continue
        tags = version_entry.get("tags", [])
        if not isinstance(tags, list):
            continue
        for tag_entry in tags:
            if not isinstance(tag_entry, dict):
                continue
            tagged_at = tag_entry.get("taggedAt")
            tagged_by = tag_entry.get("taggedBy")
            if tagged_at and tagged_by:
                timestamp_to_tagged_by.append((tagged_at, tagged_by))

    # Add rows for each tag
    workbook_tags = ctx.sigma.get_workbook_tags(urlid) or []
    for tag in workbook_tags:
        if not isinstance(tag, dict):
            continue
        tag_name = str(tag.get("name") or "").strip()
        if not tag_name:
            continue
        source_version = tag.get("sourceWorkbookVersion")
        tag_date = tag.get("workbookTaggedAt") or ""
        
        # Find the closest matching taggedBy by timestamp
        tag_by_name = ""
        if tag_date:
            from datetime import datetime
            try:
                tag_datetime = datetime.fromisoformat(tag_date.replace('Z', '+00:00'))
                
                closest_match = None
                min_diff = float('inf')
                
                for tagged_at, tagged_by_id in timestamp_to_tagged_by:
                    try:
                        tagged_datetime = datetime.fromisoformat(tagged_at.replace('Z', '+00:00'))
                        time_diff = abs((tag_datetime - tagged_datetime).total_seconds())
                        if time_diff < min_diff:
                            min_diff = time_diff
                            closest_match = tagged_by_id
                    except (ValueError, TypeError):
                        continue
                
                # Only use the match if it's within a reasonable time window (e.g., 5 seconds)
                if closest_match and min_diff <= 5:
                    tag_by_name = ctx.member_name_by_id.get(str(closest_match), "")
            except (ValueError, TypeError):
                pass

        rows.append({
            "path": path,
            "owner_name": owner_name,
            "workbookname": workbookname,
            "urlid": urlid,
            "tagname": tag_name,
            "tag_by": tag_by_name,
            "version_number": source_version if source_version not in (None, "") else 0,
            "published_date": tag_date,
            "release": "",
            "notes": "",
        })

    return rows


def load_existing_csv(path: str) -> tuple[list[str], set[tuple[str, str, int]]]:
    """Load existing CSV and return fieldnames and set of existing row keys."""
    existing_keys = set()
    fieldnames = FIELDNAMES.copy()
    
    if not os.path.exists(path):
        return fieldnames, existing_keys
    
    with open(path, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames = list(reader.fieldnames or FIELDNAMES)
        for row in reader:
            # Create unique key from urlid, tagname, and version_number
            urlid = row.get("urlid", "")
            tagname = row.get("tagname", "")
            try:
                version_number = int(row.get("version_number", 0))
            except (ValueError, TypeError):
                version_number = 0
            existing_keys.add((urlid, tagname, version_number))
    
    return fieldnames, existing_keys


def export_rows_to_csv(path: str, fieldnames: list[str], rows: list[dict[str, Any]], update_mode: bool = False) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    
    if update_mode and os.path.exists(path):
        # In update mode, append new rows to existing file
        with open(path, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            writer.writerows(rows)
    else:
        # Normal mode or file doesn't exist - write header and all rows
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description="Sigma workbook version snapshot")
    parser.add_argument("--config", help="Path to configuration file")
    parser.add_argument(
        "--output-csv",
        default="results/Sigma_Workbook_Version_Snapshot.csv",
        help="Output CSV path.",
    )
    parser.add_argument(
        "--update",
        action="store_true",
        help="Update mode: only add new rows to existing CSV file instead of overwriting.",
    )
    args = parser.parse_args()

    config = load_config(args.config)
    sigma = SigmaClient(config["base_url"], config["client_id"], config["client_secret"])
    if not sigma.authenticate():
        print("Authentication failed.")
        return

    workbooks = sigma.get_all_workbooks()
    if not isinstance(workbooks, list):
        print("Failed to retrieve workbooks.")
        return

    ctx = SnapshotContext(
        sigma=sigma,
        member_name_by_id=_build_member_name_by_id(sigma),
        now_iso=datetime.now().isoformat(timespec="seconds"),
    )

    rows: list[dict[str, Any]] = []
    for workbook in workbooks:
        if not workbook.get("isArchived"):    
            workbook_rows = _build_rows(workbook, ctx)
            rows.extend(workbook_rows)

    # In update mode, filter out existing rows
    if args.update:
        fieldnames, existing_keys = load_existing_csv(args.output_csv)
        filtered_rows = []
        for row in rows:
            urlid = row.get("urlid", "")
            tagname = row.get("tagname", "")
            try:
                version_number = int(row.get("version_number", 0))
            except (ValueError, TypeError):
                version_number = 0
            
            row_key = (urlid, tagname, version_number)
            if row_key not in existing_keys:
                filtered_rows.append(row)
        
        rows = filtered_rows
        print(f"Found {len(rows)} new rows to add to existing CSV")
    else:
        fieldnames = _build_fieldnames()

    if rows:  # Only write if there are rows to write
        export_rows_to_csv(args.output_csv, fieldnames, rows, args.update)
        print(f"Wrote {len(rows)} rows to {args.output_csv}")
    else:
        print("No new rows to write")


if __name__ == "__main__":
    main()
