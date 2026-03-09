import argparse
import csv
import os
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from _api.config import load_config
from _sigma.api import SigmaClient

BASE_FIELDS = [
    "path",
    "owner_name",
    "workbookname",
    "urlid",
    "published",
    "published_date",
]
TAIL_FIELDS = ["last_updated"]


@dataclass
class SnapshotContext:
    sigma: SigmaClient
    tag_names: list[str]
    member_name_by_id: dict[str, str]
    now_iso: str


def _build_dynamic_fieldnames(tag_names: list[str]) -> list[str]:
    tag_fields: list[str] = []
    for tag_name in tag_names:
        tag_fields.append(tag_name)
        tag_fields.append(f"{tag_name} tagged_date")
    return BASE_FIELDS + tag_fields + TAIL_FIELDS


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
    if owner_id is None:
        return ""
    return ctx.member_name_by_id.get(str(owner_id), "")


def _build_row(workbook: dict[str, Any], ctx: SnapshotContext) -> dict[str, Any] | None:
    urlid = workbook.get("workbookUrlId")
    if not urlid:
        return None

    row: dict[str, Any] = {
        "path": workbook.get("path", ""),
        "owner_name": _resolve_owner_name(workbook, ctx),
        "workbookname": workbook.get("name", ""),
        "urlid": urlid,
        "published": workbook.get("latestVersion", 0) or 0,
        "published_date": workbook.get("updatedAt", "") or "",
        "last_updated": ctx.now_iso,
    }

    for tag_name in ctx.tag_names:
        row[tag_name] = 0
        row[f"{tag_name} tagged_date"] = ""

    workbook_tags = ctx.sigma.get_workbook_tags(urlid) or []
    for tag in workbook_tags:
        if not isinstance(tag, dict):
            continue
        tag_name = str(tag.get("name") or "").strip()
        if tag_name not in ctx.tag_names:
            continue
        source_version = tag.get("sourceWorkbookVersion")
        row[tag_name] = source_version if source_version not in (None, "") else 0
        row[f"{tag_name} tagged_date"] = tag.get("workbookTaggedAt") or ""

    return row


def export_rows_to_csv(path: str, fieldnames: list[str], rows: list[dict[str, Any]]) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
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

    version_tags = sigma.get_all_tags() or []
    tag_names = sorted(
        {
            str(tag.get("name")).strip()
            for tag in version_tags
            if isinstance(tag, dict) and str(tag.get("name") or "").strip()
        }
    )
    ctx = SnapshotContext(
        sigma=sigma,
        tag_names=tag_names,
        member_name_by_id=_build_member_name_by_id(sigma),
        now_iso=datetime.now().isoformat(timespec="seconds"),
    )

    rows: list[dict[str, Any]] = []
    for workbook in workbooks:
        if not workbook.get("isArchived"):    
            row = _build_row(workbook, ctx)
            if row:
                rows.append(row)

    export_rows_to_csv(args.output_csv, _build_dynamic_fieldnames(tag_names), rows)
    print(f"Wrote {len(rows)} rows to {args.output_csv}")


if __name__ == "__main__":
    main()
