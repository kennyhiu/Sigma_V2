import argparse
import csv
import os
from datetime import datetime
from typing import Any

from _api.config import load_config
from _sigma.api import SigmaAPI

BASE_FIELDS = ["workspace_name", "owner_name", "workbookname", "urlid", "latest_version_number"]
TAIL_FIELDS = ["last_updated"]


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


def _extract_workspace_name(workbook: dict[str, Any]) -> str:
    path_value = workbook.get("path", None)
    if isinstance(path_value, str) and path_value.strip():
        return path_value.strip()

    candidates = [
        workbook.get("workspaceName"),
        workbook.get("workspace"),
        workbook.get("workspace_name"),
        (workbook.get("workspace") or {}).get("name") if isinstance(workbook.get("workspace"), dict) else None,
    ]
    for value in candidates:
        if isinstance(value, str) and value.strip():
            return value.strip()
    return ""


def _member_display_name(member: dict[str, Any]) -> str:
    direct_name = member.get("name")
    if isinstance(direct_name, str) and direct_name.strip():
        return direct_name.strip()
    first_name = str(member.get("firstName") or "").strip()
    last_name = str(member.get("lastName") or "").strip()
    full_name = f"{first_name} {last_name}".strip()
    if full_name:
        return full_name
    return str(member.get("email") or "").strip()


def _build_member_name_by_id(sigma: SigmaAPI) -> dict[str, str]:
    members = sigma.get_all_members() or []
    out: dict[str, str] = {}
    for member in members:
        if not isinstance(member, dict):
            continue
        member_id = member.get("memberId") or member.get("id")
        if not member_id:
            continue
        out[str(member_id)] = _member_display_name(member)
    return out


def _extract_owner_name(workbook: dict[str, Any], member_name_by_id: dict[str, str]) -> str:
    owner_value = workbook.get("ownerId")
    if isinstance(owner_value, dict):
        owner_name = owner_value.get("name")
        if isinstance(owner_name, str) and owner_name.strip():
            return owner_name.strip()
        owner_id = owner_value.get("memberId") or owner_value.get("id")
        return member_name_by_id.get(str(owner_id), "") if owner_id is not None else ""
    return member_name_by_id.get(str(owner_value), "") if owner_value is not None else ""


def _build_row(
    workbook: dict[str, Any],
    sigma: SigmaAPI,
    tag_name_by_id: dict[str, str],
    tag_names: list[str],
    member_name_by_id: dict[str, str],
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
        "workspace_name": _extract_workspace_name(workbook),
        "owner_name": _extract_owner_name(workbook, member_name_by_id),
        "workbookname": workbook_name,
        "urlid": workbook_urlid,
        "latest_version_number": latest_version_number,
        "last_updated": now_iso,
    }
    for tag_name in tag_names:
        row[tag_name] = "Y" if tag_name in latest_tag_names else "N"
    return row


def _fieldnames(tag_names: list[str]) -> list[str]:
    return BASE_FIELDS + tag_names + TAIL_FIELDS


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
    all_tags = sigma.get_all_version_tags() or []
    tag_name_by_id = {
        str(tag.get("id")): str(tag.get("name") or "")
        for tag in all_tags
        if isinstance(tag, dict) and tag.get("id")
    }
    tag_names = sorted({name for name in tag_name_by_id.values() if name})
    member_name_by_id = _build_member_name_by_id(sigma)
    now_iso = datetime.now().isoformat(timespec="seconds")
    rows: list[dict[str, Any]] = []
    for workbook in workbooks:
        row = _build_row(workbook, sigma, tag_name_by_id, tag_names, member_name_by_id, now_iso)
        if row:
            rows.append(row)

    _write_csv(args.output_csv, rows, _fieldnames(tag_names))

    print(f"Wrote {len(rows)} rows to {args.output_csv}")


if __name__ == "__main__":
    main()
