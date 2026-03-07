import argparse
import csv
import os
from datetime import datetime
from _api.config import load_config
from _sigma.api import SigmaAPI


def _version_sort_key(value):
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


def _latest_version(versions):
    latest = None
    for version in versions or []:
        version_number = version.get("version")
        if latest is None or _version_sort_key(version_number) > _version_sort_key(latest.get("version")):
            latest = version
    return latest


def _extract_tag_names(api: SigmaAPI, latest_version: dict) -> set[str]:
    names = set()
    for tag in (latest_version or {}).get("tags", []) or []:
        tag_id = tag.get("versionTagId") or tag.get("tagId") or tag.get("id")
        tag_name = api.get_tag_name(tag_id)
        if tag_name:
            names.add(str(tag_name).strip())
    return names


def main():
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
    rows = []
    for workbook in workbooks:
        workbook_name = workbook.get("name", "(unnamed)")
        workbook_urlid = workbook.get("workbookUrlId") or ""
        if not workbook_urlid:
            continue

        versions = sigma.get_workbook_version_history(workbook_urlid) or []
        latest = _latest_version(versions)
        latest_version_number = latest.get("version") if latest else "N/A"
        latest_tag_names = _extract_tag_names(sigma, latest)

        rows.append(
            {
                "workbookname": workbook_name,
                "urlid": workbook_urlid,
                "latest_version_number": latest_version_number,
                "INT Tag (Y/N)": "Y" if "INT" in latest_tag_names else "N",
                "Ready for UAT Tag (Y/N)": "Y" if "Ready for UAT" in latest_tag_names else "N",
                "UAT Tag (Y/N)": "Y" if "UAT" in latest_tag_names else "N",
                "VP_PROD_US Tag (Y/N)": "Y" if "VP_PROD_US" in latest_tag_names else "N",
                "VP_PROD_EU Tag (Y/N)": "Y" if "VP_PROD_EU" in latest_tag_names else "N",
                "VP_PROD_AU Tag (Y/N)": "Y" if "VP_PROD_AU" in latest_tag_names else "N",
                "last_updated": datetime.now().isoformat(timespec="seconds"),
            }
        )

    os.makedirs(os.path.dirname(args.output_csv) or ".", exist_ok=True)
    fieldnames = [
        "workbookname",
        "urlid",
        "latest_version_number",
        "INT Tag (Y/N)",
        "Ready for UAT Tag (Y/N)",
        "UAT Tag (Y/N)",
        "VP_PROD_US Tag (Y/N)",
        "VP_PROD_EU Tag (Y/N)",
        "VP_PROD_AU Tag (Y/N)",
        "last_updated",
    ]
    with open(args.output_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"Wrote {len(rows)} rows to {args.output_csv}")


if __name__ == "__main__":
    main()
