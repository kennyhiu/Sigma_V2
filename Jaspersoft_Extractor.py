from typing import Dict, List
import csv
import json
import _jaspersoft.api as api
import argparse
import configparser
import os

import re

def load_exclude_patterns(filepath: str) -> List[re.Pattern]:
    """Load exclusion patterns from a text file, one per line.

    Patterns use SQL "LIKE" syntax where `%" matches any sequence of
    characters and `_` matches a single character.  Patterns are converted to
    regular expressions for efficient matching.
    """
    patterns: List[re.Pattern] = []
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                # translate SQL-like wildcards to regex
                # escape everything, then replace escaped %/_ with regex wildcards
                regex = re.escape(line)
                regex = regex.replace(r"\%", ".*").replace(r"\_", ".")
                # ensure pattern matches anywhere in the URI (not just start)
                regex = regex
                patterns.append(re.compile(regex))
    except FileNotFoundError:
        print(f"Warning: Exclude file {filepath} not found. No exclusions applied.")
    return patterns

def export_reports_to_csv(reports: List[Dict], output_file: str) -> None:
        """
        Save basic report metadata to CSV.
        """
        if not reports:
            print("No reports found. CSV not written.")
            return

        fieldnames = sorted({
            key
            for report in reports
            if isinstance(report, dict)
            for key in report.keys()
        })

        with open(output_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            for report in reports:
                writer.writerow(report)

        print(f"CSV written: {output_file}")


def export_reports_to_json(reports: List[Dict], output_file: str) -> None:
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(reports, f, indent=2, ensure_ascii=False)
    print(f"JSON written: {output_file}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Extract reports from JasperReports Server")
    parser.add_argument("--config", type=str, default="config.ini", help="Path to config file")
    parser.add_argument("--extract", type=str, default="reports", help="reports or jobs")
    args = parser.parse_args()

    config = configparser.ConfigParser()
    config.read(args.config)

    BASE_URL = config.get("JASPERSOFT", "base_url")
    USERNAME = config.get("JASPERSOFT", "username")
    PASSWORD = config.get("JASPERSOFT", "password")

    client = api.JaspersoftClient(
        base_url=BASE_URL,
        username=USERNAME,
        password=PASSWORD,
        verify_ssl=True,   # set False only if you truly need to bypass cert validation
        timeout=60,
    )

    try:
        print("Checking server...")
        info = client.get_server_info()
        print("Connected to JasperReports Server")
        print(json.dumps(info, indent=2))

        if args.extract == 'reports':
            # Load exclusion patterns
            exclude_patterns = load_exclude_patterns("data/NonProdOrgs.txt")
            print(f"Loaded {len(exclude_patterns)} exclusion patterns.")

            print("\nFetching all reports progressively...")
            reports = client.get_all_reports(
                folder_uri="/",       # change to a narrower folder if needed
                page_size=100,        # docs say default is 100
                recursive=True,
                expanded=False,
                show_hidden_items=False,
                verbose=True,
                exclude_patterns=exclude_patterns,  # Pass exclude patterns for filtering during fetch
            )

            print(f"\nTotal reports fetched and filtered: {len(reports)}")

            # Preview first few
            for report in reports[:10]:            
                print(f"- {report.get('uri')} | {report.get('label')}")

            # determine base name from config filename (without extension)   
            base = os.path.splitext(os.path.basename(args.config))[0]
            os.makedirs("results", exist_ok=True)
            csv_path = f"results/{base}_Jaspersoft_reports.csv"
            json_path = f"results/{base}_Jaspersoft_reports.json"
            export_reports_to_csv(reports, csv_path)
            export_reports_to_json(reports, json_path)


    except Exception as e:
        print(f"ERROR: {e}") 