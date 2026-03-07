import argparse
import configparser
import csv
import json
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Iterable, List, Set
from urllib.parse import quote

import requests
from requests.auth import HTTPBasicAuth


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Extract Jaspersoft report metadata for report_uri values in a CSV file."
    )
    parser.add_argument(
        "--config",
        default="config_files/vp_prod_us.ini",
        help="INI file containing [JASPERSOFT] credentials.",
    )
    parser.add_argument(
        "--input-csv",
        default="AllReportsAndUsageData.csv",
        help="CSV with a report_uri column.",
    )
    parser.add_argument(
        "--output-csv",
        default="results/Jaspersoft_AllReportsAndUsageData.csv",
        help="Output CSV path.",
    )
    parser.add_argument(
        "--delimiter",
        default=";",
        help="Input CSV delimiter.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Optional max number of unique report URIs to fetch (0 = no limit).",
    )
    parser.add_argument(
        "--min-run-count-60d",
        type=float,
        default=None,
        help="If set, only include rows where run_count_60d is greater than this value.",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=30.0,
        help="HTTP timeout in seconds.",
    )
    parser.add_argument(
        "--insecure",
        action="store_true",
        help="Disable TLS certificate verification.",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=16,
        help="Concurrent workers for API requests.",
    )
    return parser.parse_args()


def load_jaspersoft_config(config_path: str) -> Dict[str, str]:
    cfg = configparser.ConfigParser()
    if not cfg.read(config_path):
        raise RuntimeError(f"Could not read config file: {config_path}")
    if "JASPERSOFT" not in cfg:
        raise RuntimeError(f"Missing [JASPERSOFT] section in {config_path}")

    section = cfg["JASPERSOFT"]
    base_url = section.get("base_url", "").strip().rstrip("/")
    username = section.get("username", "").strip()
    password = section.get("password", "").strip()

    if not base_url or not username or not password:
        raise RuntimeError(
            f"Config {config_path} must include JASPERSOFT.base_url, username, password."
        )

    return {"base_url": base_url, "username": username, "password": password}


def _parse_float_or_zero(raw: str) -> float:
    cleaned = (raw or "").strip().replace(",", "")
    if not cleaned:
        return 0.0
    try:
        return float(cleaned)
    except ValueError:
        return 0.0


def read_unique_report_uris(input_csv: str, delimiter: str, min_run_count_60d=None) -> List[str]:
    uris: List[str] = []
    seen: Set[str] = set()
    with open(input_csv, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter=delimiter, quotechar='"')
        if "report_uri" not in (reader.fieldnames or []):
            raise RuntimeError(f"Input CSV is missing report_uri column: {input_csv}")
        if min_run_count_60d is not None and "run_count_60d" not in (reader.fieldnames or []):
            raise RuntimeError(f"Input CSV is missing run_count_60d column: {input_csv}")
        for row in reader:
            if min_run_count_60d is not None:
                run_count = _parse_float_or_zero(row.get("run_count_60d") or "")
                if run_count <= min_run_count_60d:
                    continue
            uri = (row.get("report_uri") or "").strip()
            if not uri:
                continue
            if uri not in seen:
                seen.add(uri)
                uris.append(uri)
    return uris


def flatten_json(value, parent_key: str = "", sep: str = ".") -> Dict[str, str]:
    out: Dict[str, str] = {}
    if isinstance(value, dict):
        for key, child in value.items():
            new_key = f"{parent_key}{sep}{key}" if parent_key else str(key)
            out.update(flatten_json(child, new_key, sep))
    elif isinstance(value, list):
        if all(isinstance(i, (str, int, float, bool, type(None))) for i in value):
            out[parent_key] = ",".join("" if v is None else str(v) for v in value)
        else:
            out[parent_key] = json.dumps(value, ensure_ascii=True)
    else:
        out[parent_key] = "" if value is None else str(value)
    return out


def fetch_report_metadata(
    base_url: str,
    username: str,
    password: str,
    report_uri: str,
    timeout: float,
    verify: bool,
) -> Dict[str, str]:
    encoded_uri = quote(report_uri, safe="/")
    url = f"{base_url}/rest_v2/resources{encoded_uri}"
    try:
        resp = requests.get(
            url,
            auth=HTTPBasicAuth(username, password),
            headers={"Accept": "application/json"},
            timeout=timeout,
            verify=verify,
        )
    except Exception as exc:
        return {
            "report_uri": report_uri,
            "http_status": "",
            "error_message": str(exc),
        }

    row: Dict[str, str] = {
        "report_uri": report_uri,
        "http_status": str(resp.status_code),
    }

    if resp.status_code != 200:
        row["error_message"] = (resp.text or "").strip()[:1000]
        return row

    try:
        payload = resp.json()
    except Exception:
        row["error_message"] = "Expected JSON but received non-JSON response."
        row["raw_body"] = (resp.text or "").strip()[:1000]
        return row

    row.update(flatten_json(payload, parent_key="metadata"))
    return row


def write_rows(output_csv: str, rows: Iterable[Dict[str, str]]) -> None:
    rows = list(rows)
    if not rows:
        raise RuntimeError("No rows to write.")

    os.makedirs(os.path.dirname(output_csv) or ".", exist_ok=True)
    fieldnames = sorted({k for r in rows for k in r.keys()})
    with open(output_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    args = parse_args()
    conf = load_jaspersoft_config(args.config)
    verify = not args.insecure

    report_uris = read_unique_report_uris(
        args.input_csv,
        args.delimiter,
        min_run_count_60d=args.min_run_count_60d,
    )
    if args.limit > 0:
        report_uris = report_uris[: args.limit]

    print(f"Found {len(report_uris)} unique report_uri values.")
    rows: List[Dict[str, str]] = []
    worker_count = max(1, args.workers)
    print(f"Using {worker_count} workers.", flush=True)

    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        future_to_uri = {
            executor.submit(
                fetch_report_metadata,
                conf["base_url"],
                conf["username"],
                conf["password"],
                uri,
                args.timeout,
                verify,
            ): uri
            for uri in report_uris
        }

        completed = 0
        total = len(future_to_uri)
        for future in as_completed(future_to_uri):
            completed += 1
            rows.append(future.result())
            if completed % 250 == 0 or completed == 1 or completed == total:
                print(f"Completed {completed}/{total}", flush=True)

    write_rows(args.output_csv, rows)

    ok = sum(1 for r in rows if r.get("http_status") == "200")
    failed = len(rows) - ok
    print(f"Wrote {len(rows)} rows to {args.output_csv}. success={ok} failed={failed}")


if __name__ == "__main__":
    main()
