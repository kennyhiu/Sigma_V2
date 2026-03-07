import configparser
import time
import requests
from requests.auth import HTTPBasicAuth
from urllib.parse import quote
import argparse
import sys
import json
import csv
import os
import logging

from _api.logging_setup import setup_logging


# ---------------------------
# CLI + config
# ---------------------------

def parse_args():
    parser = argparse.ArgumentParser(description='Extract scheduled jobs from Jaspersoft Server.')
    parser.add_argument('config', help='Path to the config file (e.g., config.ini)')
    return parser.parse_args()


def read_config(config_path, logger=None):
    cfg = configparser.ConfigParser()
    read_files = cfg.read(config_path)
    if not read_files:
        if logger:
            logger.error("Could not read config file at %s", config_path)
        else:
            print(f"Error: Could not read config file at {config_path}")
        sys.exit(1)
    return cfg


# ---------------------------
# HTTP helpers
# ---------------------------

def get_response(url, username, password, params=None, logger: logging.Logger | None = None):
    r = requests.get(
        url,
        auth=HTTPBasicAuth(username, password),
        headers={'Accept': 'application/json'},
        params=params or None
    )
    try:
        payload = r.json()
    except Exception:
        payload = r.text

    if r.status_code == 200:
        return payload
    else:
        snippet = r.text[:500] if isinstance(r.text, str) else str(r.text)[:500]
        if logger:
            logger.warning("Failed GET %s: %s - %s", url, r.status_code, snippet)
        else:
            print(f"Failed GET {url}: {r.status_code} - {snippet}")
        return {"error": payload, "status_code": r.status_code}


# ---------------------------
# CSV helpers
# ---------------------------

def flatten_json(y, parent_key='', sep='.'):
    items = []
    if isinstance(y, dict):
        for k, v in y.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            items.extend(flatten_json(v, new_key, sep=sep).items())
    elif isinstance(y, list):
        if all(isinstance(i, (str, int, float, bool, type(None))) for i in y):
            items.append((parent_key, ','.join(map(str, y))))
        else:
            for idx, item in enumerate(y):
                items.extend(flatten_json(item, f"{parent_key}{sep}{idx}", sep=sep).items())
    else:
        items.append((parent_key, y))
    return dict(items)


def get_all_fieldnames(dicts):
    fieldnames = set()
    for d in dicts:
        fieldnames.update(d.keys())
    return sorted(fieldnames)


def resolve_output_path(filename):
    if os.path.dirname(filename):
        output_path = filename
    else:
        output_path = os.path.join("results", filename)
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    return output_path


def write_csv(filename, dicts, logger: logging.Logger | None = None):
    if not dicts:
        if logger:
            logger.warning("No data to write to %s", filename)
        else:
            print(f"No data to write to {filename}")
        return
    output_path = resolve_output_path(filename)
    fieldnames = get_all_fieldnames(dicts)
    with open(output_path, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(dicts)
    if logger:
        logger.info("Exported to %s", output_path)
    else:
        print(f"Exported to {output_path}")


def write_csv_fixed_fieldnames(filename, rows, fieldnames, logger: logging.Logger | None = None):
    if not rows:
        if logger:
            logger.warning("No data to write to %s", filename)
        else:
            print(f"No data to write to {filename}")
        return
    output_path = resolve_output_path(filename)
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)
    if logger:
        logger.info("Exported to %s", output_path)
    else:
        print(f"Exported to {output_path}")


# ---------------------------
# Jaspersoft: jobs
# ---------------------------

def extract_jobs(jobs_response):
    job_list = []
    if isinstance(jobs_response, dict):
        for key in ['jobsummary', 'jobSummaryList', 'jobs', 'value']:
            if key in jobs_response and isinstance(jobs_response[key], list):
                job_list = jobs_response[key]
                break
    elif isinstance(jobs_response, list):
        job_list = jobs_response
    return job_list


def flatten_state(job):
    state = job.pop('state', {})
    job['previousFireTime'] = state.get('previousFireTime')
    job['nextFireTime'] = state.get('nextFireTime')
    job['stateValue'] = state.get('value')
    return job


# ---------------------------
# Jaspersoft: report input controls (definition + values)
# ---------------------------

def get_input_controls(base_url, report_unit_uri, username, password, logger: logging.Logger | None = None):
    if not report_unit_uri:
        return None

    encoded_uri = quote(report_unit_uri, safe='/')
    url = f"{base_url}/rest_v2/reports{encoded_uri}/inputControls"
    resp = get_response(url, username, password, logger=logger)

    if isinstance(resp, list):
        return resp

    if isinstance(resp, dict):
        for key in ["inputControl", "inputControls", "items", "value", "data"]:
            if key in resp and isinstance(resp[key], list):
                return resp[key]
        if "inputControl" in resp and isinstance(resp["inputControl"], dict):
            return [resp["inputControl"]]

    if logger:
        logger.warning("Unexpected inputControls response for %s: %s", report_unit_uri, resp)
    else:
        print(f"Warning: unexpected inputControls response for {report_unit_uri}: {resp}")
    return None


def get_report_input_control_states(
    base_url,
    report_unit_uri,
    username,
    password,
    fresh_data=False,
    logger: logging.Logger | None = None,
):
    """
    GET /rest_v2/reports/<reportURI>/inputControls/values
    Returns selected state for ALL controls (what the UI shows as current/default selection).
    """
    if not report_unit_uri:
        return None

    encoded_uri = quote(report_unit_uri, safe='/')
    url = f"{base_url}/rest_v2/reports{encoded_uri}/inputControls/values"
    params = {"freshData": "true"} if fresh_data else None
    resp = get_response(url, username, password, params=params, logger=logger)

    # Expected JSON shape per many installs:
    # {"inputControlState":[{id, options[], value, uri}, ...]}
    if isinstance(resp, dict):
        if "inputControlState" in resp and isinstance(resp["inputControlState"], list):
            return resp["inputControlState"]
        if "inputControlStateList" in resp and isinstance(resp["inputControlStateList"], list):
            return resp["inputControlStateList"]

    # Sometimes it's a bare list
    if isinstance(resp, list):
        return resp

    return None


def report_states_to_selected_map(state_list):
    """
    Convert report inputControlState list -> dict[paramId] = list[str] selected values
    """
    out = {}
    if not isinstance(state_list, list):
        return out

    for st in state_list:
        if not isinstance(st, dict):
            continue
        pid = st.get("id")
        if not pid:
            continue

        selected = []

        # single value
        if "value" in st and st["value"] not in (None, ""):
            selected.append(str(st["value"]))

        # option selections
        opts = st.get("options")
        if isinstance(opts, list):
            for o in opts:
                if isinstance(o, dict) and o.get("selected"):
                    v = o.get("value")
                    if v is None:
                        continue
                    selected.append(str(v))

        # dedup preserve order
        seen = set()
        deduped = []
        for v in selected:
            if v not in seen:
                seen.add(v)
                deduped.append(v)

        out[pid] = deduped

    return out


# ---------------------------
# Scheduled job parameters extraction (job saved state)
# ---------------------------

def extract_job_selected_map(parameter_values):
    """
    Convert job parameterValues -> dict[paramId] = list[str] selected values
    Handles payload shapes:
      - {"inputControlState":[...]}
      - [...]
      - primitive / None
    """
    out = {}
    if not isinstance(parameter_values, dict):
        return out

    for param_id, payload in parameter_values.items():
        if isinstance(payload, dict):
            states = payload.get("inputControlState", [])
        elif isinstance(payload, list):
            states = payload
        elif payload is None:
            states = []
        else:
            states = [{"value": payload}]

        selected = []
        for st in states:
            if not isinstance(st, dict):
                selected.append(str(st))
                continue

            if "value" in st and st["value"] not in (None, ""):
                selected.append(str(st["value"]))

            opts = st.get("options")
            if isinstance(opts, list):
                for o in opts:
                    if isinstance(o, dict) and o.get("selected"):
                        v = o.get("value")
                        if v is None:
                            continue
                        selected.append(str(v))

        # dedup preserve order
        seen = set()
        deduped = []
        for v in selected:
            if v not in seen:
                seen.add(v)
                deduped.append(v)

        out[param_id] = deduped

    return out


def extract_parameter_rows(job_id, report_unit_uri, parameter_values):
    """
    Long-format CSV rows for scheduled job parameter state (selected values + selected labels + full options snapshot).
    """
    rows = []
    if not isinstance(parameter_values, dict):
        return rows

    def dedup(seq):
        seen = set()
        out = []
        for x in seq:
            if x not in seen:
                seen.add(x)
                out.append(x)
        return out

    for param_id, payload in parameter_values.items():
        if isinstance(payload, dict):
            states = payload.get("inputControlState", [])
        elif isinstance(payload, list):
            states = payload
        elif payload is None:
            states = []
        else:
            states = [{"value": payload}]

        selected_values = []
        selected_labels = []
        all_options = []

        for st in states:
            if not isinstance(st, dict):
                selected_values.append(str(st))
                continue

            if "value" in st and st["value"] not in (None, ""):
                selected_values.append(str(st["value"]))

            opts = st.get("options")
            if isinstance(opts, list):
                for o in opts:
                    if not isinstance(o, dict):
                        continue
                    label = "" if o.get("label") is None else str(o.get("label"))
                    value = "" if o.get("value") is None else str(o.get("value"))
                    sel = bool(o.get("selected"))
                    all_options.append({"label": label, "value": value, "selected": sel})
                    if sel:
                        if value != "":
                            selected_values.append(value)
                        if label != "":
                            selected_labels.append(label)

        selected_values = dedup(selected_values)
        selected_labels = dedup(selected_labels)

        rows.append({
            "jobId": job_id,
            "reportUnitURI": report_unit_uri or "",
            "paramId": param_id,
            "selectedValues": ",".join(selected_values),
            "selectedLabels": ",".join(selected_labels),
            "allOptionsJson": json.dumps(all_options) if all_options else ""
        })

    return rows


# ---------------------------
# Comparison normalization (fix ~NULL~ vs ~NOTHING~ vs blanks)
# ---------------------------

def normalize_ic_value(v):
    """
    Normalize Jaspersoft 'null-ish' values so comparisons are meaningful.
    Treat these as None/unset:
      - None
      - "" (empty)
      - whitespace-only
      - "~NULL~"
      - "~NOTHING~"
    """
    if v is None:
        return None
    s = str(v).strip()
    if s == "":
        return None
    if s.upper() in {"~NULL~", "~NOTHING~"}:
        return None
    return s


def normalize_value_list(vals):
    """
    Normalize list values and drop null-ish items.
    Return a stable list with dedup preserved.
    """
    if not vals:
        return []
    out = []
    seen = set()
    for v in vals:
        nv = normalize_ic_value(v)
        if nv is None:
            continue
        if nv not in seen:
            seen.add(nv)
            out.append(nv)
    return out


# ---------------------------
# Comparison: job selected vs report selected/default (from inputControls/values)
# ---------------------------

def compare_job_to_report_selected(job_selected_map, report_selected_map):
    """
    Returns rows: paramId, reportSelectedValues, jobSelectedValues, different
    Uses normalization so blanks/~NULL~/~NOTHING~ are treated as the same.
    """
    rows = []
    all_param_ids = sorted(set(job_selected_map.keys()) | set(report_selected_map.keys()))

    for pid in all_param_ids:
        job_vals_raw = job_selected_map.get(pid, [])
        rpt_vals_raw = report_selected_map.get(pid, [])

        job_vals = normalize_value_list(job_vals_raw)
        rpt_vals = normalize_value_list(rpt_vals_raw)

        # If both are effectively unset, it's NOT a difference
        if not job_vals and not rpt_vals:
            different = "NO"
        else:
            different = "NO" if set(job_vals) == set(rpt_vals) else "YES"

        rows.append({
            "paramId": pid,
            "reportSelectedValues": ",".join(rpt_vals),
            "jobSelectedValues": ",".join(job_vals),
            "different": different,
            # Uncomment if you want troubleshooting columns:
            # "reportSelectedValuesRaw": ",".join(map(str, rpt_vals_raw)) if rpt_vals_raw else "",
            # "jobSelectedValuesRaw": ",".join(map(str, job_vals_raw)) if job_vals_raw else "",
        })
    return rows


# ---------------------------
# Owner credential parsing (your org-specific behavior)
# ---------------------------

def parse_owner_credentials(owner_str: str):
    """
    Your org-specific login behavior (as you stated):
      - username is the full owner string (e.g. "username|orgID")
      - password is the portion before the first pipe (username portion)
    """
    if not owner_str:
        return None, None
    if '|' in owner_str:
        u = owner_str.strip()
        p = owner_str.split('|', 1)[0].strip()
        return u, p
    return owner_str.strip(), None


# ---------------------------
# Main
# ---------------------------

def main():
    logger = setup_logging("logs")
    args = parse_args()
    cfg = read_config(args.config, logger=logger)
    logger.info("Starting Jaspersoft extractor with config: %s", args.config)

    base_url = cfg['JASPERSOFT']['base_url']
    service_username = cfg['JASPERSOFT']['username']
    service_password = cfg['JASPERSOFT']['password']

    jobs_url = f"{base_url}/rest_v2/jobs"

    # Fetch jobs using service account
    jobs_response = get_response(jobs_url, service_username, service_password, logger=logger)
    job_list = extract_jobs(jobs_response)

    if not job_list:
        logger.info("No jobs found.")
        sys.exit(0)

    logger.info("%s total jobs found.", len(job_list))
    logger.info("Sample jobs:\n%s", json.dumps(job_list[:2], indent=2))

    # Flatten state for summary CSV
    for job in job_list:
        flatten_state(job)

    write_csv('VP_PROD_US_Scheduled_Job_Summary.csv', job_list, logger=logger)

    # Output collections
    job_details_rows = []
    job_param_rows = []
    job_vs_report_rows = []
    report_ic_rows = []  # optional report input control definitions

    # Cache report selected/default states per report URI
    report_state_cache = {}  # reportUnitURI -> selected_map(dict)

    # Process jobs (change [:2] to job_list when ready)
    for job in job_list:
        job_id = job.get('id')
        if not job_id:
            continue

        owner_raw = job.get('owner', '')
        job_owner_user, job_owner_pass = parse_owner_credentials(owner_raw)
        if not job_owner_user or not job_owner_pass:
            logger.warning("Skipping job %s: owner credentials missing/invalid. owner='%s'", job_id, owner_raw)
            continue

        logger.info("Processing job ID: %s", job_id)
        job_details_url = f"{base_url}/rest_v2/jobs/{job_id}"
        job_details = get_response(job_details_url, job_owner_user, job_owner_pass, logger=logger)

        if not (isinstance(job_details, dict) and "error" not in job_details):
            logger.warning("Skipping job %s due to error.", job_id)
            continue

        report_unit_uri = job_details.get('source', {}).get('reportUnitURI')
        logger.info("job=%s report_unit_uri=%s", job_id, report_unit_uri)

        # Job details CSV (flattened)
        job_copy = job.copy()
        job_copy.update(job_details)
        job_details_rows.append(flatten_json(job_copy))

        # Job parameter values (job saved state)
        parameter_values = (
            job_details.get("source", {})
                       .get("parameters", {})
                       .get("parameterValues", {})
        )

        job_param_rows.extend(extract_parameter_rows(job_id, report_unit_uri, parameter_values))
        job_selected_map = extract_job_selected_map(parameter_values)

        # Report selected/default state (what report shows with no overrides)
        if report_unit_uri and report_unit_uri not in report_state_cache:
            state_list = get_report_input_control_states(
                base_url,
                report_unit_uri,
                job_owner_user,
                job_owner_pass,
                fresh_data=False,
                logger=logger,
            )
            report_state_cache[report_unit_uri] = report_states_to_selected_map(state_list)

        report_selected_map = report_state_cache.get(report_unit_uri, {}) if report_unit_uri else {}

        # Compare job vs report (normalized)
        diff_rows = compare_job_to_report_selected(job_selected_map, report_selected_map)
        for dr in diff_rows:
            job_vs_report_rows.append({
                "jobId": job_id,
                "reportUnitURI": report_unit_uri or "",
                "paramId": dr["paramId"],
                "reportSelectedValues": dr["reportSelectedValues"],
                "jobSelectedValues": dr["jobSelectedValues"],
                "different": dr["different"]
            })

        # Optional: report input control definitions (structure)
        input_controls = get_input_controls(
            base_url, report_unit_uri, job_owner_user, job_owner_pass, logger=logger
        )
        if isinstance(input_controls, list):
            for ic in input_controls:
                report_ic_rows.append({
                    "reportUnitURI": report_unit_uri or "",
                    "controlId": ic.get("id", ""),
                    "controlLabel": ic.get("label", ""),
                    "controlType": ic.get("type", ""),
                    "mandatory": ic.get("mandatory", ""),
                    "readOnly": ic.get("readOnly", ""),
                    "visible": ic.get("visible", ""),
                    "controlDefinitionJson": json.dumps(ic)
                })

        time.sleep(0.1)

    # Write outputs
    write_csv('VP_PROD_US_Scheduled_Job_Details.csv', job_details_rows, logger=logger)

    write_csv_fixed_fieldnames(
        "VP_PROD_US_Scheduled_Job_InputControlParams.csv",
        job_param_rows,
        fieldnames=["jobId", "reportUnitURI", "paramId", "selectedValues", "selectedLabels", "allOptionsJson"],
        logger=logger,
    )

    write_csv_fixed_fieldnames(
        "VP_PROD_US_JobVsReport_ParamDiffs.csv",
        job_vs_report_rows,
        fieldnames=["jobId", "reportUnitURI", "paramId", "reportSelectedValues", "jobSelectedValues", "different"],
        logger=logger,
    )

    write_csv_fixed_fieldnames(
        "VP_PROD_US_Report_InputControls.csv",
        report_ic_rows,
        fieldnames=[
            "reportUnitURI",
            "controlId",
            "controlLabel",
            "controlType",
            "mandatory",
            "readOnly",
            "visible",
            "controlDefinitionJson"
        ],
        logger=logger,
    )

    logger.info("Jaspersoft extraction completed.")


if __name__ == "__main__":
    main()
