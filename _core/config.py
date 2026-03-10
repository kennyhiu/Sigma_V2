from __future__ import annotations

import configparser
from pathlib import Path
from typing import Any, Dict


def _to_bool(value: str, default: bool) -> bool:
    if value is None:
        return default
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "y", "on"}:
        return True
    if normalized in {"0", "false", "no", "n", "off"}:
        return False
    return default


def load_config(path: str) -> Dict[str, Any]:
    config_path = Path(path).resolve()
    parser = configparser.ConfigParser()
    read_files = parser.read(config_path)
    if not read_files:
        raise RuntimeError(f"Unable to read config file: {config_path}")

    if "SIGMA" not in parser:
        raise RuntimeError(f"Missing [SIGMA] section in {config_path}")

    sigma = parser["SIGMA"]
    settings = parser["SETTINGS"] if "SETTINGS" in parser else {}

    base_url = sigma.get("base_url", "").strip().rstrip("/")
    client_id = sigma.get("client_id", "").strip()
    client_secret = sigma.get("client_secret", "").strip()

    if not base_url or not client_id or not client_secret:
        raise RuntimeError(
            f"Config {config_path} must include SIGMA.base_url, SIGMA.client_id, SIGMA.client_secret"
        )

    page_limit_raw = settings.get("page_limit", "200")
    timeout_raw = settings.get("timeout_seconds", settings.get("request_timeout_seconds", "60"))
    sleep_raw = settings.get("request_sleep_seconds", "0")
    dry_run_raw = settings.get("dry_run", "false")

    try:
        page_limit = int(page_limit_raw)
    except ValueError as exc:
        raise RuntimeError(f"Invalid SETTINGS.page_limit in {config_path}: {page_limit_raw}") from exc

    try:
        timeout = float(timeout_raw)
    except ValueError as exc:
        raise RuntimeError(f"Invalid timeout setting in {config_path}: {timeout_raw}") from exc

    try:
        sleep_seconds = float(sleep_raw)
    except ValueError as exc:
        raise RuntimeError(f"Invalid SETTINGS.request_sleep_seconds in {config_path}: {sleep_raw}") from exc

    return {
        "config_path": str(config_path),
        "config_name": config_path.stem,
        "base_url": base_url,
        "client_id": client_id,
        "client_secret": client_secret,
        "limit": max(1, page_limit),
        "timeout": max(1.0, timeout),
        "request_sleep_seconds": max(0.0, sleep_seconds),
        "dry_run": _to_bool(dry_run_raw, default=False),
        "log_dir": str((config_path.parent.parent / "logs").resolve()),
    }

