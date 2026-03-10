from __future__ import annotations

import json
import time
from typing import Any, Dict, Iterable, List, Optional
from urllib import error, parse, request


def _normalize_path(path: str) -> str:
    return path if path.startswith("/") else f"/{path}"


def request_json(
    method: str,
    base_url: str,
    path: str,
    actor_token: str,
    logger,
    json_data: Optional[Dict[str, Any]] = None,
    params: Optional[Dict[str, Any]] = None,
    timeout: float = 60.0,
    allow_404: bool = False,
) -> Any:
    url = f"{base_url.rstrip('/')}{_normalize_path(path)}"
    headers = {
        "Accept": "application/json",
        "Authorization": f"Bearer {actor_token}",
    }
    if json_data is not None:
        headers["Content-Type"] = "application/json"

    if params:
        query = parse.urlencode(params)
        separator = "&" if "?" in url else "?"
        url = f"{url}{separator}{query}"

    body = json.dumps(json_data).encode("utf-8") if json_data is not None else None
    req = request.Request(url=url, data=body, headers=headers, method=method.upper())

    try:
        with request.urlopen(req, timeout=timeout) as response:
            status_code = response.getcode()
            raw = response.read()
    except error.HTTPError as exc:
        if allow_404 and exc.code == 404:
            return None
        text = exc.read().decode("utf-8", errors="replace").strip()
        logger.error(
            "HTTP %s %s failed status=%s body=%s",
            method.upper(),
            url,
            exc.code,
            text[:1000],
        )
        raise RuntimeError(f"HTTP {exc.code} for {method.upper()} {url}") from exc

    if allow_404 and status_code == 404:
        return None

    if status_code >= 400:
        raise RuntimeError(f"HTTP {status_code} for {method.upper()} {url}")

    if not raw:
        return {}

    try:
        return json.loads(raw.decode("utf-8"))
    except ValueError as exc:
        raise RuntimeError(f"Expected JSON response for {method.upper()} {url}") from exc


def _extract_page_entries(payload: Any) -> List[Dict[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if not isinstance(payload, dict):
        return []

    for key in ("entries", "data", "items", "members"):
        value = payload.get(key)
        if isinstance(value, list):
            return [item for item in value if isinstance(item, dict)]
    return []


def _extract_next_token(payload: Any) -> Optional[str]:
    if not isinstance(payload, dict):
        return None

    for key in ("nextPageToken", "next_page_token"):
        token = payload.get(key)
        if isinstance(token, str) and token.strip():
            return token.strip()

    next_page = payload.get("nextPage")
    if isinstance(next_page, dict):
        token = next_page.get("token") or next_page.get("nextPageToken")
        if isinstance(token, str) and token.strip():
            return token.strip()

    pagination = payload.get("pagination")
    if isinstance(pagination, dict):
        token = pagination.get("nextPageToken") or pagination.get("next_page_token")
        if isinstance(token, str) and token.strip():
            return token.strip()

    return None


def paginate(
    base_url: str,
    actor_token: str,
    path: str,
    logger,
    limit: int = 200,
    timeout: float = 60.0,
    sleep_seconds: float = 0.0,
) -> Iterable[Dict[str, Any]]:
    next_token: Optional[str] = None
    page_num = 0

    while True:
        page_num += 1
        params: Dict[str, Any] = {"limit": limit}
        if next_token:
            params["nextPageToken"] = next_token

        payload = request_json(
            "GET",
            base_url,
            path,
            actor_token,
            logger,
            params=params,
            timeout=timeout,
        )
        rows = _extract_page_entries(payload)
        logger.info("Fetched page=%s rows=%s path=%s", page_num, len(rows), path)
        for row in rows:
            yield row

        token = _extract_next_token(payload)
        if not token:
            break
        if token == next_token:
            logger.warning("Detected repeated next token at page=%s, stopping pagination.", page_num)
            break

        next_token = token
        if sleep_seconds > 0:
            time.sleep(sleep_seconds)
