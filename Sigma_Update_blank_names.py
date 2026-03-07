from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

from _sigma.auth import get_actor_token
from _api.config import load_config
from _api.http_client import paginate, request_json
from _api.logging_setup import setup_logging


EMAIL_REGEX = re.compile(r"^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$", re.IGNORECASE)
ALLOWED_DOMAINS = {"powerfleet.com", "mixtelematics.com"}


def _pick(*values: Optional[str]) -> Optional[str]:
	for value in values:
		if isinstance(value, str) and value.strip():
			return value.strip()
	return None


def _extract_names(member: Dict[str, Any]) -> Tuple[Optional[str], Optional[str], Optional[str]]:
	profile = member.get("profile") or {}
	user = member.get("user") or {}

	first = _pick(member.get("firstName"), profile.get("firstName"), user.get("firstName"))
	last = _pick(member.get("lastName"), profile.get("lastName"), user.get("lastName"))
	email = _pick(member.get("email"), profile.get("email"), user.get("email"))

	return first, last, email


def _is_email_like(value: Optional[str]) -> bool:
	if not value:
		return False
	return EMAIL_REGEX.match(value) is not None


def _normalize_name(value: str) -> str:
	cleaned = re.sub(r"[_-]+", " ", value.strip())
	cleaned = re.sub(r"\s+", " ", cleaned)
	return cleaned.title().strip()


def _infer_names_from_email(email: str) -> Optional[Tuple[str, str]]:
	if "@" not in email:
		return None

	local, domain = email.rsplit("@", 1)
	domain = domain.lower().strip()
	if domain not in ALLOWED_DOMAINS:
		return None

	local = local.strip()
	if not local:
		return None

	if "." in local:
		parts = [p for p in local.split(".") if p]
		if len(parts) < 2:
			return None
		first_part = parts[0]
		last_part = parts[-1]
	else:
		if len(local) < 2:
			return None
		first_part = local[0]
		last_part = local[1:]

	first = _normalize_name(first_part)
	last = _normalize_name(last_part)
	if not first or not last:
		return None
	return first, last


def _should_flag(first: Optional[str], last: Optional[str]) -> Tuple[bool, List[str]]:
	reasons: List[str] = []
	if not first:
		reasons.append("blank_first")
	if not last:
		reasons.append("blank_last")
	if _is_email_like(first):
		reasons.append("first_like_email")
	if _is_email_like(last):
		reasons.append("last_like_email")
	return bool(reasons), reasons


def _find_config_files(root: Path) -> List[Path]:
	config_dir = root / "config_files"
	files = sorted(config_dir.glob("*.ini")) if config_dir.exists() else []
	if files:
		return files
	return sorted(root.glob("config*.ini"))


def _iter_members(cfg: Dict[str, Any], actor_token: str, logger) -> Iterable[Dict[str, Any]]:
	return paginate(
		cfg["base_url"],
		actor_token,
		"/v2/members",
		logger,
		limit=cfg["limit"],
		timeout=cfg["timeout"],
		sleep_seconds=cfg.get("request_sleep_seconds", 0.0),
	)


def _process_config(config_path: Path) -> Dict[str, int]:
	cfg = load_config(str(config_path))
	logger = setup_logging(cfg["log_dir"])

	logger.info(f"Config file: {config_path}")
	logger.info(f"Base URL: {cfg['base_url']}")

	actor_token = get_actor_token(cfg["base_url"], cfg["client_id"], cfg["client_secret"], cfg["timeout"], logger)
	logger.info("Authenticated (actor token).")

	totals = {
		"members_scanned": 0,
		"members_flagged": 0,
		"members_updated": 0,
		"members_skipped_domain": 0,
		"members_skipped_parse": 0,
		"members_skipped_no_change": 0,
		"update_failures": 0,
		"blank_first": 0,
		"blank_last": 0,
		"first_like_email": 0,
		"last_like_email": 0,
	}

	for member in _iter_members(cfg, actor_token, logger):
		totals["members_scanned"] += 1

		member_id = _pick(member.get("memberId"), member.get("id")) or "(unknown)"
		first, last, email = _extract_names(member)
		flagged, reasons = _should_flag(first, last)

		if not flagged:
			continue

		totals["members_flagged"] += 1
		for reason in reasons:
			totals[reason] += 1

		logger.info(
			"Flagged memberId=%s email=%s first=%s last=%s reasons=%s",
			member_id,
			email or "(none)",
			first or "(blank)",
			last or "(blank)",
			",".join(reasons),
		)

		if member_id == "(unknown)":
			totals["members_skipped_parse"] += 1
			logger.info("Skip update (missing memberId) email=%s", email or "(none)")
			continue

		if not email:
			totals["members_skipped_parse"] += 1
			logger.info("Skip update (no email) for memberId=%s", member_id)
			continue

		inferred = _infer_names_from_email(email)
		if not inferred:
			totals["members_skipped_domain"] += 1
			logger.info("Skip update (domain or parse) for memberId=%s email=%s", member_id, email)
			continue

		new_first, new_last = inferred
		if (first or "").strip().lower() == new_first.lower() and (last or "").strip().lower() == new_last.lower():
			totals["members_skipped_no_change"] += 1
			logger.info("Skip update (no change) for memberId=%s", member_id)
			continue

		try:
			request_json(
				"PATCH",
				cfg["base_url"],
				f"/v2/members/{member_id}",
				actor_token,
				logger,
				json_data={"firstName": new_first, "lastName": new_last},
				timeout=cfg["timeout"],
				allow_404=False,
			)
			totals["members_updated"] += 1
			logger.info(
				"Updated memberId=%s email=%s first=%s last=%s",
				member_id,
				email,
				new_first,
				new_last,
			)
		except Exception as exc:
			totals["update_failures"] += 1
			logger.error("Update failed for memberId=%s email=%s err=%s", member_id, email, exc)

	logger.info("===== Summary =====")
	logger.info(f"Members scanned: {totals['members_scanned']}")
	logger.info(f"Members flagged: {totals['members_flagged']}")
	logger.info(f"Members updated: {totals['members_updated']}")
	logger.info(f"Skipped (no email/memberId): {totals['members_skipped_parse']}")
	logger.info(f"Skipped (domain/parse): {totals['members_skipped_domain']}")
	logger.info(f"Skipped (no change): {totals['members_skipped_no_change']}")
	logger.info(f"Update failures: {totals['update_failures']}")
	logger.info(f"Blank first name: {totals['blank_first']}")
	logger.info(f"Blank last name: {totals['blank_last']}")
	logger.info(f"First name like email: {totals['first_like_email']}")
	logger.info(f"Last name like email: {totals['last_like_email']}")
	logger.info("Done.")

	return totals


def main() -> None:
	root = Path(__file__).resolve().parent
	config_files = _find_config_files(root)

	if not config_files:
		raise RuntimeError(f"No config*.ini files found in {root}")

	for config_path in config_files:
		try:
			_process_config(config_path)
		except Exception as exc:
			logger = setup_logging("logs")
			logger.error(f"Failed to process {config_path}: {exc}")


if __name__ == "__main__":
	main()
