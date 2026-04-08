"""
Fetch task logs from Airflow REST API.

Single responsibility: talks to Airflow only.
Log pre-processing: extracts exception, stack trace, and last N lines.

Auth flow: POST /auth/token → Bearer token for subsequent requests.
Log fetch: GET /tries → find latest try_number → GET /logs/{try_number}
"""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass, field

import httpx

from bau.config import settings

LOG_TAIL_LINES = 150

logger = logging.getLogger(__name__)


@dataclass
class ProcessedLog:
    raw_log: str
    exception: str | None = None
    stack_trace: str | None = None
    tail_lines: list[str] = field(default_factory=list)
    source: str = "airflow"


def _extract_exception(log_text: str) -> str | None:
    """Extract the last exception line from log text."""
    # Match common Python exception patterns
    patterns = [
        r"^(\w+(?:\.\w+)*(?:Error|Exception|Fault|Failure).*)$",
        r"^(raise \w+.*)$",
    ]
    matches = []
    for pattern in patterns:
        matches.extend(re.findall(pattern, log_text, re.MULTILINE))
    return matches[-1].strip() if matches else None


def _extract_stack_trace(log_text: str) -> str | None:
    """Extract the last Traceback block from log text (including the final exception line)."""
    # Find all traceback blocks — capture from "Traceback" through indented lines and the exception line
    tb_pattern = re.compile(
        r"(Traceback \(most recent call last\):\n(?:[ \t]+.*\n)*\S.*)",
        re.MULTILINE,
    )
    matches = tb_pattern.findall(log_text)
    if matches:
        return matches[-1].strip()
    return None


def process_log(raw_log: str, source: str = "airflow") -> ProcessedLog:
    """Pre-process raw log text: extract exception, stack trace, last N lines."""
    lines = raw_log.splitlines() if raw_log else []
    return ProcessedLog(
        raw_log=raw_log,
        exception=_extract_exception(raw_log),
        stack_trace=_extract_stack_trace(raw_log),
        tail_lines=lines[-LOG_TAIL_LINES:] if lines else [],
        source=source,
    )


async def _get_token(client: httpx.AsyncClient) -> str:
    """Authenticate with Airflow and return a JWT Bearer token."""
    base = settings.airflow_base_url.rstrip("/")
    token_url = f"{base}/auth/token"

    payload = {
        "username": settings.airflow_username,
        "password": settings.airflow_password,
    }

    resp = await client.post(
        token_url,
        json=payload,
        auth=(settings.airflow_username, settings.airflow_password),
        headers={"Content-Type": "application/json"},
        timeout=10.0,
    )
    resp.raise_for_status()

    token_data = resp.json()
    token = token_data.get("access_token")
    if not token:
        raise RuntimeError(f"Airflow token endpoint returned no access_token: {token_data}")
    return token


async def _get_latest_try_number(
    client: httpx.AsyncClient,
    headers: dict,
    dag_id: str,
    dag_run_id: str,
    task_id: str,
) -> int:
    """Fetch task tries and return the highest try_number."""
    base = settings.airflow_base_url.rstrip("/")
    url = f"{base}/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/tries"

    resp = await client.get(url, headers=headers, timeout=30.0)
    resp.raise_for_status()

    data = resp.json()
    tries = data.get("task_instances", [])
    if not tries:
        raise RuntimeError(
            f"No tries found for {dag_id}/{dag_run_id}/{task_id}"
        )

    latest = max(tries, key=lambda t: t["try_number"])
    return latest["try_number"]


def _json_log_to_text(data: dict) -> str:
    """Convert Airflow v2 JSON log response to plain text.

    The v2 API returns {"content": [{"event": "...", "timestamp": "...", ...}, ...]}.
    We reconstruct a readable log from the event fields, including error_detail
    tracebacks when present.
    """
    content = data.get("content", [])
    lines = []
    for entry in content:
        event = entry.get("event", "")
        timestamp = entry.get("timestamp", "")

        # Format: [timestamp] event
        if timestamp and event:
            lines.append(f"[{timestamp}] {event}")
        elif event:
            lines.append(event)

        # Expand error_detail into traceback lines
        error_detail = entry.get("error_detail", [])
        for err in error_detail:
            frames = err.get("frames", [])
            lines.append("Traceback (most recent call last):")
            for frame in frames:
                lines.append(
                    f'  File "{frame.get("filename", "?")}", '
                    f'line {frame.get("lineno", "?")}, '
                    f'in {frame.get("name", "?")}'
                )
            exc_type = err.get("exc_type", "")
            exc_value = err.get("exc_value", "")
            if exc_type:
                lines.append(f"{exc_type}: {exc_value}")

    return "\n".join(lines)


async def get_airflow_log(
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    try_number: int | None = None,
) -> ProcessedLog:
    """
    Fetch task log from Airflow REST API and return processed result.

    Auth: JWT token-based (POST /auth/token first).
    If try_number is None, fetches the latest try automatically.
    """
    base = settings.airflow_base_url.rstrip("/")

    async with httpx.AsyncClient(verify=False) as client:
        token = await _get_token(client)
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }

        if try_number is None:
            try_number = await _get_latest_try_number(
                client, headers, dag_id, dag_run_id, task_id
            )
            logger.info(f"Latest try number for {dag_id}/{task_id}: {try_number}")

        url = f"{base}/api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/logs/{try_number}"

        resp = await client.get(
            url,
            headers={**headers, "Accept": "application/json"},
            timeout=30.0,
        )
        resp.raise_for_status()
        raw_log = _json_log_to_text(resp.json())

    return process_log(raw_log, source="airflow")


async def get_full_airflow_log(
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    try_number: int | None = None,
) -> str:
    """Fetch untruncated log from Airflow. Used as agent fallback tool."""
    result = await get_airflow_log(dag_id, dag_run_id, task_id, try_number)
    return result.raw_log
