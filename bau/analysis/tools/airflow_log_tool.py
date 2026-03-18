"""
Fetch task logs from Airflow REST API.

Single responsibility: talks to Airflow only.
Log pre-processing: extracts exception, stack trace, and last N lines.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field

import httpx

from bau.config import settings

LOG_TAIL_LINES = 150


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


async def get_airflow_log(
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    try_number: int = -1,
) -> ProcessedLog:
    """
    Fetch task log from Airflow REST API and return processed result.

    Args:
        dag_id: The DAG identifier.
        dag_run_id: The DAG run identifier.
        task_id: The task identifier.
        try_number: Which try to fetch (-1 = latest).
    """
    base = settings.airflow_base_url.rstrip("/")
    url = f"{base}/api/v1/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/logs/{try_number}"

    async with httpx.AsyncClient() as client:
        resp = await client.get(
            url,
            auth=(settings.airflow_username, settings.airflow_password),
            headers={"Accept": "text/plain"},
            timeout=30.0,
        )
        resp.raise_for_status()
        raw_log = resp.text

    return process_log(raw_log, source="airflow")


async def get_full_airflow_log(
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    try_number: int = -1,
) -> str:
    """Fetch untruncated log from Airflow. Used as agent fallback tool."""
    result = await get_airflow_log(dag_id, dag_run_id, task_id, try_number)
    return result.raw_log
