"""
Email parser for Airflow BAU alert emails.

Design decision: pure regex, no LLM.
The email format is highly structured and consistent — using an LLM here
would add cost and non-determinism for zero benefit.

Expected email format (one block per DAG run):
    Dag ID: <dag_id>
    Dag Run ID: <run_id>\\t failed \\t Dag Run Link Click Here
    Start date: <date> SGT
    Duration: HH:MM:SS (avg:HH:MM:SS)
    \\t Task ID: <task_id> \\t <status>
    ...
"""

from __future__ import annotations

import re
import uuid
from datetime import datetime, timedelta

from bau.config import settings
from bau.state.models import FailedTaskReport

# ── Regex patterns ─────────────────────────────────────────────────────────────

_DAG_BLOCK_RE = re.compile(
    r"Dag ID:\s*(?P<dag_id>\S+).*?"
    r"Dag Run ID:\s*(?P<run_id>\S+).*?"
    r"Start date:\s*(?P<start_date>.+?SGT).*?"
    r"Duration:\s*(?P<duration>\d{2}:\d{2}:\d{2})\s*\(avg:(?P<avg_duration>\d{2}:\d{2}:\d{2})\)"
    r"(?P<tasks>.*?)(?=Dag ID:|\Z)",
    re.DOTALL,
)

_TASK_RE = re.compile(
    r"Task ID:\s*(?P<task_id>\S+)\s+(?P<status>failed|upstream_failed|success|skipped)"
)

_DATE_FMT = "%Y-%m-%d %H:%M:%S"


def _parse_duration(s: str) -> timedelta:
    h, m, sec = map(int, s.split(":"))
    return timedelta(hours=h, minutes=m, seconds=sec)


def _parse_datetime(s: str) -> datetime:
    # e.g. "2026-02-26 01:59:49 SGT" — strip timezone label
    clean = s.strip().replace(" SGT", "").strip()
    return datetime.strptime(clean, _DATE_FMT)


def _infer_scheduled_time(run_id: str) -> datetime:
    """
    Extract scheduled time from run_id like:
      scheduled__2026-02-25T18:00:00+00:00
    Falls back to epoch if format is unexpected.
    """
    m = re.search(r"scheduled__(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})", run_id)
    if m:
        return datetime.fromisoformat(m.group(1))
    return datetime.utcfromtimestamp(0)


def parse_email_body(body: str) -> list[FailedTaskReport]:
    """
    Parse raw email body text into a list of FailedTaskReport objects.
    Only returns entries that have at least one failed (non-upstream) task.
    """
    reports: list[FailedTaskReport] = []

    for match in _DAG_BLOCK_RE.finditer(body):
        dag_id = match.group("dag_id").strip()
        run_id = match.group("run_id").strip()
        start_date = _parse_datetime(match.group("start_date"))
        duration = _parse_duration(match.group("duration"))
        avg_duration = _parse_duration(match.group("avg_duration"))
        tasks_block = match.group("tasks")

        failed_tasks: list[str] = []
        upstream_failed_tasks: list[str] = []

        for task_match in _TASK_RE.finditer(tasks_block):
            task_id = task_match.group("task_id")
            status = task_match.group("status")
            if status == "failed":
                failed_tasks.append(task_id)
            elif status == "upstream_failed":
                upstream_failed_tasks.append(task_id)

        # Skip if no real failures (e.g. only upstream_failed)
        if not failed_tasks:
            continue

        # Root cause = tasks that failed but are NOT listed as upstream_failed
        # (in practice, root_cause_tasks == failed_tasks for most DAGs)
        root_cause_tasks = [t for t in failed_tasks if t not in upstream_failed_tasks]
        if not root_cause_tasks:
            root_cause_tasks = failed_tasks  # fallback: treat all failed as root cause

        is_overtime = (
            avg_duration.total_seconds() > 0
            and duration > avg_duration * settings.overtime_threshold
        )

        reports.append(
            FailedTaskReport(
                dag_id=dag_id,
                dag_run_id=run_id,
                scheduled_time=_infer_scheduled_time(run_id),
                start_date=start_date,
                duration=duration,
                avg_duration=avg_duration,
                is_overtime=is_overtime,
                failed_tasks=failed_tasks,
                upstream_failed_tasks=upstream_failed_tasks,
                root_cause_tasks=root_cause_tasks,
            )
        )

    return reports
