"""
Email parser for Airflow BAU alert emails.

Design decision: pure regex, no LLM.
The email format is highly structured and consistent — using an LLM here
would add cost and non-determinism for zero benefit.

Expected email format:
    --- DAG IDs Not Found in Scheduler ---
    DAG ID: <dag_id> does not exist in the scheduler.

    Dag ID: <dag_id>
    Dag Run ID: <run_id>\\t <status> \\t <a href="url">Dag Run Link Click Here</a>
    Start date: <date> SGT
    Duration: HH:MM:SS (avg:HH:MM:SS|N/A)
    \\t Task ID: <task_id> \\t <status>
    ...
"""

from __future__ import annotations

import re
from datetime import datetime, timedelta

from bau.config import settings
from bau.state.models import (
    EmailParseResult,
    FailedTaskReport,
    RunningDAGReport,
    SchedulerNotFoundDAG,
)

# ── Regex patterns ─────────────────────────────────────────────────────────────

_NOT_FOUND_RE = re.compile(
    r"DAG ID:\s*(?P<dag_id>\S+)\s+does not exist in the scheduler"
)

_DAG_BLOCK_RE = re.compile(
    r"Dag ID:\s*(?P<dag_id>\S+).*?"
    r"Dag Run ID:\s*(?P<run_id>\S+)\s+(?P<dag_status>\S+)\s+"
    r"(?:<a\s+href=[\"'](?P<url>[^\"']+)[\"'][^>]*>)?"
    r".*?"
    r"Start date:\s*(?P<start_date>.+?SGT).*?"
    r"Duration:\s*(?P<duration>\d{2}:\d{2}:\d{2})\s*\(avg:(?P<avg_duration>\d{2}:\d{2}:\d{2}|N/A)\)"
    r"(?P<tasks>.*?)(?=Dag ID:|\Z)",
    re.DOTALL,
)

_TASK_RE = re.compile(
    r"Task ID:\s*(?P<task_id>\S+)\s+(?P<status>failed|upstream_failed|success|skipped|running|None)"
)

_DATE_FMT = "%Y-%m-%d %H:%M:%S"


def _parse_duration(s: str) -> timedelta | None:
    if s == "N/A":
        return None
    h, m, sec = map(int, s.split(":"))
    return timedelta(hours=h, minutes=m, seconds=sec)


def _parse_datetime(s: str) -> datetime:
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

    Legacy API — returns only failed reports for backward compatibility.
    Use parse_email_full() for the complete parse result.
    """
    result = parse_email_full(body)
    return result.failed_reports


def parse_email_full(body: str) -> EmailParseResult:
    """
    Parse raw email body into a full EmailParseResult containing:
    - DAGs not found in scheduler
    - Failed DAG run reports
    - Running DAG run reports
    """
    not_found_dags = [
        SchedulerNotFoundDAG(dag_id=m.group("dag_id"))
        for m in _NOT_FOUND_RE.finditer(body)
    ]

    failed_reports: list[FailedTaskReport] = []
    running_reports: list[RunningDAGReport] = []

    for match in _DAG_BLOCK_RE.finditer(body):
        dag_id = match.group("dag_id").strip()
        run_id = match.group("run_id").strip()
        dag_status = match.group("dag_status").strip()
        dag_run_url = match.group("url")
        start_date = _parse_datetime(match.group("start_date"))
        duration = _parse_duration(match.group("duration"))
        avg_duration = _parse_duration(match.group("avg_duration"))
        tasks_block = match.group("tasks")

        # duration is always present (not N/A), so it's always a timedelta
        assert isinstance(duration, timedelta)

        if dag_status == "running":
            running_tasks: list[str] = []
            pending_tasks: list[str] = []
            for task_match in _TASK_RE.finditer(tasks_block):
                task_id = task_match.group("task_id")
                status = task_match.group("status")
                if status == "running":
                    running_tasks.append(task_id)
                elif status == "None":
                    pending_tasks.append(task_id)

            if avg_duration is not None:
                is_overtime: bool | None = duration > avg_duration * settings.overtime_threshold
            else:
                is_overtime = None

            running_reports.append(
                RunningDAGReport(
                    dag_id=dag_id,
                    dag_run_id=run_id,
                    dag_run_url=dag_run_url,
                    scheduled_time=_infer_scheduled_time(run_id),
                    start_date=start_date,
                    duration=duration,
                    avg_duration=avg_duration,
                    is_overtime=is_overtime,
                    running_tasks=running_tasks,
                    pending_tasks=pending_tasks,
                )
            )
        else:
            # failed / success / other — apply existing logic
            failed_tasks: list[str] = []
            upstream_failed_tasks: list[str] = []

            for task_match in _TASK_RE.finditer(tasks_block):
                task_id = task_match.group("task_id")
                status = task_match.group("status")
                if status == "failed":
                    failed_tasks.append(task_id)
                elif status == "upstream_failed":
                    upstream_failed_tasks.append(task_id)

            if not failed_tasks:
                continue

            root_cause_tasks = [t for t in failed_tasks if t not in upstream_failed_tasks]
            if not root_cause_tasks:
                root_cause_tasks = failed_tasks

            if avg_duration is not None:
                is_overtime_flag = (
                    avg_duration.total_seconds() > 0
                    and duration > avg_duration * settings.overtime_threshold
                )
            else:
                is_overtime_flag = False

            failed_reports.append(
                FailedTaskReport(
                    dag_id=dag_id,
                    dag_run_id=run_id,
                    dag_run_url=dag_run_url,
                    scheduled_time=_infer_scheduled_time(run_id),
                    start_date=start_date,
                    duration=duration,
                    avg_duration=avg_duration,
                    is_overtime=is_overtime_flag,
                    failed_tasks=failed_tasks,
                    upstream_failed_tasks=upstream_failed_tasks,
                    root_cause_tasks=root_cause_tasks,
                )
            )

    return EmailParseResult(
        not_found_dags=not_found_dags,
        failed_reports=failed_reports,
        running_reports=running_reports,
    )
