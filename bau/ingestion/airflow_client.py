"""
Airflow API ingestion client — replaces Gmail + email parsing.

Queries the Airflow REST API (v2) directly to discover failed and running
DAG runs, then produces the same IngestionResult that the email parser did.

Endpoints used:
  GET api/v2/dags/{dag_id}                          — check DAG exists
  GET api/v2/dags/{dag_id}/dagRuns?logical_date_*   — list runs in window
  GET api/v2/dags/{dag_id}/dagRuns/{id}/taskInstances — task states
  GET api/v2/dags/{dag_id}/dagRuns?state=success     — historical avg
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path

import httpx

from bau.config import settings
from bau.ingestion.airflow_auth import api_get, get_token, make_auth_headers
from bau.state.models import (
    FailedTaskReport,
    IngestionResult,
    RunningDAGReport,
    SchedulerNotFoundDAG,
)

logger = logging.getLogger(__name__)

SGT = timezone(timedelta(hours=8))


def load_dag_list(path: str | None = None) -> list[str]:
    """Load DAG IDs from a text file (one per line, blank lines ignored)."""
    path = path or settings.dag_list_path
    if not path:
        raise ValueError("DAG_LIST_PATH not configured — set it in .env or pass a path")
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"DAG list file not found: {p}")
    return [line.strip() for line in p.read_text().splitlines() if line.strip()]


async def _check_dag_exists(
    client: httpx.AsyncClient, headers: dict, dag_id: str
) -> bool:
    """Check whether a DAG ID exists in the Airflow scheduler."""
    data = await api_get(client, headers, f"api/v2/dags/{dag_id}")
    return data is not None


async def _get_dag_runs(
    client: httpx.AsyncClient,
    headers: dict,
    dag_id: str,
    date_gte: str,
    date_lte: str,
) -> list[dict]:
    """Get DAG runs within a logical date window."""
    endpoint = (
        f"api/v2/dags/{dag_id}/dagRuns"
        f"?logical_date_gte={date_gte}&logical_date_lte={date_lte}"
    )
    data = await api_get(client, headers, endpoint)
    if not data:
        return []
    return data.get("dag_runs", [])


async def _get_task_instances(
    client: httpx.AsyncClient,
    headers: dict,
    dag_id: str,
    dag_run_id: str,
) -> list[dict]:
    """Get all task instances for a DAG run."""
    endpoint = f"api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances?limit=500"
    data = await api_get(client, headers, endpoint)
    if not data:
        return []
    return data.get("task_instances", [])


async def _calculate_historical_average(
    client: httpx.AsyncClient,
    headers: dict,
    dag_id: str,
    limit: int = 30,
) -> timedelta | None:
    """Calculate average duration from recent successful DAG runs."""
    endpoint = (
        f"api/v2/dags/{dag_id}/dagRuns"
        f"?order_by=-end_date&state=success&limit={limit}"
    )
    data = await api_get(client, headers, endpoint)
    if not data:
        return None

    dag_runs = data.get("dag_runs", [])
    durations = [r["duration"] for r in dag_runs if r.get("duration") and r["duration"] > 0]
    if not durations:
        return None

    avg_seconds = sum(durations) / len(durations)
    return timedelta(seconds=avg_seconds)


def _compute_date_window(grass_date: str) -> tuple[str, str]:
    """Compute the UTC date window for a given grass_date (YYYY-MM-DD).

    Window: previous day 14:00 UTC to next day 14:00 UTC.
    This covers SGT midnight-to-midnight.
    """
    dt = datetime.strptime(grass_date, "%Y-%m-%d")
    prev_day = dt - timedelta(days=1)
    next_day = dt + timedelta(days=1)
    return (
        prev_day.strftime("%Y-%m-%dT14:00:00Z"),
        next_day.strftime("%Y-%m-%dT14:00:00Z"),
    )


def _build_dag_run_url(dag_id: str, dag_run_id: str) -> str:
    """Build the Airflow UI URL for a DAG run."""
    base = settings.airflow_base_url.rstrip("/")
    return f"{base}/dags/{dag_id}/runs/{dag_run_id}"


def _infer_scheduled_time(run_id: str) -> datetime:
    """Extract scheduled time from run_id like scheduled__2026-02-25T18:00:00+00:00."""
    import re
    m = re.search(r"scheduled__(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})", run_id)
    if m:
        return datetime.fromisoformat(m.group(1))
    return datetime.utcfromtimestamp(0)


def _parse_airflow_datetime(date_str: str | None) -> datetime:
    """Parse an Airflow UTC datetime string into a datetime object."""
    if not date_str:
        return datetime.utcfromtimestamp(0)
    clean = date_str.replace("Z", "+00:00")
    return datetime.fromisoformat(clean)


async def fetch_dag_status(
    dag_ids: list[str] | None = None,
    grass_date: str | None = None,
) -> IngestionResult:
    """
    Query Airflow API for current DAG run statuses.

    Args:
        dag_ids: List of DAG IDs to check. If None, loads from DAG_LIST_PATH.
        grass_date: Date to check (YYYY-MM-DD). Defaults to today (SGT).

    Returns:
        IngestionResult with not_found, failed, and running reports.
    """
    if dag_ids is None:
        dag_ids = load_dag_list()

    if grass_date is None:
        grass_date = datetime.now(SGT).strftime("%Y-%m-%d")

    date_gte, date_lte = _compute_date_window(grass_date)

    not_found_dags: list[SchedulerNotFoundDAG] = []
    failed_reports: list[FailedTaskReport] = []
    running_reports: list[RunningDAGReport] = []

    async with httpx.AsyncClient(verify=False) as client:
        token = await get_token(client)
        headers = make_auth_headers(token)

        for dag_id in dag_ids:
            # Check if DAG exists
            if not await _check_dag_exists(client, headers, dag_id):
                not_found_dags.append(SchedulerNotFoundDAG(dag_id=dag_id))
                logger.info(f"DAG not found in scheduler: {dag_id}")
                continue

            # Get DAG runs in the date window
            dag_runs = await _get_dag_runs(client, headers, dag_id, date_gte, date_lte)

            for run in dag_runs:
                dag_run_id = run.get("dag_run_id", "")
                state = run.get("state", "")
                start_date_str = run.get("start_date")
                duration_seconds = run.get("duration") or 0

                # Get task instances
                task_instances = await _get_task_instances(
                    client, headers, dag_id, dag_run_id
                )

                # Only report if at least one task is not successful
                non_success = [t for t in task_instances if t.get("state") != "success"]
                if not non_success:
                    continue

                # Get historical average
                avg_duration = await _calculate_historical_average(
                    client, headers, dag_id
                )

                start_date = _parse_airflow_datetime(start_date_str)
                scheduled_time = _infer_scheduled_time(dag_run_id)
                dag_run_url = _build_dag_run_url(dag_id, dag_run_id)

                if state == "running":
                    # For running DAGs, compute current duration
                    elapsed = datetime.now(timezone.utc) - start_date.replace(
                        tzinfo=timezone.utc
                    ) if start_date.tzinfo is None else datetime.now(timezone.utc) - start_date
                    duration = max(timedelta(0), elapsed)

                    running_tasks = [
                        t["task_id"] for t in non_success if t.get("state") == "running"
                    ]
                    pending_tasks = [
                        t["task_id"] for t in non_success
                        if t.get("state") in (None, "None", "none", "scheduled", "queued")
                    ]

                    if avg_duration is not None:
                        is_overtime: bool | None = duration > avg_duration * settings.overtime_threshold
                    else:
                        is_overtime = None

                    running_reports.append(
                        RunningDAGReport(
                            dag_id=dag_id,
                            dag_run_id=dag_run_id,
                            dag_run_url=dag_run_url,
                            scheduled_time=scheduled_time,
                            start_date=start_date,
                            duration=duration,
                            avg_duration=avg_duration,
                            is_overtime=is_overtime,
                            running_tasks=running_tasks,
                            pending_tasks=pending_tasks,
                        )
                    )

                # Check for failed tasks regardless of DAG run state.
                # A DAG run can be "success" overall but still contain
                # individual tasks that failed (e.g. non-blocking branches).
                failed_tasks = [
                    t["task_id"] for t in task_instances if t.get("state") == "failed"
                ]
                upstream_failed = [
                    t["task_id"] for t in task_instances
                    if t.get("state") == "upstream_failed"
                ]

                if failed_tasks and state != "running":
                    duration = timedelta(seconds=duration_seconds)

                    root_cause_tasks = [
                        t for t in failed_tasks if t not in upstream_failed
                    ]
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
                            dag_run_id=dag_run_id,
                            dag_run_url=dag_run_url,
                            scheduled_time=scheduled_time,
                            start_date=start_date,
                            duration=duration,
                            avg_duration=avg_duration,
                            is_overtime=is_overtime_flag,
                            failed_tasks=failed_tasks,
                            upstream_failed_tasks=upstream_failed,
                            root_cause_tasks=root_cause_tasks,
                        )
                    )

    logger.info(
        f"Ingestion complete: {len(not_found_dags)} not found, "
        f"{len(failed_reports)} failed, {len(running_reports)} running"
    )

    return IngestionResult(
        not_found_dags=not_found_dags,
        failed_reports=failed_reports,
        running_reports=running_reports,
    )
