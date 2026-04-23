"""
Tests for Airflow API ingestion client.
These tests mock the Airflow API to test the client logic without network calls.
"""

from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from bau.ingestion.airflow_client import (
    _build_dag_run_url,
    _compute_date_window,
    _infer_scheduled_time,
    _parse_airflow_datetime,
    fetch_dag_status,
    load_dag_list,
)


# ── Unit tests for helper functions ──────────────────────────────────────────


def test_compute_date_window():
    gte, lte = _compute_date_window("2026-04-01")
    assert gte == "2026-03-31T14:00:00Z"
    assert lte == "2026-04-02T14:00:00Z"


def test_build_dag_run_url():
    with patch("bau.ingestion.airflow_client.settings") as mock_settings:
        mock_settings.airflow_base_url = "https://airflow.example.com"
        url = _build_dag_run_url("my_dag", "scheduled__2026-01-01T00:00:00+00:00")
        assert url == "https://airflow.example.com/dags/my_dag/runs/scheduled__2026-01-01T00:00:00+00:00"


def test_infer_scheduled_time():
    dt = _infer_scheduled_time("scheduled__2026-03-31T16:16:00+00:00")
    assert dt.year == 2026
    assert dt.month == 3
    assert dt.day == 31
    assert dt.hour == 16
    assert dt.minute == 16


def test_infer_scheduled_time_fallback():
    dt = _infer_scheduled_time("manual__xyz")
    assert dt.year == 1970


def test_parse_airflow_datetime_utc():
    dt = _parse_airflow_datetime("2026-04-01T03:15:00.123456Z")
    assert dt.year == 2026
    assert dt.month == 4
    assert dt.hour == 3


def test_parse_airflow_datetime_none():
    dt = _parse_airflow_datetime(None)
    assert dt.year == 1970


def test_load_dag_list(tmp_path):
    dag_file = tmp_path / "dags.txt"
    dag_file.write_text("dag_one\ndag_two\n\ndag_three\n")
    result = load_dag_list(str(dag_file))
    assert result == ["dag_one", "dag_two", "dag_three"]


def test_load_dag_list_missing():
    with pytest.raises(FileNotFoundError):
        load_dag_list("/nonexistent/path.txt")


def test_load_dag_list_no_path():
    with patch("bau.ingestion.airflow_client.settings") as mock_settings:
        mock_settings.dag_list_path = ""
        with pytest.raises(ValueError):
            load_dag_list()


# ── Integration test with mocked API ─────────────────────────────────────────


@pytest.mark.asyncio
async def test_fetch_dag_status_failed_run():
    """Test that a failed DAG run is correctly turned into a FailedTaskReport."""

    async def mock_api_get(client, headers, endpoint):
        if endpoint == "api/v2/dags/test_dag":
            return {"dag_id": "test_dag"}
        if "logical_date" in endpoint:
            return {
                "dag_runs": [
                    {
                        "dag_id": "test_dag",
                        "dag_run_id": "scheduled__2026-04-01T00:00:00+00:00",
                        "state": "failed",
                        "start_date": "2026-04-01T08:00:00Z",
                        "duration": 120,
                    },
                ]
            }
        if "taskInstances" in endpoint:
            return {
                "task_instances": [
                    {"task_id": "collect", "state": "failed"},
                    {"task_id": "parse", "state": "upstream_failed"},
                ]
            }
        if "order_by=-end_date" in endpoint:
            return {"dag_runs": []}
        return None

    with patch("bau.ingestion.airflow_client.get_token", new_callable=AsyncMock, return_value="fake-token"):
        with patch("bau.ingestion.airflow_client.api_get", side_effect=mock_api_get):
            result = await fetch_dag_status(
                dag_ids=["test_dag"],
                grass_date="2026-04-01",
            )

    assert len(result.not_found_dags) == 0
    assert len(result.failed_reports) == 1
    assert len(result.running_reports) == 0

    report = result.failed_reports[0]
    assert report.dag_id == "test_dag"
    assert report.failed_tasks == ["collect"]
    assert report.upstream_failed_tasks == ["parse"]
    assert report.root_cause_tasks == ["collect"]
    assert report.avg_duration is None
    assert report.is_overtime is False


@pytest.mark.asyncio
async def test_fetch_dag_status_not_found():
    """Test that a missing DAG is reported as SchedulerNotFoundDAG."""

    async def mock_api_get(client, headers, endpoint):
        return None  # 404 for everything

    with patch("bau.ingestion.airflow_client.get_token", new_callable=AsyncMock, return_value="fake-token"):
        with patch("bau.ingestion.airflow_client.api_get", side_effect=mock_api_get):
            result = await fetch_dag_status(
                dag_ids=["missing_dag"],
                grass_date="2026-04-01",
            )

    assert len(result.not_found_dags) == 1
    assert result.not_found_dags[0].dag_id == "missing_dag"
    assert len(result.failed_reports) == 0
    assert len(result.running_reports) == 0


@pytest.mark.asyncio
async def test_fetch_dag_status_running_with_avg():
    """Test a running DAG with historical average is checked for overtime."""

    async def mock_api_get(client, headers, endpoint):
        if endpoint == "api/v2/dags/run_dag":
            return {"dag_id": "run_dag"}
        if "logical_date" in endpoint:
            return {
                "dag_runs": [
                    {
                        "dag_id": "run_dag",
                        "dag_run_id": "scheduled__2026-04-01T00:00:00+00:00",
                        "state": "running",
                        "start_date": "2026-04-01T08:00:00Z",
                        "duration": None,
                    },
                ]
            }
        if "taskInstances" in endpoint:
            return {
                "task_instances": [
                    {"task_id": "etl", "state": "running"},
                    {"task_id": "notify", "state": None},
                ]
            }
        if "order_by=-end_date" in endpoint:
            return {
                "dag_runs": [
                    {"duration": 600},
                    {"duration": 660},
                ]
            }
        return None

    with patch("bau.ingestion.airflow_client.get_token", new_callable=AsyncMock, return_value="fake-token"):
        with patch("bau.ingestion.airflow_client.api_get", side_effect=mock_api_get):
            result = await fetch_dag_status(
                dag_ids=["run_dag"],
                grass_date="2026-04-01",
            )

    assert len(result.running_reports) == 1
    report = result.running_reports[0]
    assert report.dag_id == "run_dag"
    assert "etl" in report.running_tasks
    assert "notify" in report.pending_tasks
    assert report.avg_duration is not None


@pytest.mark.asyncio
async def test_fetch_dag_status_all_success_skipped():
    """Test that DAG runs where all tasks succeeded are not reported."""

    async def mock_api_get(client, headers, endpoint):
        if endpoint == "api/v2/dags/ok_dag":
            return {"dag_id": "ok_dag"}
        if "logical_date" in endpoint:
            return {
                "dag_runs": [
                    {
                        "dag_id": "ok_dag",
                        "dag_run_id": "scheduled__2026-04-01T00:00:00+00:00",
                        "state": "success",
                        "start_date": "2026-04-01T08:00:00Z",
                        "duration": 60,
                    },
                ]
            }
        if "taskInstances" in endpoint:
            return {
                "task_instances": [
                    {"task_id": "step1", "state": "success"},
                    {"task_id": "step2", "state": "success"},
                ]
            }
        return None

    with patch("bau.ingestion.airflow_client.get_token", new_callable=AsyncMock, return_value="fake-token"):
        with patch("bau.ingestion.airflow_client.api_get", side_effect=mock_api_get):
            result = await fetch_dag_status(
                dag_ids=["ok_dag"],
                grass_date="2026-04-01",
            )

    assert len(result.not_found_dags) == 0
    assert len(result.failed_reports) == 0
    assert len(result.running_reports) == 0


@pytest.mark.asyncio
async def test_fetch_dag_status_success_run_with_failed_task():
    """Test that a DAG run with state=success but a failed task is still reported."""

    async def mock_api_get(client, headers, endpoint):
        if endpoint == "api/v2/dags/mixed_dag":
            return {"dag_id": "mixed_dag"}
        if "logical_date" in endpoint:
            return {
                "dag_runs": [
                    {
                        "dag_id": "mixed_dag",
                        "dag_run_id": "scheduled__2026-04-01T00:00:00+00:00",
                        "state": "success",
                        "start_date": "2026-04-01T08:00:00Z",
                        "duration": 300,
                    },
                ]
            }
        if "taskInstances" in endpoint:
            return {
                "task_instances": [
                    {"task_id": "step1", "state": "success"},
                    {"task_id": "step2", "state": "failed"},
                    {"task_id": "step3", "state": "success"},
                ]
            }
        if "order_by=-end_date" in endpoint:
            return {"dag_runs": []}
        return None

    with patch("bau.ingestion.airflow_client.get_token", new_callable=AsyncMock, return_value="fake-token"):
        with patch("bau.ingestion.airflow_client.api_get", side_effect=mock_api_get):
            result = await fetch_dag_status(
                dag_ids=["mixed_dag"],
                grass_date="2026-04-01",
            )

    assert len(result.failed_reports) == 1
    report = result.failed_reports[0]
    assert report.dag_id == "mixed_dag"
    assert report.failed_tasks == ["step2"]
    assert report.root_cause_tasks == ["step2"]
