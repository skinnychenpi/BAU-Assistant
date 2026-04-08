"""
Shared test fixtures for BAU-Assistant.
"""

from datetime import datetime, timedelta

import pytest

from bau.analysis.tools.airflow_log_tool import ProcessedLog
from bau.state.models import Action, DiagnosisResult, FailedTaskReport


@pytest.fixture
def sample_report() -> FailedTaskReport:
    return FailedTaskReport(
        dag_id="data_pltingestion_email_demo_linear_dag",
        dag_run_id="scheduled__2026-02-25T18:00:00+00:00",
        dag_run_url=None,
        scheduled_time=datetime(2026, 2, 25, 18, 0, 0),
        start_date=datetime(2026, 2, 26, 1, 59, 49),
        duration=timedelta(hours=0, minutes=1, seconds=58),
        avg_duration=timedelta(hours=0, minutes=3, seconds=54),
        is_overtime=False,
        failed_tasks=["collect"],
        upstream_failed_tasks=["parse"],
        root_cause_tasks=["collect"],
    )


@pytest.fixture
def sample_log() -> ProcessedLog:
    return ProcessedLog(
        raw_log=(
            "INFO - Starting task collect\n"
            "INFO - Connecting to upstream API\n"
            "ERROR - Connection failed\n"
            "Traceback (most recent call last):\n"
            '  File "/opt/airflow/dags/tasks/collect.py", line 42, in run\n'
            "    response = requests.get(url, timeout=30)\n"
            "ConnectionRefusedError: [Errno 111] Connection refused\n"
        ),
        exception="ConnectionRefusedError: [Errno 111] Connection refused",
        stack_trace=(
            "Traceback (most recent call last):\n"
            '  File "/opt/airflow/dags/tasks/collect.py", line 42, in run\n'
            "    response = requests.get(url, timeout=30)\n"
            "ConnectionRefusedError: [Errno 111] Connection refused"
        ),
        tail_lines=[
            "INFO - Starting task collect",
            "INFO - Connecting to upstream API",
            "ERROR - Connection failed",
            "Traceback (most recent call last):",
            '  File "/opt/airflow/dags/tasks/collect.py", line 42, in run',
            "    response = requests.get(url, timeout=30)",
            "ConnectionRefusedError: [Errno 111] Connection refused",
        ],
        source="airflow",
    )


@pytest.fixture
def sample_diagnosis(sample_report) -> DiagnosisResult:
    return DiagnosisResult(
        dag_id=sample_report.dag_id,
        dag_run_id=sample_report.dag_run_id,
        root_cause_category="network_error",
        confidence=0.85,
        evidence=[
            "ConnectionRefusedError: [Errno 111] Connection refused",
            "Task 'collect' failed at line 42 trying to connect to upstream API",
        ],
        suggested_actions=[
            Action(
                action_id="test-action-001",
                action_type="airflow_rerun",
                target=sample_report.dag_run_id,
                params={},
                reason="Transient network error — rerun should succeed",
            )
        ],
    )


@pytest.fixture
def sample_dag_config() -> dict:
    return {
        "data_pltingestion_email_demo_linear_dag": {
            "confluence_url": "https://confluence.example.com/pages/12345",
            "tasks": {
                "collect": {
                    "type": "spark",
                    "yarn_app_name": "pltingestion_email_collect",
                },
                "parse": {
                    "type": "bash",
                },
                "transform": {
                    "type": "spark",
                    "yarn_app_name": "pltingestion_email_transform",
                },
            },
        },
        "data_demo_bash_dag": {
            "confluence_url": "https://confluence.example.com/pages/67890",
            "tasks": {
                "extract": {
                    "type": "bash",
                },
            },
        },
    }
