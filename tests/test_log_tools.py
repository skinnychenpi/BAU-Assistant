"""
Tests for log tools — airflow_log_tool, yarn_log_tool, log_router.

Tests focus on log processing logic and routing decisions.
API calls are mocked since we don't have real Airflow/YARN access.
"""

import pytest

from bau.analysis.tools.airflow_log_tool import (
    LOG_TAIL_LINES,
    _extract_exception,
    _extract_stack_trace,
    process_log,
)
from bau.analysis.tools.log_router import get_task_log


# ── Log processing tests ─────────────────────────────────────────────────────


class TestExtractException:
    def test_extracts_python_exception(self):
        log = "some output\nConnectionRefusedError: connection refused\nmore output"
        assert _extract_exception(log) == "ConnectionRefusedError: connection refused"

    def test_extracts_last_exception(self):
        log = (
            "ValueError: bad value\n"
            "INFO - retrying\n"
            "TimeoutError: timed out\n"
        )
        assert _extract_exception(log) == "TimeoutError: timed out"

    def test_returns_none_when_no_exception(self):
        log = "INFO - all good\nINFO - done"
        assert _extract_exception(log) is None

    def test_handles_dotted_exception_names(self):
        log = "requests.exceptions.ConnectionError: failed"
        assert _extract_exception(log) == "requests.exceptions.ConnectionError: failed"


class TestExtractStackTrace:
    def test_extracts_traceback(self):
        log = (
            "INFO - starting\n"
            "Traceback (most recent call last):\n"
            '  File "foo.py", line 10, in bar\n'
            "    x = 1/0\n"
            "ZeroDivisionError: division by zero\n"
        )
        trace = _extract_stack_trace(log)
        assert trace is not None
        assert "Traceback" in trace
        assert "ZeroDivisionError" in trace

    def test_returns_none_when_no_traceback(self):
        log = "INFO - all good\nINFO - done"
        assert _extract_stack_trace(log) is None

    def test_extracts_last_traceback(self):
        log = (
            "Traceback (most recent call last):\n"
            '  File "a.py", line 1\n'
            "ValueError: first\n"
            "INFO - retry\n"
            "Traceback (most recent call last):\n"
            '  File "b.py", line 2\n'
            "TypeError: second\n"
        )
        trace = _extract_stack_trace(log)
        assert trace is not None
        assert "TypeError: second" in trace


class TestProcessLog:
    def test_basic_processing(self):
        log = "line1\nline2\nValueError: bad\nline4"
        result = process_log(log)
        assert result.source == "airflow"
        assert result.exception == "ValueError: bad"
        assert len(result.tail_lines) == 4

    def test_tail_lines_truncation(self):
        lines = [f"line {i}" for i in range(300)]
        log = "\n".join(lines)
        result = process_log(log)
        assert len(result.tail_lines) == LOG_TAIL_LINES
        assert result.tail_lines[-1] == "line 299"

    def test_empty_log(self):
        result = process_log("")
        assert result.exception is None
        assert result.stack_trace is None
        assert result.tail_lines == []

    def test_custom_source(self):
        result = process_log("test", source="yarn")
        assert result.source == "yarn"


# ── Log router tests ──────────────────────────────────────────────────────────


class TestLogRouter:
    @pytest.mark.asyncio
    async def test_routes_bash_to_airflow(self, sample_dag_config, monkeypatch):
        """Bash tasks should route to Airflow log tool."""
        called_with = {}

        async def mock_airflow_log(dag_id, dag_run_id, task_id):
            called_with.update(dag_id=dag_id, task_id=task_id)
            return process_log("mock airflow log")

        monkeypatch.setattr(
            "bau.analysis.tools.log_router.get_airflow_log",
            mock_airflow_log,
        )

        result = await get_task_log(
            "data_pltingestion_email_demo_linear_dag",
            "run-1",
            "parse",
            config=sample_dag_config,
        )
        assert result.source == "airflow"
        assert called_with["task_id"] == "parse"

    @pytest.mark.asyncio
    async def test_routes_spark_to_yarn(self, sample_dag_config, monkeypatch):
        """Spark tasks should route to YARN log tool."""
        called_with = {}

        async def mock_yarn_log(yarn_app_name):
            called_with["yarn_app_name"] = yarn_app_name
            return process_log("mock yarn log", source="yarn")

        monkeypatch.setattr(
            "bau.analysis.tools.log_router.get_yarn_log",
            mock_yarn_log,
        )

        result = await get_task_log(
            "data_pltingestion_email_demo_linear_dag",
            "run-1",
            "collect",
            config=sample_dag_config,
        )
        assert result.source == "yarn"
        assert called_with["yarn_app_name"] == "pltingestion_email_collect"

    @pytest.mark.asyncio
    async def test_unknown_task_defaults_to_airflow(self, sample_dag_config, monkeypatch):
        """Tasks not in config should default to Airflow."""
        async def mock_airflow_log(dag_id, dag_run_id, task_id):
            return process_log("fallback log")

        monkeypatch.setattr(
            "bau.analysis.tools.log_router.get_airflow_log",
            mock_airflow_log,
        )

        result = await get_task_log(
            "data_pltingestion_email_demo_linear_dag",
            "run-1",
            "unknown_task",
            config=sample_dag_config,
        )
        assert result.source == "airflow"

    @pytest.mark.asyncio
    async def test_spark_task_without_yarn_app_name(self, monkeypatch):
        """Spark task missing yarn_app_name should return error."""
        bad_config = {
            "test_dag": {
                "tasks": {
                    "spark_task": {"type": "spark"},  # no yarn_app_name
                },
            },
        }

        result = await get_task_log("test_dag", "run-1", "spark_task", config=bad_config)
        assert result.exception is not None
        assert "No yarn_app_name" in result.exception

    @pytest.mark.asyncio
    async def test_unknown_dag_defaults_to_airflow(self, monkeypatch):
        """DAG not in config should default to Airflow."""
        async def mock_airflow_log(dag_id, dag_run_id, task_id):
            return process_log("default log")

        monkeypatch.setattr(
            "bau.analysis.tools.log_router.get_airflow_log",
            mock_airflow_log,
        )

        result = await get_task_log(
            "nonexistent_dag", "run-1", "task-1", config={}
        )
        assert result.source == "airflow"
