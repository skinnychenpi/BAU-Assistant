"""
Tests for prompts.py — prompt building and tool definitions.
"""

import pytest

from bau.analysis.prompts import (
    SYSTEM_PROMPT,
    TOOL_DEFINITIONS,
    build_initial_user_message,
)


class TestSystemPrompt:
    def test_contains_diagnosis_categories(self):
        for cat in ["data_malformed", "network_error", "code_bug", "permission_error", "unknown"]:
            assert cat in SYSTEM_PROMPT

    def test_contains_action_types(self):
        for action in ["airflow_rerun", "send_message", "manual_only"]:
            assert action in SYSTEM_PROMPT

    def test_contains_json_output_format(self):
        assert "root_cause_category" in SYSTEM_PROMPT
        assert "confidence" in SYSTEM_PROMPT
        assert "evidence" in SYSTEM_PROMPT


class TestToolDefinitions:
    def test_has_three_tools(self):
        assert len(TOOL_DEFINITIONS) == 3

    def test_tool_names(self):
        names = {t["name"] for t in TOOL_DEFINITIONS}
        assert names == {"get_source_code", "get_job_runbook", "get_full_log"}

    def test_tools_have_required_fields(self):
        for tool in TOOL_DEFINITIONS:
            assert "name" in tool
            assert "description" in tool
            assert "input_schema" in tool
            assert tool["input_schema"]["type"] == "object"


class TestBuildInitialUserMessage:
    def test_includes_report_fields(self, sample_report, sample_log):
        msg = build_initial_user_message(sample_report, sample_log, "No history.")
        assert sample_report.dag_id in msg
        assert sample_report.dag_run_id in msg
        assert "collect" in msg

    def test_includes_log_context(self, sample_report, sample_log):
        msg = build_initial_user_message(sample_report, sample_log, "No history.")
        assert "ConnectionRefusedError" in msg
        assert "Traceback" in msg

    def test_includes_historical_issues(self, sample_report, sample_log):
        history = "Issue #1: Network timeout on 2026-01-15, fixed by rerun."
        msg = build_initial_user_message(sample_report, sample_log, history)
        assert "Network timeout" in msg

    def test_handles_empty_log(self, sample_report):
        from bau.analysis.tools.airflow_log_tool import ProcessedLog

        empty_log = ProcessedLog(raw_log="", source="airflow")
        msg = build_initial_user_message(sample_report, empty_log, "No history.")
        assert "No log content available" in msg

    def test_includes_overtime_info(self, sample_report, sample_log):
        msg = build_initial_user_message(sample_report, sample_log, "No history.")
        assert "Overtime" in msg
