"""
Tests for history_tool.py — formatting historical issues for the agent prompt.
"""

import pytest

from bau.analysis.tools.history_tool import format_issues_for_prompt
from bau.ingestion.gsheet_client import HistoricalIssue


class TestFormatIssuesForPrompt:
    def test_empty_issues(self):
        result = format_issues_for_prompt([])
        assert "No historical issues" in result

    def test_single_issue(self):
        issues = [
            HistoricalIssue(
                issue_date="2026-01-15",
                description="DAG test_dag failed with network timeout",
                logs="ConnectionError: timeout",
                fix_action="Reran the task after upstream service recovered",
            )
        ]
        result = format_issues_for_prompt(issues)
        assert "1 historical issue" in result
        assert "2026-01-15" in result
        assert "network timeout" in result
        assert "ConnectionError" in result
        assert "Reran the task" in result

    def test_multiple_issues(self):
        issues = [
            HistoricalIssue("2026-01-15", "Error A", "log A", "Fix A"),
            HistoricalIssue("2026-01-20", "Error B", "log B", "Fix B"),
            HistoricalIssue("2026-02-01", "Error C", "log C", "Fix C"),
        ]
        result = format_issues_for_prompt(issues)
        assert "3 historical issue" in result
        assert "Issue #1" in result
        assert "Issue #2" in result
        assert "Issue #3" in result

    def test_truncates_long_logs(self):
        issues = [
            HistoricalIssue(
                issue_date="2026-01-15",
                description="test",
                logs="x" * 1000,  # very long log
                fix_action="fix",
            )
        ]
        result = format_issues_for_prompt(issues)
        assert "..." in result  # should be truncated

    def test_handles_empty_logs(self):
        issues = [
            HistoricalIssue(
                issue_date="2026-01-15",
                description="test",
                logs="",
                fix_action="fix",
            )
        ]
        result = format_issues_for_prompt(issues)
        assert "Logs:" not in result  # empty logs should be skipped
