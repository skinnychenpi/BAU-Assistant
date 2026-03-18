"""
Tests for analysis/agent.py — diagnosis parsing and tool dispatch.

The actual LLM call is mocked. We test:
1. JSON parsing of diagnosis responses
2. Tool dispatch routing
3. Edge cases (max steps, malformed JSON)
"""

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from bau.analysis.agent import _dispatch_tool, _parse_diagnosis, diagnose


class TestParseDiagnosis:
    def test_parses_valid_json(self, sample_report):
        text = json.dumps({
            "root_cause_category": "network_error",
            "confidence": 0.9,
            "evidence": ["Connection refused on port 8080"],
            "suggested_actions": [
                {
                    "action_type": "airflow_rerun",
                    "target": "dag_run_1",
                    "params": {},
                    "reason": "Transient error",
                }
            ],
        })
        result = _parse_diagnosis(text, sample_report)
        assert result.root_cause_category == "network_error"
        assert result.confidence == 0.9
        assert len(result.evidence) == 1
        assert len(result.suggested_actions) == 1
        assert result.suggested_actions[0].action_type == "airflow_rerun"

    def test_parses_json_in_code_block(self, sample_report):
        text = '```json\n{"root_cause_category": "code_bug", "confidence": 0.7, "evidence": ["bug"], "suggested_actions": []}\n```'
        result = _parse_diagnosis(text, sample_report)
        assert result.root_cause_category == "code_bug"

    def test_parses_json_in_plain_code_block(self, sample_report):
        text = '```\n{"root_cause_category": "unknown", "confidence": 0.3, "evidence": [], "suggested_actions": []}\n```'
        result = _parse_diagnosis(text, sample_report)
        assert result.root_cause_category == "unknown"

    def test_assigns_action_ids(self, sample_report):
        text = json.dumps({
            "root_cause_category": "network_error",
            "confidence": 0.5,
            "evidence": [],
            "suggested_actions": [
                {"action_type": "airflow_rerun", "reason": "test"},
                {"action_type": "send_message", "target": "team-data", "reason": "notify"},
            ],
        })
        result = _parse_diagnosis(text, sample_report)
        assert len(result.suggested_actions) == 2
        # Each action should have a unique UUID
        ids = [a.action_id for a in result.suggested_actions]
        assert ids[0] != ids[1]

    def test_defaults_missing_fields(self, sample_report):
        text = json.dumps({
            "root_cause_category": "data_malformed",
            "confidence": 0.6,
        })
        result = _parse_diagnosis(text, sample_report)
        assert result.evidence == []
        assert result.suggested_actions == []

    def test_raises_on_invalid_json(self, sample_report):
        with pytest.raises(json.JSONDecodeError):
            _parse_diagnosis("not json at all", sample_report)


class TestDispatchTool:
    @pytest.mark.asyncio
    async def test_dispatch_get_source_code(self, monkeypatch):
        from bau.analysis.tools.gitlab_tool import SourceCodeResult

        async def mock_get_source(*args, **kwargs):
            return SourceCodeResult(
                task_code="print('hello')",
                task_file_path="tasks/collect.py",
                dag_code="dag = DAG('test')",
                dag_file_path="dags/test_dag.py",
            )

        monkeypatch.setattr("bau.analysis.agent.get_source_code", mock_get_source)
        result = await _dispatch_tool("get_source_code", {"dag_id": "test", "task_id": "collect"})
        assert "print('hello')" in result
        assert "DAG('test')" in result

    @pytest.mark.asyncio
    async def test_dispatch_get_job_runbook(self, monkeypatch):
        async def mock_runbook(*args, **kwargs):
            return "# Job Runbook\nThis job collects data from API."

        monkeypatch.setattr("bau.analysis.agent.get_job_runbook", mock_runbook)
        result = await _dispatch_tool("get_job_runbook", {"dag_id": "test"})
        assert "Job Runbook" in result

    @pytest.mark.asyncio
    async def test_dispatch_unknown_tool(self):
        result = await _dispatch_tool("nonexistent_tool", {})
        assert "Unknown tool" in result

    @pytest.mark.asyncio
    async def test_dispatch_get_full_log(self, monkeypatch):
        async def mock_full_log(*args, **kwargs):
            return "full log content here"

        monkeypatch.setattr("bau.analysis.agent.get_full_airflow_log", mock_full_log)
        result = await _dispatch_tool(
            "get_full_log",
            {"dag_id": "test", "dag_run_id": "run-1", "task_id": "collect"},
        )
        assert "full log content" in result
