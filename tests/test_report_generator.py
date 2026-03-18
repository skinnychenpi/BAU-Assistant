"""
Tests for report/generator.py — payload and text generation.
"""

import pytest

from bau.report.generator import format_report_text, generate_report_payload


class TestGenerateReportPayload:
    def test_contains_required_fields(self, sample_report, sample_diagnosis):
        payload = generate_report_payload(sample_report, sample_diagnosis)
        assert payload["dag_id"] == sample_report.dag_id
        assert payload["dag_run_id"] == sample_report.dag_run_id
        assert payload["root_cause_category"] == "network_error"
        assert payload["confidence"] == 0.85
        assert payload["is_overtime"] is False
        assert "diagnosed_at" in payload
        assert "scheduled_time" in payload

    def test_includes_evidence(self, sample_report, sample_diagnosis):
        payload = generate_report_payload(sample_report, sample_diagnosis)
        assert len(payload["evidence"]) == 2
        assert "ConnectionRefusedError" in payload["evidence"][0]

    def test_includes_actions(self, sample_report, sample_diagnosis):
        payload = generate_report_payload(sample_report, sample_diagnosis)
        assert len(payload["suggested_actions"]) == 1
        action = payload["suggested_actions"][0]
        assert action["action_type"] == "airflow_rerun"
        assert action["action_id"] == "test-action-001"

    def test_includes_task_info(self, sample_report, sample_diagnosis):
        payload = generate_report_payload(sample_report, sample_diagnosis)
        assert payload["failed_tasks"] == ["collect"]
        assert payload["root_cause_tasks"] == ["collect"]


class TestFormatReportText:
    def test_contains_key_info(self, sample_report, sample_diagnosis):
        text = format_report_text(sample_report, sample_diagnosis)
        assert "network_error" in text
        assert "85%" in text
        assert "collect" in text
        assert "airflow_rerun" in text

    def test_contains_evidence(self, sample_report, sample_diagnosis):
        text = format_report_text(sample_report, sample_diagnosis)
        assert "ConnectionRefusedError" in text

    def test_contains_action_ids(self, sample_report, sample_diagnosis):
        text = format_report_text(sample_report, sample_diagnosis)
        assert "test-act" in text  # first 8 chars of action_id
