"""
Tests for email_parser.py — Phase 1 completion gate.
Run with: pytest tests/test_email_parser.py -v
"""

from pathlib import Path
from datetime import timedelta

import pytest

from bau.ingestion.email_parser import parse_email_body

SAMPLE = (Path(__file__).parent / "fixtures" / "sample_email.txt").read_text()


def test_parses_correct_number_of_reports():
    reports = parse_email_body(SAMPLE)
    # 6 DAG runs in the sample, all have at least one failed task
    assert len(reports) == 6


def test_linear_dag_root_cause():
    reports = parse_email_body(SAMPLE)
    linear = [r for r in reports if "linear" in r.dag_id]
    assert len(linear) == 2
    for r in linear:
        assert r.failed_tasks == ["collect"]
        assert r.upstream_failed_tasks == ["parse"]
        assert r.root_cause_tasks == ["collect"]


def test_fork_dag_multiple_failures():
    reports = parse_email_body(SAMPLE)
    fork = [r for r in reports if "fork" in r.dag_id][0]
    assert "item_1_collect" in fork.failed_tasks
    assert "item_1_cook" in fork.upstream_failed_tasks
    assert "item_1_cook" not in fork.root_cause_tasks


def test_taskgroup_dag_dotted_task_ids():
    reports = parse_email_body(SAMPLE)
    tg = [r for r in reports if "taskgroup" in r.dag_id][0]
    assert "game_1.game_1_collect" in tg.failed_tasks
    assert "end" in tg.upstream_failed_tasks


def test_overtime_detection():
    reports = parse_email_body(SAMPLE)
    # fork_dag: duration 00:11:00, avg 00:06:01 → ratio ~1.83 → overtime at 1.5x
    fork = [r for r in reports if "fork" in r.dag_id][0]
    assert fork.is_overtime is True

    # linear_dag: duration 00:01:58, avg 00:03:54 → ratio ~0.5 → NOT overtime
    linear = [r for r in reports if "linear" in r.dag_id][0]
    assert linear.is_overtime is False


def test_scheduled_time_extracted_from_run_id():
    reports = parse_email_body(SAMPLE)
    r = reports[0]
    assert r.scheduled_time.year == 2026
    assert r.scheduled_time.month == 2


def test_empty_body_returns_empty_list():
    assert parse_email_body("") == []


def test_body_with_no_failures_returns_empty_list():
    body = """
Dag ID: some_dag
Dag Run ID: scheduled__2026-01-01T00:00:00+00:00     success
Start date: 2026-01-01 08:00:00 SGT
Duration: 00:01:00 (avg:00:01:00)
     Task ID: task_a     success
"""
    assert parse_email_body(body) == []
