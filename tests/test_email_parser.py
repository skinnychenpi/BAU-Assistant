"""
Tests for email_parser.py — Phase 1 completion gate.
Run with: pytest tests/test_email_parser.py -v
"""

from pathlib import Path
from datetime import timedelta

import pytest

from bau.ingestion.email_parser import parse_email_body, parse_email_full

SAMPLE = (Path(__file__).parent / "fixtures" / "sample_email.txt").read_text()
REAL_SAMPLE = (Path(__file__).parent / "fixtures" / "sample_email_real.html").read_text()


# ── Legacy API tests (parse_email_body — backward compat) ────────────────────

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


# ── Old fixture: dag_run_url is None (no <a> tags in old format) ─────────────

def test_old_fixture_has_no_url():
    reports = parse_email_body(SAMPLE)
    for r in reports:
        assert r.dag_run_url is None


# ── Real email tests (parse_email_full) ──────────────────────────────────────

def test_real_not_found_in_scheduler():
    result = parse_email_full(REAL_SAMPLE)
    assert len(result.not_found_dags) == 1
    assert result.not_found_dags[0].dag_id == "data_data_pltingestion_codm_sea_rawlog"


def test_real_failed_report_count():
    result = parse_email_full(REAL_SAMPLE)
    # 14 failed DAG runs in the real sample
    assert len(result.failed_reports) == 14


def test_real_running_report_count():
    result = parse_email_full(REAL_SAMPLE)
    # 2 running DAG runs
    assert len(result.running_reports) == 2


def test_real_dag_run_url_extracted():
    result = parse_email_full(REAL_SAMPLE)
    r = result.failed_reports[0]
    assert r.dag_run_url is not None
    assert "tradewinds.grass.garenanow.com" in r.dag_run_url
    assert r.dag_id in r.dag_run_url


def test_real_avg_na_handled():
    result = parse_email_full(REAL_SAMPLE)
    for r in result.failed_reports:
        assert r.avg_duration is None
        assert r.is_overtime is False


def test_real_running_tasks_parsed():
    result = parse_email_full(REAL_SAMPLE)
    blacklist = [r for r in result.running_reports if "blacklist" in r.dag_id][0]
    assert "ff_blacklist_collect" in blacklist.running_tasks
    assert "send_time_tracing_message" in blacklist.pending_tasks


def test_real_running_na_avg_is_overtime_none():
    result = parse_email_full(REAL_SAMPLE)
    for r in result.running_reports:
        assert r.avg_duration is None
        assert r.is_overtime is None


def test_real_running_url_extracted():
    result = parse_email_full(REAL_SAMPLE)
    for r in result.running_reports:
        assert r.dag_run_url is not None
        assert r.dag_id in r.dag_run_url


def test_real_taskgroup_dotted_ids():
    result = parse_email_full(REAL_SAMPLE)
    cslogs = [r for r in result.failed_reports if "cslogs" in r.dag_id and "vn" in r.dag_id][0]
    assert "cslogs_priority.collect_data" in cslogs.failed_tasks
    assert "cslogs_non_priority.collect_data" in cslogs.upstream_failed_tasks


def test_real_multiple_root_cause_tasks():
    result = parse_email_full(REAL_SAMPLE)
    non_split = [r for r in result.failed_reports if "non_split" in r.dag_id][0]
    assert len(non_split.root_cause_tasks) > 1
    assert "garena_point_db_channel_tab_send_sh" in non_split.root_cause_tasks
    assert "send_time_tracing_message" not in non_split.root_cause_tasks


def test_real_running_multiple_pending_tasks():
    result = parse_email_full(REAL_SAMPLE)
    batch = [r for r in result.running_reports if "batch" in r.dag_id][0]
    assert "collect_EventTypePlayerGameSummary" in batch.running_tasks
    assert len(batch.pending_tasks) == 3
