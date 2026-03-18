"""
Tests for state/store.py — SQLite CRUD operations.
"""

from datetime import datetime
from pathlib import Path

import pytest

from bau.state.models import Action, AgentRun
from bau.state.store import (
    create_run,
    get_history,
    get_pending_actions,
    get_run,
    init_db,
    save_action,
    save_diagnosis,
    update_action_status,
    update_run_status,
)


@pytest.fixture
def db_path(tmp_path) -> Path:
    """Create a temporary database for each test."""
    path = tmp_path / "test_bau.db"
    init_db(path)
    return path


@pytest.fixture
def sample_run() -> AgentRun:
    return AgentRun(
        run_id="test-run-001",
        triggered_at=datetime(2026, 3, 1, 10, 0, 0),
        status="IDLE",
    )


@pytest.fixture
def sample_action() -> Action:
    return Action(
        action_id="test-action-001",
        action_type="airflow_rerun",
        target="scheduled__2026-02-25T18:00:00+00:00",
        params={"task_id": "collect"},
        reason="Transient error, rerun should fix",
    )


def test_init_db_creates_tables(db_path):
    # init_db already called in fixture — just verify tables exist
    import sqlite3

    conn = sqlite3.connect(db_path)
    tables = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()
    table_names = {t[0] for t in tables}
    assert "agent_runs" in table_names
    assert "pending_actions" in table_names
    assert "diagnosis_history" in table_names
    conn.close()


def test_init_db_is_idempotent(db_path):
    # Calling init_db again should not raise
    init_db(db_path)


def test_create_and_get_run(db_path, sample_run):
    create_run(sample_run, path=db_path)
    row = get_run(sample_run.run_id, path=db_path)
    assert row is not None
    assert row["run_id"] == "test-run-001"
    assert row["status"] == "IDLE"


def test_get_nonexistent_run(db_path):
    row = get_run("nonexistent", path=db_path)
    assert row is None


def test_update_run_status(db_path, sample_run):
    create_run(sample_run, path=db_path)
    update_run_status(sample_run.run_id, "ANALYZE", path=db_path)
    row = get_run(sample_run.run_id, path=db_path)
    assert row["status"] == "ANALYZE"


def test_save_and_get_pending_actions(db_path, sample_run, sample_action):
    create_run(sample_run, path=db_path)
    save_action(sample_run.run_id, sample_action, path=db_path)

    actions = get_pending_actions(sample_run.run_id, path=db_path)
    assert len(actions) == 1
    assert actions[0]["action_id"] == "test-action-001"
    assert actions[0]["action_type"] == "airflow_rerun"
    assert actions[0]["status"] == "PENDING"


def test_get_pending_actions_all(db_path, sample_run, sample_action):
    create_run(sample_run, path=db_path)
    save_action(sample_run.run_id, sample_action, path=db_path)

    actions = get_pending_actions(path=db_path)
    assert len(actions) == 1


def test_update_action_status(db_path, sample_run, sample_action):
    create_run(sample_run, path=db_path)
    save_action(sample_run.run_id, sample_action, path=db_path)

    update_action_status("test-action-001", "APPROVED", path=db_path)
    # APPROVED actions should no longer appear in pending
    actions = get_pending_actions(sample_run.run_id, path=db_path)
    assert len(actions) == 0


def test_save_and_get_diagnosis_history(db_path):
    save_diagnosis("test_dag", "run-001", "network_error", "Reran successfully", path=db_path)
    save_diagnosis("test_dag", "run-002", "data_malformed", "Fixed upstream", path=db_path)
    save_diagnosis("other_dag", "run-003", "code_bug", None, path=db_path)

    history = get_history("test_dag", path=db_path)
    assert len(history) == 2
    assert history[0]["root_cause_category"] in ("network_error", "data_malformed")


def test_diagnosis_history_limit(db_path):
    for i in range(15):
        save_diagnosis("test_dag", f"run-{i:03d}", "unknown", path=db_path)

    history = get_history("test_dag", limit=5, path=db_path)
    assert len(history) == 5


def test_diagnosis_upsert(db_path):
    save_diagnosis("test_dag", "run-001", "unknown", None, path=db_path)
    save_diagnosis("test_dag", "run-001", "network_error", "Fixed", path=db_path)

    history = get_history("test_dag", path=db_path)
    assert len(history) == 1
    assert history[0]["root_cause_category"] == "network_error"
    assert history[0]["resolution"] == "Fixed"
