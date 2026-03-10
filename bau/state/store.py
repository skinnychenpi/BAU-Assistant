"""
SQLite-backed state store.
Provides durable execution across process restarts — critical for the
AWAIT_HUMAN → EXECUTE transition where the process exits and resumes later.
"""

from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path

from bau.state.models import Action, ActionStatus, AgentRun, AgentStatus

DB_PATH = Path("bau_state.db")


def init_db(path: Path = DB_PATH) -> None:
    """Create tables if they don't exist. Safe to call on every startup."""
    with _conn(path) as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS agent_runs (
                run_id       TEXT PRIMARY KEY,
                triggered_at TIMESTAMP NOT NULL,
                status       TEXT NOT NULL,
                summary      JSON
            );

            CREATE TABLE IF NOT EXISTS pending_actions (
                action_id    TEXT PRIMARY KEY,
                run_id       TEXT NOT NULL,
                action_type  TEXT NOT NULL,
                target       TEXT NOT NULL,
                params       JSON NOT NULL,
                reason       TEXT NOT NULL,
                status       TEXT NOT NULL DEFAULT 'PENDING',
                created_at   TIMESTAMP NOT NULL,
                FOREIGN KEY (run_id) REFERENCES agent_runs(run_id)
            );

            CREATE TABLE IF NOT EXISTS diagnosis_history (
                dag_id               TEXT NOT NULL,
                run_id               TEXT NOT NULL,
                diagnosed_at         TIMESTAMP NOT NULL,
                root_cause_category  TEXT NOT NULL,
                resolution           TEXT,
                PRIMARY KEY (dag_id, run_id)
            );
        """)


@contextmanager
def _conn(path: Path = DB_PATH):
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ── Agent runs ─────────────────────────────────────────────────────────────────

def create_run(run: AgentRun, path: Path = DB_PATH) -> None:
    with _conn(path) as conn:
        conn.execute(
            "INSERT INTO agent_runs (run_id, triggered_at, status, summary) VALUES (?,?,?,?)",
            (run.run_id, run.triggered_at.isoformat(), run.status, json.dumps({})),
        )


def update_run_status(run_id: str, status: AgentStatus, path: Path = DB_PATH) -> None:
    with _conn(path) as conn:
        conn.execute(
            "UPDATE agent_runs SET status = ? WHERE run_id = ?",
            (status, run_id),
        )


def get_run(run_id: str, path: Path = DB_PATH) -> sqlite3.Row | None:
    with _conn(path) as conn:
        return conn.execute(
            "SELECT * FROM agent_runs WHERE run_id = ?", (run_id,)
        ).fetchone()


# ── Pending actions ────────────────────────────────────────────────────────────

def save_action(run_id: str, action: Action, path: Path = DB_PATH) -> None:
    with _conn(path) as conn:
        conn.execute(
            """INSERT INTO pending_actions
               (action_id, run_id, action_type, target, params, reason, status, created_at)
               VALUES (?,?,?,?,?,?,?,?)""",
            (
                action.action_id,
                run_id,
                action.action_type,
                action.target,
                json.dumps(action.params),
                action.reason,
                action.status,
                datetime.utcnow().isoformat(),
            ),
        )


def get_pending_actions(run_id: str | None = None, path: Path = DB_PATH) -> list[sqlite3.Row]:
    with _conn(path) as conn:
        if run_id:
            return conn.execute(
                "SELECT * FROM pending_actions WHERE run_id = ? AND status = 'PENDING'",
                (run_id,),
            ).fetchall()
        return conn.execute(
            "SELECT * FROM pending_actions WHERE status = 'PENDING'"
        ).fetchall()


def update_action_status(
    action_id: str, status: ActionStatus, path: Path = DB_PATH
) -> None:
    with _conn(path) as conn:
        conn.execute(
            "UPDATE pending_actions SET status = ? WHERE action_id = ?",
            (status, action_id),
        )


# ── Diagnosis history ──────────────────────────────────────────────────────────

def save_diagnosis(
    dag_id: str,
    run_id: str,
    category: str,
    resolution: str | None = None,
    path: Path = DB_PATH,
) -> None:
    with _conn(path) as conn:
        conn.execute(
            """INSERT OR REPLACE INTO diagnosis_history
               (dag_id, run_id, diagnosed_at, root_cause_category, resolution)
               VALUES (?,?,?,?,?)""",
            (dag_id, run_id, datetime.utcnow().isoformat(), category, resolution),
        )


def get_history(dag_id: str, limit: int = 10, path: Path = DB_PATH) -> list[sqlite3.Row]:
    with _conn(path) as conn:
        return conn.execute(
            """SELECT * FROM diagnosis_history WHERE dag_id = ?
               ORDER BY diagnosed_at DESC LIMIT ?""",
            (dag_id, limit),
        ).fetchall()
