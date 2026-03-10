"""
Core data models for BAU-Assistant.
These dataclasses flow through the entire pipeline:
  Email → FailedTaskReport → DiagnosisResult → Action → pending_actions table
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Literal


# ── Ingestion layer output ─────────────────────────────────────────────────────

@dataclass
class FailedTaskReport:
    dag_id: str
    dag_run_id: str
    scheduled_time: datetime
    start_date: datetime
    duration: timedelta
    avg_duration: timedelta
    is_overtime: bool
    failed_tasks: list[str]           # status == "failed"  → real root causes
    upstream_failed_tasks: list[str]  # status == "upstream_failed" → cascades
    root_cause_tasks: list[str]       # computed: failed_tasks minus upstreams

    @property
    def overtime_ratio(self) -> float:
        if self.avg_duration.total_seconds() == 0:
            return 0.0
        return self.duration.total_seconds() / self.avg_duration.total_seconds()


# ── Analysis layer output ──────────────────────────────────────────────────────

RootCauseCategory = Literal[
    "data_malformed",
    "network_error",
    "code_bug",
    "permission_error",
    "unknown",
]

ActionType = Literal["airflow_rerun", "send_message", "manual_only"]

AgentStatus = Literal[
    "IDLE", "FETCH", "ANALYZE", "REPORT", "AWAIT_HUMAN", "EXECUTE", "DONE", "FAILED"
]

ActionStatus = Literal["PENDING", "APPROVED", "REJECTED", "EXECUTED"]


@dataclass
class Action:
    action_id: str
    action_type: ActionType
    target: str          # dag_run_id for rerun, user_id for message
    params: dict
    reason: str          # LLM's justification
    requires_confirmation: bool = True
    status: ActionStatus = "PENDING"


@dataclass
class DiagnosisResult:
    dag_id: str
    dag_run_id: str
    root_cause_category: RootCauseCategory
    confidence: float                  # 0.0 – 1.0
    evidence: list[str]                # log snippets / code lines supporting diagnosis
    suggested_actions: list[Action]
    reasoning_trace: list[dict] = field(default_factory=list)  # full LLM step log


# ── Agent run record ───────────────────────────────────────────────────────────

@dataclass
class AgentRun:
    run_id: str
    triggered_at: datetime
    status: AgentStatus
    failed_reports: list[FailedTaskReport] = field(default_factory=list)
    diagnoses: list[DiagnosisResult] = field(default_factory=list)
    error_message: str | None = None
