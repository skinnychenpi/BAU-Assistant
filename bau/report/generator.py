"""
Report generator — converts DiagnosisResult into API payloads.
"""

from __future__ import annotations

from datetime import datetime

from bau.state.models import DiagnosisResult, FailedTaskReport


def generate_report_payload(
    report: FailedTaskReport,
    diagnosis: DiagnosisResult,
) -> dict:
    """
    Convert a DiagnosisResult into a payload for the internal report API.

    Returns a dict ready to be JSON-serialized and posted.
    """
    return {
        "dag_id": diagnosis.dag_id,
        "dag_run_id": diagnosis.dag_run_id,
        "diagnosed_at": datetime.utcnow().isoformat(),
        "scheduled_time": report.scheduled_time.isoformat(),
        "root_cause_category": diagnosis.root_cause_category,
        "confidence": diagnosis.confidence,
        "is_overtime": report.is_overtime,
        "overtime_ratio": report.overtime_ratio,
        "failed_tasks": report.failed_tasks,
        "root_cause_tasks": report.root_cause_tasks,
        "evidence": diagnosis.evidence,
        "suggested_actions": [
            {
                "action_id": action.action_id,
                "action_type": action.action_type,
                "target": action.target,
                "params": action.params,
                "reason": action.reason,
            }
            for action in diagnosis.suggested_actions
        ],
    }


def format_report_text(
    report: FailedTaskReport,
    diagnosis: DiagnosisResult,
) -> str:
    """Format a human-readable report for CLI display."""
    lines = [
        f"{'='*60}",
        f"BAU Diagnosis Report",
        f"{'='*60}",
        f"DAG:          {diagnosis.dag_id}",
        f"Run ID:       {diagnosis.dag_run_id}",
        f"Scheduled:    {report.scheduled_time}",
        f"Duration:     {report.duration} (avg: {report.avg_duration})",
        f"Overtime:     {'YES' if report.is_overtime else 'No'}",
        f"",
        f"Root Cause:   {diagnosis.root_cause_category}",
        f"Confidence:   {diagnosis.confidence:.0%}",
        f"",
        f"Failed Tasks: {', '.join(report.failed_tasks)}",
        f"Root Cause:   {', '.join(report.root_cause_tasks)}",
        f"",
        f"Evidence:",
    ]
    for e in diagnosis.evidence:
        lines.append(f"  - {e}")

    lines.append("")
    lines.append("Suggested Actions:")
    for action in diagnosis.suggested_actions:
        lines.append(f"  [{action.action_id[:8]}] {action.action_type}: {action.reason}")

    lines.append(f"{'='*60}")
    return "\n".join(lines)
