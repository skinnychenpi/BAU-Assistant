"""
Report generator — converts DiagnosisResult into API payloads.
"""

from __future__ import annotations

from datetime import datetime

from bau.state.models import (
    DiagnosisResult,
    IngestionResult,
    FailedTaskReport,
    RunningDAGReport,
    SchedulerNotFoundDAG,
)


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


def format_not_found_report(dags: list[SchedulerNotFoundDAG]) -> str:
    """Format a report section for DAGs not found in the scheduler."""
    if not dags:
        return ""
    lines = [
        f"{'='*60}",
        "DAGs Not Found in Scheduler",
        f"{'='*60}",
    ]
    for dag in dags:
        lines.append(f"  - {dag.dag_id}")
    lines.append("")
    lines.append("These DAG IDs do not exist in the scheduler.")
    lines.append("Check if they were renamed, deleted, or not yet deployed.")
    lines.append(f"{'='*60}")
    return "\n".join(lines)


def format_running_report(report: RunningDAGReport) -> str:
    """Format a report for a DAG run that is still running."""
    if report.is_overtime is None:
        overtime_str = "UNKNOWN (no avg duration available — pay attention)"
    elif report.is_overtime:
        overtime_str = "YES — running significantly longer than average"
    else:
        overtime_str = "No"

    avg_str = str(report.avg_duration) if report.avg_duration else "N/A"

    lines = [
        f"{'='*60}",
        "Still Running — Attention Needed",
        f"{'='*60}",
        f"DAG:          {report.dag_id}",
        f"Run ID:       {report.dag_run_id}",
        f"Scheduled:    {report.scheduled_time}",
        f"Duration:     {report.duration} (avg: {avg_str})",
        f"Overtime:     {overtime_str}",
    ]
    if report.dag_run_url:
        lines.append(f"Airflow UI:   {report.dag_run_url}")
    lines.append("")
    if report.running_tasks:
        lines.append(f"Running Tasks:  {', '.join(report.running_tasks)}")
    if report.pending_tasks:
        lines.append(f"Pending Tasks:  {', '.join(report.pending_tasks)}")
    lines.append(f"{'='*60}")
    return "\n".join(lines)


def format_full_summary(parse_result: IngestionResult, diagnoses: list[DiagnosisResult] | None = None) -> str:
    """Format a complete summary report from an email parse result."""
    sections: list[str] = []

    if parse_result.not_found_dags:
        sections.append(format_not_found_report(parse_result.not_found_dags))

    if diagnoses:
        for diag in diagnoses:
            matching = [r for r in parse_result.failed_reports if r.dag_run_id == diag.dag_run_id]
            if matching:
                sections.append(format_report_text(matching[0], diag))
    elif parse_result.failed_reports:
        lines = [
            f"{'='*60}",
            f"Failed DAG Runs: {len(parse_result.failed_reports)}",
            f"{'='*60}",
        ]
        for r in parse_result.failed_reports:
            avg_str = str(r.avg_duration) if r.avg_duration else "N/A"
            url_str = f" | {r.dag_run_url}" if r.dag_run_url else ""
            lines.append(f"  {r.dag_id}")
            lines.append(f"    Run:      {r.dag_run_id}")
            lines.append(f"    Duration: {r.duration} (avg: {avg_str})")
            lines.append(f"    Root cause tasks: {', '.join(r.root_cause_tasks)}{url_str}")
            lines.append("")
        lines.append(f"{'='*60}")
        sections.append("\n".join(lines))

    for r in parse_result.running_reports:
        sections.append(format_running_report(r))

    return "\n\n".join(sections)
