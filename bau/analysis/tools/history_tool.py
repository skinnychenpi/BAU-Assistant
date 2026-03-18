"""
Historical issues tool — fetches past issues from Google Sheets.

Used by the orchestrator to pre-fetch context before the agent runs.
"""

from __future__ import annotations

from bau.ingestion.gsheet_client import HistoricalIssue, fetch_issues_for_dag


def format_issues_for_prompt(issues: list[HistoricalIssue]) -> str:
    """Format historical issues into a text block for the LLM prompt."""
    if not issues:
        return "No historical issues found for this DAG."

    lines = [f"Found {len(issues)} historical issue(s):\n"]
    for i, issue in enumerate(issues, 1):
        lines.append(f"--- Issue #{i} ---")
        lines.append(f"Date: {issue.issue_date}")
        lines.append(f"Description: {issue.description}")
        if issue.logs:
            # Truncate very long logs
            log_preview = issue.logs[:500]
            if len(issue.logs) > 500:
                log_preview += "..."
            lines.append(f"Logs: {log_preview}")
        lines.append(f"Fix: {issue.fix_action}")
        lines.append("")
    return "\n".join(lines)


def get_historical_issues(dag_id: str) -> str:
    """
    Fetch and format historical issues for a DAG.

    Returns a formatted text block ready for inclusion in the agent prompt.
    """
    issues = fetch_issues_for_dag(dag_id)
    return format_issues_for_prompt(issues)
