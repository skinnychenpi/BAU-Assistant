"""
System prompt and reasoning framework for the BAU diagnosis agent.
"""

from __future__ import annotations

from bau.analysis.tools.airflow_log_tool import ProcessedLog
from bau.state.models import FailedTaskReport

SYSTEM_PROMPT = """\
You are an expert data pipeline BAU (Business As Usual) engineer. Your job is to \
diagnose why an Airflow DAG task failed and suggest the correct fix.

## Your Reasoning Process

1. **Read the log** — Identify the exception type and error message. \
Look for the root cause, not just symptoms.
2. **Check historical issues** — Has this DAG failed before with the same error? \
If so, the same fix likely applies.
3. **Read the code** (if needed) — Use the `get_source_code` tool to understand \
what the task does. Look at imports, API calls, file paths, and data transformations.
4. **Read the runbook** (if needed) — Use the `get_job_runbook` tool to understand \
business context, expected data sources, and known quirks.
5. **Synthesize** — Combine all evidence to determine the root cause category \
and suggest specific actions.

## Diagnosis Categories

You MUST classify the root cause into one of these categories:

- `data_malformed`: The input data has unexpected format, missing fields, \
encoding issues, schema mismatch, or corrupt records.
- `network_error`: Connection refused, timeout, DNS resolution failure, \
HTTP 5xx from upstream service, VPN issues.
- `code_bug`: Logic error in the code — wrong column name, incorrect join, \
null pointer, type mismatch, missing import.
- `permission_error`: Access denied, expired credentials, missing IAM role, \
file permission denied, database access revoked.
- `unknown`: You cannot confidently determine the root cause. Be honest — \
do NOT guess.

## Action Types

For each diagnosis, suggest one or more actions:

- `airflow_rerun`: The failure is transient (network blip, temporary data issue). \
Rerunning the task should fix it.
- `send_message`: A human needs to be notified (data source owner, infra team, etc.). \
Specify who and what message.
- `manual_only`: The fix requires human intervention (code change, config update, \
permission grant). Describe what needs to be done.

## Output Format

Respond with a JSON object:
```json
{
    "root_cause_category": "one of: data_malformed, network_error, code_bug, permission_error, unknown",
    "confidence": 0.0 to 1.0,
    "evidence": ["list of specific log lines, code snippets, or facts supporting your diagnosis"],
    "suggested_actions": [
        {
            "action_type": "airflow_rerun | send_message | manual_only",
            "target": "dag_run_id for rerun, or person/team for message",
            "params": {},
            "reason": "why this action is needed"
        }
    ],
    "summary": "A 1-2 sentence summary of the diagnosis for the report."
}
```

## Important Rules

- Be specific in your evidence — cite exact error messages, line numbers, file paths.
- If the log shows a transient error (timeout, connection reset) and history shows \
the same DAG succeeded after rerun, suggest `airflow_rerun` with high confidence.
- If you see a code-level bug, suggest `manual_only` — do NOT suggest rerunning.
- Set confidence below 0.5 if you are uncertain. It is better to say "unknown" \
than to guess wrong.
- When using tools, be targeted — don't fetch code unless the log alone is insufficient.
"""


def build_initial_user_message(
    report: FailedTaskReport,
    log: ProcessedLog,
    historical_issues: str,
) -> str:
    """
    Build the first user message for the agent, including all pre-fetched context.
    """
    parts = [
        "## Failed Task Alert\n",
        f"- **DAG ID**: {report.dag_id}",
        f"- **DAG Run ID**: {report.dag_run_id}",
        f"- **Scheduled Time**: {report.scheduled_time}",
        f"- **Start Date**: {report.start_date}",
        f"- **Duration**: {report.duration} (avg: {report.avg_duration or 'N/A'})",
        f"- **Overtime**: {'YES' if report.is_overtime else 'No'} "
        f"(ratio: {report.overtime_ratio:.2f})",
        f"- **Root Cause Tasks**: {', '.join(report.root_cause_tasks)}",
        f"- **Failed Tasks**: {', '.join(report.failed_tasks)}",
        f"- **Upstream Failed Tasks**: {', '.join(report.upstream_failed_tasks)}",
    ]

    # Add log context
    parts.append("\n## Task Log\n")
    parts.append(f"**Log Source**: {log.source}")
    if log.exception:
        parts.append(f"\n**Exception**: {log.exception}")
    if log.stack_trace:
        parts.append(f"\n**Stack Trace**:\n```\n{log.stack_trace}\n```")
    if log.tail_lines:
        tail_text = "\n".join(log.tail_lines)
        parts.append(f"\n**Last {len(log.tail_lines)} lines**:\n```\n{tail_text}\n```")
    elif not log.raw_log:
        parts.append("\n*No log content available.*")

    # Add historical issues
    parts.append("\n## Historical Issues\n")
    parts.append(historical_issues)

    parts.append(
        "\n\n---\n"
        "Please diagnose this failure. Use the available tools if you need "
        "more context (source code, runbook). Respond with the JSON diagnosis."
    )

    return "\n".join(parts)


# ── Tool definitions for Claude API ───────────────────────────────────────────

TOOL_DEFINITIONS = [
    {
        "name": "get_source_code",
        "description": (
            "Fetch the task source code and DAG definition from GitLab. "
            "Use this when you need to understand what the code does, "
            "check for bugs, or see how tasks are connected."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "dag_id": {
                    "type": "string",
                    "description": "The DAG identifier",
                },
                "task_id": {
                    "type": "string",
                    "description": "The task identifier",
                },
            },
            "required": ["dag_id", "task_id"],
        },
    },
    {
        "name": "get_job_runbook",
        "description": (
            "Fetch the Confluence runbook page for this DAG. "
            "Use this when you need business context, expected data sources, "
            "known quirks, or operational procedures."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "dag_id": {
                    "type": "string",
                    "description": "The DAG identifier",
                },
            },
            "required": ["dag_id"],
        },
    },
    {
        "name": "get_full_log",
        "description": (
            "Fetch the full untruncated log for a task. "
            "Use this only if the last 150 lines are not enough to diagnose the issue."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "dag_id": {
                    "type": "string",
                    "description": "The DAG identifier",
                },
                "dag_run_id": {
                    "type": "string",
                    "description": "The DAG run identifier",
                },
                "task_id": {
                    "type": "string",
                    "description": "The task identifier",
                },
            },
            "required": ["dag_id", "dag_run_id", "task_id"],
        },
    },
]
