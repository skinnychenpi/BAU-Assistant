"""
Core diagnosis agent — runs a Claude tool-use loop.

Guided approach:
  - Orchestrator pre-fetches log + historical issues
  - Agent receives rich initial context
  - Agent can call additional tools on-demand (source code, runbook, full log)
"""

from __future__ import annotations

import json
import logging
import uuid
from dataclasses import field

import anthropic

from bau.analysis.prompts import (
    SYSTEM_PROMPT,
    TOOL_DEFINITIONS,
    build_initial_user_message,
)
from bau.analysis.tools.airflow_log_tool import ProcessedLog, get_full_airflow_log
from bau.analysis.tools.confluence_tool import get_job_runbook
from bau.analysis.tools.gitlab_tool import get_source_code
from bau.config import settings
from bau.state.models import Action, DiagnosisResult, FailedTaskReport

logger = logging.getLogger(__name__)


async def _dispatch_tool(name: str, input_args: dict) -> str:
    """Dispatch a tool call to the appropriate handler, return result as string."""
    if name == "get_source_code":
        result = await get_source_code(
            dag_id=input_args["dag_id"],
            task_id=input_args["task_id"],
        )
        parts = []
        if result.dag_code:
            parts.append(f"## DAG Definition ({result.dag_file_path})\n```python\n{result.dag_code}\n```")
        if result.task_code:
            parts.append(f"## Task Source ({result.task_file_path})\n```python\n{result.task_code}\n```")
        if result.error:
            parts.append(f"Error: {result.error}")
        return "\n\n".join(parts) if parts else "No source code found."

    elif name == "get_job_runbook":
        return await get_job_runbook(dag_id=input_args["dag_id"])

    elif name == "get_full_log":
        return await get_full_airflow_log(
            dag_id=input_args["dag_id"],
            dag_run_id=input_args["dag_run_id"],
            task_id=input_args["task_id"],
        )

    return f"Unknown tool: {name}"


def _parse_diagnosis(text: str, report: FailedTaskReport) -> DiagnosisResult:
    """Parse the LLM's JSON response into a DiagnosisResult."""
    # Extract JSON from the response (may be wrapped in markdown code block)
    json_text = text
    if "```json" in text:
        start = text.index("```json") + 7
        end = text.index("```", start)
        json_text = text[start:end]
    elif "```" in text:
        start = text.index("```") + 3
        end = text.index("```", start)
        json_text = text[start:end]

    data = json.loads(json_text.strip())

    actions = []
    for a in data.get("suggested_actions", []):
        actions.append(
            Action(
                action_id=str(uuid.uuid4()),
                action_type=a.get("action_type", "manual_only"),
                target=a.get("target", report.dag_run_id),
                params=a.get("params", {}),
                reason=a.get("reason", ""),
            )
        )

    return DiagnosisResult(
        dag_id=report.dag_id,
        dag_run_id=report.dag_run_id,
        root_cause_category=data.get("root_cause_category", "unknown"),
        confidence=float(data.get("confidence", 0.0)),
        evidence=data.get("evidence", []),
        suggested_actions=actions,
    )


async def diagnose(
    report: FailedTaskReport,
    log: ProcessedLog,
    historical_issues: str,
) -> DiagnosisResult:
    """
    Run the diagnosis agent for a single FailedTaskReport.

    This is the main entry point for the analysis engine.
    The orchestrator pre-fetches log and historical issues,
    then calls this function.

    Args:
        report: The parsed failure report from email.
        log: Pre-fetched and pre-processed task log.
        historical_issues: Pre-fetched historical issues text.

    Returns:
        DiagnosisResult with root cause, evidence, and suggested actions.
    """
    client = anthropic.AsyncAnthropic(api_key=settings.anthropic_api_key)

    # Build initial message with all pre-fetched context
    user_message = build_initial_user_message(report, log, historical_issues)

    messages = [{"role": "user", "content": user_message}]
    reasoning_trace: list[dict] = []

    for step in range(settings.agent_max_steps):
        logger.info(f"Agent step {step + 1}/{settings.agent_max_steps}")

        response = await client.messages.create(
            model=settings.anthropic_model,
            max_tokens=4096,
            system=SYSTEM_PROMPT,
            tools=TOOL_DEFINITIONS,
            messages=messages,
        )

        # Record this step in the reasoning trace
        reasoning_trace.append({
            "step": step + 1,
            "stop_reason": response.stop_reason,
            "content": [block.model_dump() for block in response.content],
        })

        # If the model is done (no tool use), parse the final response
        if response.stop_reason == "end_turn":
            # Extract text from the response
            text_parts = [
                block.text for block in response.content if block.type == "text"
            ]
            full_text = "\n".join(text_parts)

            result = _parse_diagnosis(full_text, report)
            result.reasoning_trace = reasoning_trace
            return result

        # Handle tool use
        if response.stop_reason == "tool_use":
            # Add assistant's response (with tool_use blocks) to messages
            messages.append({"role": "assistant", "content": response.content})

            # Process each tool call
            tool_results = []
            for block in response.content:
                if block.type == "tool_use":
                    logger.info(f"Tool call: {block.name}({block.input})")
                    try:
                        result_text = await _dispatch_tool(block.name, block.input)
                    except Exception as e:
                        logger.error(f"Tool error: {e}")
                        result_text = f"Error calling {block.name}: {e}"

                    tool_results.append({
                        "type": "tool_result",
                        "tool_use_id": block.id,
                        "content": result_text,
                    })

            messages.append({"role": "user", "content": tool_results})

    # Max steps reached — return what we have
    logger.warning(f"Agent hit max steps ({settings.agent_max_steps})")
    return DiagnosisResult(
        dag_id=report.dag_id,
        dag_run_id=report.dag_run_id,
        root_cause_category="unknown",
        confidence=0.0,
        evidence=["Agent reached maximum steps without completing diagnosis"],
        suggested_actions=[
            Action(
                action_id=str(uuid.uuid4()),
                action_type="manual_only",
                target=report.dag_run_id,
                params={},
                reason="Automated diagnosis could not determine root cause. Manual investigation required.",
            )
        ],
        reasoning_trace=reasoning_trace,
    )
