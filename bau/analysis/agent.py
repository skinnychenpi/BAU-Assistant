"""
Core diagnosis agent — runs a tool-use loop via SMART Platform API.

Guided approach:
  - Orchestrator pre-fetches log + historical issues
  - Agent receives rich initial context
  - Agent can call additional tools on-demand (source code, runbook, full log)

The SMART agent (GPT-4o-mini) responds with JSON:
  - {"action": "call_tool", "tool": "...", "params": {...}} → we dispatch and continue
  - {"action": "final_answer", "diagnosis": {...}} → we parse and return
"""

from __future__ import annotations

import json
import logging
import uuid

from bau.analysis.prompts import build_initial_user_message
from bau.analysis.smart_client import invoke as smart_invoke
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


def _extract_json(text: str) -> dict:
    """Extract JSON from response text, handling markdown code blocks."""
    json_text = text
    if "```json" in text:
        start = text.index("```json") + 7
        end = text.index("```", start)
        json_text = text[start:end]
    elif "```" in text:
        start = text.index("```") + 3
        end = text.index("```", start)
        json_text = text[start:end]
    return json.loads(json_text.strip())


def _parse_diagnosis(text: str, report: FailedTaskReport) -> DiagnosisResult:
    """Parse the LLM's JSON response into a DiagnosisResult."""
    data = _extract_json(text)

    # Handle both direct diagnosis and wrapped {"action": "final_answer", "diagnosis": {...}}
    if "action" in data and data["action"] == "final_answer":
        data = data.get("diagnosis", data)

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

    Uses SMART Platform API (GPT-4o-mini) with thread-based
    conversation for multi-turn tool-use loops.

    Args:
        report: The parsed failure report from email.
        log: Pre-fetched and pre-processed task log.
        historical_issues: Pre-fetched historical issues text.

    Returns:
        DiagnosisResult with root cause, evidence, and suggested actions.
    """
    # Build initial message with all pre-fetched context
    user_message = build_initial_user_message(report, log, historical_issues)

    reasoning_trace = []
    thread_id = None

    for step in range(settings.agent_max_steps):
        logger.info(f"Agent step {step + 1}/{settings.agent_max_steps}")

        # Call SMART API
        smart_response = await smart_invoke(
            message=user_message,
            thread_id=thread_id,
        )

        response_str = smart_response["response_str"]
        thread_id = smart_response["thread_id"]

        # Record this step
        reasoning_trace.append({
            "step": step + 1,
            "thread_id": thread_id,
            "response": response_str,
        })

        # Parse the JSON response
        try:
            data = _extract_json(response_str)
        except (json.JSONDecodeError, ValueError) as e:
            logger.warning(f"Failed to parse JSON from response: {e}")
            logger.debug(f"Raw response: {response_str}")
            # Try to treat the whole response as a final answer
            try:
                result = _parse_diagnosis(response_str, report)
                result.reasoning_trace = reasoning_trace
                return result
            except Exception:
                # Continue to next step or bail out
                user_message = (
                    "Your response was not valid JSON. "
                    "Please respond with a single JSON object as specified."
                )
                continue

        action = data.get("action", "final_answer")

        # Final answer — parse and return
        if action == "final_answer":
            result = _parse_diagnosis(response_str, report)
            result.reasoning_trace = reasoning_trace
            return result

        # Tool call — dispatch and continue
        if action == "call_tool":
            tool_name = data.get("tool", "")
            tool_params = data.get("params", {})
            tool_reason = data.get("reason", "")

            logger.info(f"Tool call: {tool_name}({tool_params}) — {tool_reason}")

            try:
                tool_result = await _dispatch_tool(tool_name, tool_params)
            except Exception as e:
                logger.error(f"Tool error: {e}")
                tool_result = f"Error calling {tool_name}: {e}"

            # Send tool result back on the same thread
            user_message = (
                f"## Tool Result: {tool_name}\n\n"
                f"{tool_result}\n\n"
                "Continue your diagnosis. Respond with JSON."
            )
            continue

        # Unknown action — maybe the agent used tool name as action
        # e.g. {"action": "get_source_code", "params": {...}}
        known_tools = {"get_source_code", "get_job_runbook", "get_full_log"}
        if action in known_tools:
            logger.info(f"Agent used tool name as action, treating as call_tool: {action}")
            tool_params = data.get("params", {})
            tool_reason = data.get("reason", "")
            try:
                tool_result = await _dispatch_tool(action, tool_params)
            except Exception as e:
                logger.error(f"Tool error: {e}")
                tool_result = f"Error calling {action}: {e}"

            user_message = (
                f"## Tool Result: {action}\n\n"
                f"{tool_result}\n\n"
                "Continue your diagnosis. Respond with JSON."
            )
            continue

        logger.warning(f"Unknown action in response: {action}")
        user_message = (
            "Your response had an unrecognized action. "
            "Please respond with either {\"action\": \"call_tool\", ...} "
            "or {\"action\": \"final_answer\", \"diagnosis\": {...}}."
        )

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
