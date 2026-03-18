"""
Orchestrator — the main pipeline loop.

Guided approach:
  1. Fetch emails → parse into FailedTaskReports
  2. For each report, pre-fetch log + historical issues
  3. Run agent diagnosis
  4. Generate report and save actions
  5. Transition to AWAIT_HUMAN

Can be triggered manually via CLI or on schedule via APScheduler.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from datetime import datetime

from bau.analysis.agent import diagnose
from bau.analysis.tools.history_tool import get_historical_issues
from bau.analysis.tools.log_router import get_task_log
from bau.config import settings
from bau.report.generator import format_report_text, generate_report_payload
from bau.state.models import AgentRun
from bau.state.store import (
    create_run,
    init_db,
    save_action,
    save_diagnosis,
    update_run_status,
)

logger = logging.getLogger(__name__)


async def run_pipeline(reports=None) -> str:
    """
    Run the full BAU diagnosis pipeline.

    Args:
        reports: Optional pre-parsed FailedTaskReports (for testing).
                 If None, will fetch from Gmail (not yet implemented).

    Returns:
        The run_id for this pipeline execution.
    """
    init_db()

    run_id = str(uuid.uuid4())
    agent_run = AgentRun(
        run_id=run_id,
        triggered_at=datetime.utcnow(),
        status="IDLE",
    )
    create_run(agent_run)

    try:
        # ── FETCH ─────────────────────────────────────────────────────
        update_run_status(run_id, "FETCH")
        logger.info(f"[{run_id[:8]}] FETCH — getting failure reports")

        if reports is None:
            # TODO: Implement Gmail fetching
            # from bau.ingestion.gmail_client import fetch_emails
            # from bau.ingestion.email_parser import parse_email_body
            # emails = fetch_emails()
            # reports = []
            # for email in emails:
            #     reports.extend(parse_email_body(email.body))
            logger.info("No reports provided and Gmail client not yet implemented")
            update_run_status(run_id, "DONE")
            return run_id

        if not reports:
            logger.info(f"[{run_id[:8]}] No failures found — done")
            update_run_status(run_id, "DONE")
            return run_id

        # ── ANALYZE ───────────────────────────────────────────────────
        update_run_status(run_id, "ANALYZE")
        logger.info(f"[{run_id[:8]}] ANALYZE — diagnosing {len(reports)} report(s)")

        for report in reports:
            logger.info(f"[{run_id[:8]}] Diagnosing {report.dag_id} / {report.root_cause_tasks}")

            # Pre-fetch log for first root cause task
            task_id = report.root_cause_tasks[0] if report.root_cause_tasks else report.failed_tasks[0]
            log = await get_task_log(report.dag_id, report.dag_run_id, task_id)

            # Pre-fetch historical issues
            historical = get_historical_issues(report.dag_id)

            # Run agent diagnosis
            diagnosis = await diagnose(report, log, historical)

            # ── REPORT ────────────────────────────────────────────────
            logger.info(
                f"[{run_id[:8]}] Diagnosis: {diagnosis.root_cause_category} "
                f"(confidence: {diagnosis.confidence:.0%})"
            )

            # Save diagnosis to history
            save_diagnosis(
                dag_id=diagnosis.dag_id,
                run_id=diagnosis.dag_run_id,
                category=diagnosis.root_cause_category,
            )

            # Save actions for human approval
            for action in diagnosis.suggested_actions:
                save_action(run_id, action)

            # Print human-readable report
            report_text = format_report_text(report, diagnosis)
            logger.info(f"\n{report_text}")

        # ── AWAIT_HUMAN ───────────────────────────────────────────────
        update_run_status(run_id, "AWAIT_HUMAN")
        logger.info(f"[{run_id[:8]}] AWAIT_HUMAN — waiting for approval")

    except Exception as e:
        logger.error(f"[{run_id[:8]}] Pipeline failed: {e}", exc_info=True)
        update_run_status(run_id, "FAILED")

    return run_id
