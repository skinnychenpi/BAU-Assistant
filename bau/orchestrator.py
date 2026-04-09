"""
Orchestrator — the main pipeline loop.

Guided approach:
  1. Query Airflow API → build IngestionResult
  2. Report informational items (not-found DAGs, running DAGs)
  3. For each failed report, pre-fetch log + historical issues
  4. Run agent diagnosis
  5. Generate report and save actions
  6. Transition to AWAIT_HUMAN

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
from bau.ingestion.airflow_client import fetch_dag_status
from bau.report.generator import (
    format_full_summary,
    format_not_found_report,
    format_report_text,
    format_running_report,
    generate_report_payload,
)
from bau.state.models import AgentRun, IngestionResult
from bau.state.store import (
    create_run,
    init_db,
    save_action,
    save_diagnosis,
    update_run_status,
)

logger = logging.getLogger(__name__)


async def run_pipeline(
    dag_ids: list[str] | None = None,
    grass_date: str | None = None,
    ingestion_result: IngestionResult | None = None,
) -> str:
    """
    Run the full BAU diagnosis pipeline.

    Args:
        dag_ids: Optional list of DAG IDs to check. If None, loads from config.
        grass_date: Date to check (YYYY-MM-DD). Defaults to today (SGT).
        ingestion_result: Optional pre-built IngestionResult (for testing).

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
        logger.info(f"[{run_id[:8]}] FETCH — querying Airflow API")

        if ingestion_result is None:
            ingestion_result = await fetch_dag_status(
                dag_ids=dag_ids, grass_date=grass_date
            )

        # ── REPORT informational items ────────────────────────────────
        if ingestion_result.not_found_dags:
            report_text = format_not_found_report(ingestion_result.not_found_dags)
            logger.info(f"\n{report_text}")

        for running in ingestion_result.running_reports:
            report_text = format_running_report(running)
            logger.info(f"\n{report_text}")

        if not ingestion_result.failed_reports:
            logger.info(f"[{run_id[:8]}] No failed DAG runs found — done")
            update_run_status(run_id, "DONE")
            return run_id

        # ── ANALYZE ───────────────────────────────────────────────────
        update_run_status(run_id, "ANALYZE")
        reports = ingestion_result.failed_reports
        logger.info(f"[{run_id[:8]}] ANALYZE — diagnosing {len(reports)} failed report(s)")

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
