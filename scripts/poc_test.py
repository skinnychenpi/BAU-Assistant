"""
POC Test Script — Diagnosis Agent with Real Airflow Log

Reads a log file (or fetches from Airflow API), constructs a FailedTaskReport,
runs the AI agent, and prints the diagnosis.

Usage:
    # From file (no API calls needed):
    python -m scripts.poc_test --from-file tests/fixtures/sample_airflow_log_ftp_timeout.txt

    # From Airflow API:
    python -m scripts.poc_test --from-api

Requires ANTHROPIC_API_KEY in .env (or environment).
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

# Add project root to path so imports work when run as script
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

from dotenv import load_dotenv
load_dotenv(project_root / ".env")

from bau.analysis.agent import diagnose
from bau.analysis.tools.airflow_log_tool import ProcessedLog, process_log
from bau.state.models import FailedTaskReport

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("poc_test")

# ── Test case parameters ─────────────────────────────────────────────────────

TEST_CASE = {
    "dag_id": "data_data_pltingestion_gxx_ftp_all",
    "dag_run_id": "scheduled__2026-03-17T20:44:00+00:00",
    "task_id": "gxx_auth_log_collect",
    "scheduled_time": datetime(2026, 3, 17, 20, 44, 0, tzinfo=timezone.utc),
    "start_date": datetime(2026, 3, 18, 4, 48, 18, tzinfo=timezone(timedelta(hours=8))),
    "duration": timedelta(minutes=5, seconds=4),
    "avg_duration": timedelta(minutes=3, seconds=54),
}

DEFAULT_LOG_FILE = "tests/fixtures/sample_airflow_log_ftp_timeout.txt"


def build_report() -> FailedTaskReport:
    """Build a FailedTaskReport for the test case."""
    tc = TEST_CASE
    return FailedTaskReport(
        dag_id=tc["dag_id"],
        dag_run_id=tc["dag_run_id"],
        scheduled_time=tc["scheduled_time"],
        start_date=tc["start_date"],
        duration=tc["duration"],
        avg_duration=tc["avg_duration"],
        is_overtime=tc["duration"] > tc["avg_duration"] * 1.5,
        failed_tasks=[tc["task_id"]],
        upstream_failed_tasks=[],
        root_cause_tasks=[tc["task_id"]],
    )


def load_log_from_file(path: str) -> ProcessedLog:
    """Load and process a log from a local file."""
    log_path = Path(path)
    if not log_path.is_absolute():
        log_path = project_root / log_path
    raw_log = log_path.read_text(encoding="utf-8")
    logger.info(f"Loaded log from {log_path} ({len(raw_log)} chars)")
    return process_log(raw_log, source="airflow")


async def fetch_log_from_api() -> ProcessedLog:
    """Fetch log from Airflow REST API."""
    from bau.analysis.tools.airflow_log_tool import get_airflow_log
    tc = TEST_CASE
    logger.info(f"Fetching log from Airflow API for {tc['dag_id']}/{tc['task_id']}...")
    return await get_airflow_log(tc["dag_id"], tc["dag_run_id"], tc["task_id"])


def print_diagnosis(result) -> None:
    """Pretty-print the diagnosis result."""
    print("\n" + "=" * 70)
    print("DIAGNOSIS RESULT")
    print("=" * 70)
    print(f"  DAG:              {result.dag_id}")
    print(f"  Run:              {result.dag_run_id}")
    print(f"  Root Cause:       {result.root_cause_category}")
    print(f"  Confidence:       {result.confidence:.0%}")
    print(f"  Evidence:")
    for i, e in enumerate(result.evidence, 1):
        print(f"    {i}. {e}")
    print(f"  Suggested Actions:")
    for a in result.suggested_actions:
        print(f"    - [{a.action_type}] {a.reason}")
        if a.params:
            print(f"      params: {json.dumps(a.params, indent=2)}")
    print(f"  Agent Steps:      {len(result.reasoning_trace)}")
    print("=" * 70)


async def main():
    parser = argparse.ArgumentParser(description="POC: Test AI diagnosis agent")
    source = parser.add_mutually_exclusive_group()
    source.add_argument(
        "--from-file",
        default=DEFAULT_LOG_FILE,
        help=f"Path to log file (default: {DEFAULT_LOG_FILE})",
    )
    source.add_argument(
        "--from-api",
        action="store_true",
        help="Fetch log from Airflow REST API instead of file",
    )
    args = parser.parse_args()

    # 1. Build the report
    report = build_report()
    logger.info(f"Test case: {report.dag_id} / {report.root_cause_tasks}")

    # 2. Get the log
    if args.from_api:
        log = await fetch_log_from_api()
    else:
        log = load_log_from_file(args.from_file)

    # Show what the log processor extracted
    print("\n--- Log Processing Summary ---")
    print(f"  Source:     {log.source}")
    print(f"  Exception:  {log.exception}")
    print(f"  Stack trace: {'Yes' if log.stack_trace else 'No'} "
          f"({len(log.stack_trace) if log.stack_trace else 0} chars)")
    print(f"  Tail lines: {len(log.tail_lines)}")
    print("---\n")

    # 3. Run diagnosis (no historical issues for POC)
    historical_issues = "No historical issues found for this DAG."
    logger.info("Starting AI diagnosis...")
    result = await diagnose(report, log, historical_issues)

    # 4. Print results
    print_diagnosis(result)


if __name__ == "__main__":
    asyncio.run(main())
