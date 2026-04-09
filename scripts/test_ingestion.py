"""
Standalone test for the Airflow ingestion client.

Usage:
    python3 scripts/test_ingestion.py                  # uses today (SGT)
    python3 scripts/test_ingestion.py 2026-04-08       # specific grass_date
"""

import asyncio
import sys

from dotenv import load_dotenv

load_dotenv()

from bau.ingestion.airflow_client import fetch_dag_status


async def main(grass_date: str | None) -> None:
    print(f"Fetching DAG status for grass_date={grass_date or 'today (SGT)'}...")
    result = await fetch_dag_status(grass_date=grass_date)

    print()
    print("=" * 60)
    print(f"  not_found_dags : {len(result.not_found_dags)}")
    print(f"  failed_reports : {len(result.failed_reports)}")
    print(f"  running_reports: {len(result.running_reports)}")
    print("=" * 60)

    if result.not_found_dags:
        print("\n--- Not found in scheduler ---")
        for d in result.not_found_dags:
            print(f"  * {d.dag_id}")

    if result.failed_reports:
        print("\n--- Failed reports ---")
        for r in result.failed_reports:
            print(f"  * {r.dag_id} / {r.dag_run_id}")
            print(f"      failed_tasks    : {r.failed_tasks}")
            print(f"      upstream_failed : {r.upstream_failed_tasks}")
            print(f"      root_cause      : {r.root_cause_tasks}")
            print(f"      duration        : {r.duration}  avg={r.avg_duration}")
            print(f"      url             : {r.dag_run_url}")

    if result.running_reports:
        print("\n--- Running reports ---")
        for r in result.running_reports:
            print(f"  * {r.dag_id} / {r.dag_run_id}")
            print(f"      running_tasks   : {r.running_tasks}")
            print(f"      pending_tasks   : {r.pending_tasks}")
            print(f"      duration        : {r.duration}  avg={r.avg_duration}")
            print(f"      is_overtime     : {r.is_overtime}")


if __name__ == "__main__":
    grass_date = sys.argv[1] if len(sys.argv) > 1 else None
    asyncio.run(main(grass_date))
