"""
Test Airflow API connectivity and log fetching.

Usage:
    python scripts/test_airflow_api.py

Requires in .env:
    AIRFLOW_BASE_URL, AIRFLOW_USERNAME, AIRFLOW_PASSWORD
"""

from __future__ import annotations

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parent.parent / ".env")

import httpx
from bau.config import settings


async def test_airflow_connection():
    """Test 1: Can we reach Airflow at all?"""
    print("=" * 60)
    print("TEST 1: Airflow API connectivity")
    print("=" * 60)
    base = settings.airflow_base_url.rstrip("/")
    if not base:
        print("SKIP — AIRFLOW_BASE_URL not set in .env")
        return False

    url = f"{base}/api/v1/health"
    print(f"  URL:      {url}")
    print(f"  Username: {settings.airflow_username}")

    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get(
                url,
                auth=(settings.airflow_username, settings.airflow_password),
                timeout=10.0,
            )
            print(f"  Status:   {resp.status_code}")
            print(f"  Body:     {resp.text[:500]}")
            if resp.status_code == 200:
                print("  RESULT:   PASS")
                return True
            else:
                print("  RESULT:   FAIL")
                return False
    except Exception as e:
        print(f"  Error:    {e}")
        print("  RESULT:   FAIL")
        return False


async def test_list_dag_runs():
    """Test 2: Can we list DAG runs?"""
    print("\n" + "=" * 60)
    print("TEST 2: List DAG runs")
    print("=" * 60)
    base = settings.airflow_base_url.rstrip("/")
    dag_id = "data_data_pltingestion_gxx_ftp_all"
    url = f"{base}/api/v1/dags/{dag_id}/dagRuns?limit=3&order_by=-execution_date"
    print(f"  DAG:  {dag_id}")
    print(f"  URL:  {url}")

    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get(
                url,
                auth=(settings.airflow_username, settings.airflow_password),
                timeout=15.0,
            )
            print(f"  Status: {resp.status_code}")
            if resp.status_code == 200:
                data = resp.json()
                runs = data.get("dag_runs", [])
                print(f"  Found {len(runs)} recent runs:")
                for run in runs:
                    print(f"    - {run['dag_run_id']}  state={run['state']}")
                print("  RESULT: PASS")
                return runs
            else:
                print(f"  Body: {resp.text[:500]}")
                print("  RESULT: FAIL")
                return []
    except Exception as e:
        print(f"  Error: {e}")
        print("  RESULT: FAIL")
        return []


async def test_fetch_log():
    """Test 3: Can we fetch a task log?"""
    print("\n" + "=" * 60)
    print("TEST 3: Fetch task log")
    print("=" * 60)
    base = settings.airflow_base_url.rstrip("/")
    dag_id = "data_data_pltingestion_gxx_ftp_all"
    dag_run_id = "scheduled__2026-03-17T20:44:00+00:00"
    task_id = "gxx_auth_log_collect"
    try_number = -1

    url = f"{base}/api/v1/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/logs/{try_number}"
    print(f"  DAG:     {dag_id}")
    print(f"  Run:     {dag_run_id}")
    print(f"  Task:    {task_id}")
    print(f"  URL:     {url}")

    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get(
                url,
                auth=(settings.airflow_username, settings.airflow_password),
                headers={"Accept": "text/plain"},
                timeout=30.0,
            )
            print(f"  Status:  {resp.status_code}")
            if resp.status_code == 200:
                log_text = resp.text
                print(f"  Log size: {len(log_text)} chars, {len(log_text.splitlines())} lines")
                print(f"  First 200 chars:\n    {log_text[:200]}")
                print(f"  Last 200 chars:\n    {log_text[-200:]}")
                print("  RESULT:  PASS")
                return log_text
            else:
                print(f"  Body: {resp.text[:500]}")
                print("  RESULT:  FAIL")
                return None
    except Exception as e:
        print(f"  Error: {e}")
        print("  RESULT:  FAIL")
        return None


async def test_log_processing(raw_log: str | None):
    """Test 4: Does our log processor extract the right info?"""
    print("\n" + "=" * 60)
    print("TEST 4: Log processing")
    print("=" * 60)

    if raw_log is None:
        # Fall back to fixture file
        fixture = Path(__file__).resolve().parent.parent / "tests/fixtures/sample_airflow_log_ftp_timeout.txt"
        if fixture.exists():
            raw_log = fixture.read_text()
            print(f"  Using fixture file: {fixture.name}")
        else:
            print("  SKIP — no log available and no fixture file")
            return

    from bau.analysis.tools.airflow_log_tool import process_log
    result = process_log(raw_log, source="airflow")

    print(f"  Exception:    {result.exception}")
    print(f"  Stack trace:  {'Yes' if result.stack_trace else 'No'}"
          f" ({len(result.stack_trace) if result.stack_trace else 0} chars)")
    print(f"  Tail lines:   {len(result.tail_lines)}")
    if result.stack_trace:
        print(f"  Stack trace preview:\n    {result.stack_trace[:300]}")
    print("  RESULT:  PASS")


async def main():
    print("BAU-Assistant — Airflow API Test Suite")
    print(f"  AIRFLOW_BASE_URL: {settings.airflow_base_url or '(not set)'}")
    print()

    ok = await test_airflow_connection()
    runs = []
    raw_log = None
    if ok:
        runs = await test_list_dag_runs()
        raw_log = await test_fetch_log()

    await test_log_processing(raw_log)

    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    if not settings.airflow_base_url:
        print("  Set AIRFLOW_BASE_URL in .env to test API connectivity")
    elif ok:
        print("  Airflow API: reachable")
        print(f"  DAG runs found: {len(runs)}")
        print(f"  Log fetch: {'OK' if raw_log else 'FAILED'}")
    else:
        print("  Airflow API: NOT reachable — check URL, credentials, and network")


if __name__ == "__main__":
    asyncio.run(main())
