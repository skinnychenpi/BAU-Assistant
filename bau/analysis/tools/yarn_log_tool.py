"""
Fetch task logs from YARN ResourceManager REST API.

Single responsibility: talks to YARN only.
Used for Spark cluster-mode tasks whose logs are not in Airflow.

NOTE: YARN ResourceManager API access is pending infra team confirmation.
"""

from __future__ import annotations

import httpx

from bau.analysis.tools.airflow_log_tool import ProcessedLog, process_log
from bau.config import settings


async def _find_application_id(app_name: str) -> str | None:
    """
    Query YARN ResourceManager to find application ID by application name.
    Searches recent FINISHED and FAILED applications.
    """
    base = settings.yarn_resourcemanager_url.rstrip("/")
    url = f"{base}/ws/v1/cluster/apps"

    async with httpx.AsyncClient() as client:
        resp = await client.get(
            url,
            params={"applicationTypes": "SPARK", "limit": "100"},
            headers={"Accept": "application/json"},
            timeout=30.0,
        )
        resp.raise_for_status()
        data = resp.json()

    apps = data.get("apps", {}).get("app", [])
    for app in apps:
        if app.get("name") == app_name:
            return app.get("id")
    return None


async def _fetch_yarn_log(application_id: str) -> str:
    """Fetch stdout log content for a YARN application."""
    base = settings.yarn_resourcemanager_url.rstrip("/")
    # Use the YARN log aggregation endpoint
    url = f"{base}/ws/v1/cluster/apps/{application_id}/logs"

    async with httpx.AsyncClient() as client:
        resp = await client.get(
            url,
            headers={"Accept": "text/plain"},
            timeout=60.0,
        )
        resp.raise_for_status()
        return resp.text


async def get_yarn_log(yarn_app_name: str) -> ProcessedLog:
    """
    Fetch Spark application log from YARN by application name.

    Args:
        yarn_app_name: The Spark application name (as specified in spark config).
    """
    app_id = await _find_application_id(yarn_app_name)
    if not app_id:
        return ProcessedLog(
            raw_log="",
            exception=f"YARN application not found: {yarn_app_name}",
            source="yarn",
        )

    raw_log = await _fetch_yarn_log(app_id)
    return process_log(raw_log, source="yarn")
