"""
Shared Airflow API authentication and HTTP client utilities.

Auth flow: POST /auth/token with credentials → JWT Bearer token.
Used by both the ingestion client (DAG status) and the log tool (task logs).
"""

from __future__ import annotations

import logging

import httpx

from bau.config import settings

logger = logging.getLogger(__name__)


async def get_token(client: httpx.AsyncClient) -> str:
    """Authenticate with Airflow and return a JWT Bearer token."""
    base = settings.airflow_base_url.rstrip("/")
    token_url = f"{base}/auth/token"

    payload = {
        "username": settings.airflow_username,
        "password": settings.airflow_password,
    }

    resp = await client.post(
        token_url,
        json=payload,
        auth=(settings.airflow_username, settings.airflow_password),
        headers={"Content-Type": "application/json"},
        timeout=10.0,
    )
    resp.raise_for_status()

    token_data = resp.json()
    token = token_data.get("access_token")
    if not token:
        raise RuntimeError(f"Airflow token endpoint returned no access_token: {token_data}")
    return token


async def api_get(client: httpx.AsyncClient, headers: dict, endpoint: str) -> dict | None:
    """Perform a GET request to the Airflow API.

    Args:
        client: httpx async client (caller manages lifecycle).
        headers: Auth headers (Authorization: Bearer ...).
        endpoint: Relative endpoint (e.g. 'api/v2/dags/...').

    Returns:
        Parsed JSON dict, or None on error.
    """
    base = settings.airflow_base_url.rstrip("/")
    url = f"{base}/{endpoint}"

    resp = await client.get(url, headers=headers, timeout=30.0)

    if resp.status_code == 200:
        return resp.json()

    if resp.status_code in (401, 403):
        logger.error(f"Airflow auth failed ({resp.status_code}) for {endpoint}")
    elif resp.status_code == 404:
        logger.debug(f"Airflow 404 for {endpoint}")
    else:
        logger.warning(f"Airflow API {resp.status_code} for {endpoint}: {resp.text[:200]}")

    return None


def make_auth_headers(token: str) -> dict:
    """Build standard auth headers for Airflow API calls."""
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
