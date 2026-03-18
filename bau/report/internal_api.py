"""
Internal API client — pushes diagnosis reports to internal systems.

Currently mocked. Will be replaced with real API integration in Phase 4.
"""

from __future__ import annotations

import logging

import httpx

from bau.config import settings

logger = logging.getLogger(__name__)


async def push_report(payload: dict) -> dict:
    """
    Push a diagnosis report to the internal report API.

    Args:
        payload: The report payload from generator.generate_report_payload().

    Returns:
        The API response as a dict.
    """
    url = settings.internal_report_api_url
    token = settings.internal_report_api_token

    async with httpx.AsyncClient() as client:
        resp = await client.post(
            url,
            json=payload,
            headers={"Authorization": f"Bearer {token}"},
            timeout=30.0,
        )
        resp.raise_for_status()
        return resp.json()


async def send_message(target: str, message: str) -> dict:
    """
    Send a notification message via internal messaging API.

    Args:
        target: The recipient (user ID, team name, channel).
        message: The message content.

    Returns:
        The API response as a dict.
    """
    url = settings.internal_msg_api_url
    token = settings.internal_msg_api_token

    async with httpx.AsyncClient() as client:
        resp = await client.post(
            url,
            json={"target": target, "message": message},
            headers={"Authorization": f"Bearer {token}"},
            timeout=30.0,
        )
        resp.raise_for_status()
        return resp.json()
