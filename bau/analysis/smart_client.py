"""
SMART Platform API client.

Wraps the SMART endpoint deployment API for the BAU diagnosis agent.
Uses thread-based conversation for multi-turn tool-use loops.
"""

from __future__ import annotations

import logging

import httpx

from bau.config import settings

logger = logging.getLogger(__name__)


async def invoke(
    message: str,
    thread_id: str | None = None,
    mode: str = "sync",
) -> dict:
    """
    Send a message to the SMART agent and return the response.

    Args:
        message: The input text to send.
        thread_id: Optional thread ID to continue a conversation.
        mode: "sync" (wait for response) or "async" (poll later).

    Returns:
        dict with keys: response_str, thread_id, is_interrupted, raw_response
    """
    payload = {
        "endpoint_deployment_hash_id": settings.smart_hash_id,
        "endpoint_deployment_key": settings.smart_endpoint_key,
        "user_id": settings.smart_user_id,
        "message": {
            "input_str": message,
        },
        "mode": mode,
    }
    if thread_id:
        payload["message"]["thread_id"] = thread_id

    logger.info(f"SMART API call (thread={thread_id or 'new'}, msg_len={len(message)})")

    async with httpx.AsyncClient() as client:
        resp = await client.post(
            settings.smart_api_url,
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=120.0,
        )
        resp.raise_for_status()
        data = resp.json()

    response_data = data.get("data", {})
    response = response_data.get("response", {})

    result = {
        "response_str": response.get("response_str", ""),
        "thread_id": response.get("thread_id", ""),
        "is_interrupted": response_data.get("is_interrupted", False),
        "raw_response": data,
    }

    logger.info(f"SMART API response (thread={result['thread_id']}, "
                f"len={len(result['response_str'])})")

    return result
