"""
Fetch job runbook from Confluence via DAG config YAML mapping.

Flow: dag_id → dag_config.yaml → confluence_url → Confluence REST API → clean text
"""

from __future__ import annotations

import re
from html import unescape

import httpx

from bau.config import settings
from bau.knowledge.loader import get_dag_info

MAX_CONTENT_LENGTH = 8000  # chars — keep token budget manageable


def _extract_page_id(confluence_url: str) -> str | None:
    """Extract page ID from various Confluence URL formats."""
    # Format: /pages/12345 or /pages/viewpage.action?pageId=12345
    m = re.search(r"pageId=(\d+)", confluence_url)
    if m:
        return m.group(1)
    m = re.search(r"/pages/(\d+)", confluence_url)
    if m:
        return m.group(1)
    return None


def _clean_html(html_content: str) -> str:
    """Strip HTML tags and clean up Confluence content to plain text."""
    # Remove script/style blocks
    text = re.sub(r"<(script|style)[^>]*>.*?</\1>", "", html_content, flags=re.DOTALL)
    # Replace <br>, <p>, <div>, <li> with newlines
    text = re.sub(r"<br\s*/?>", "\n", text)
    text = re.sub(r"</(p|div|li|tr|h[1-6])>", "\n", text)
    # Remove remaining tags
    text = re.sub(r"<[^>]+>", "", text)
    # Decode HTML entities
    text = unescape(text)
    # Collapse multiple blank lines
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


async def get_job_runbook(dag_id: str, config: dict | None = None) -> str:
    """
    Fetch the Confluence runbook page for a DAG.

    Looks up the confluence_url from dag_config.yaml, fetches the page
    content via Confluence REST API, and returns cleaned text.

    Args:
        dag_id: The DAG identifier.
        config: Optional override for DAG config (useful for testing).
    """
    dag_info = get_dag_info(dag_id, config)
    confluence_url = dag_info.get("confluence_url")

    if not confluence_url:
        return f"No Confluence runbook configured for DAG: {dag_id}"

    page_id = _extract_page_id(confluence_url)
    if not page_id:
        return f"Could not extract page ID from URL: {confluence_url}"

    base = settings.confluence_base_url.rstrip("/")
    url = f"{base}/rest/api/content/{page_id}"

    async with httpx.AsyncClient() as client:
        resp = await client.get(
            url,
            params={"expand": "body.storage"},
            auth=(settings.confluence_username, settings.confluence_token),
            timeout=30.0,
        )
        resp.raise_for_status()
        data = resp.json()

    title = data.get("title", "")
    html_body = data.get("body", {}).get("storage", {}).get("value", "")
    clean_text = _clean_html(html_body)

    # Truncate if too long
    if len(clean_text) > MAX_CONTENT_LENGTH:
        clean_text = clean_text[:MAX_CONTENT_LENGTH] + "\n\n[... truncated ...]"

    return f"# {title}\n\n{clean_text}"
