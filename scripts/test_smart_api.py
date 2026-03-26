"""
Test SMART Platform API connectivity with a minimal call.

Usage:
    python scripts/test_smart_api.py

Requires in .env:
    SMART_HASH_ID, SMART_ENDPOINT_KEY
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parent.parent / ".env")

from bau.config import settings


async def main():
    print("BAU-Assistant — SMART Platform API Test")
    print("=" * 60)

    if not settings.smart_hash_id:
        print("  SKIP — SMART_HASH_ID not set in .env")
        return
    if not settings.smart_endpoint_key:
        print("  SKIP — SMART_ENDPOINT_KEY not set in .env")
        return

    print(f"  API URL:  {settings.smart_api_url}")
    print(f"  Hash ID:  {settings.smart_hash_id}")
    print(f"  User ID:  {settings.smart_user_id}")
    print()

    from bau.analysis.smart_client import invoke

    try:
        result = await invoke(
            message='Respond with exactly this JSON and nothing else: {"action": "final_answer", "diagnosis": {"root_cause_category": "unknown", "confidence": 0.0, "evidence": [], "suggested_actions": [], "summary": "API test"}}',
        )
        print(f"  Response:    {result['response_str'][:200]}")
        print(f"  Thread ID:   {result['thread_id']}")
        print(f"  Interrupted: {result['is_interrupted']}")
        print("  RESULT:      PASS")
    except Exception as e:
        print(f"  Error: {e}")
        print("  RESULT:      FAIL")


if __name__ == "__main__":
    asyncio.run(main())
