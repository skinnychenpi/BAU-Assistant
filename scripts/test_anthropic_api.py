"""
Test Anthropic API connectivity with a minimal call.

Usage:
    python scripts/test_anthropic_api.py

Requires in .env:
    ANTHROPIC_API_KEY
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parent.parent / ".env")

import anthropic
from bau.config import settings


def main():
    print("BAU-Assistant — Anthropic API Test")
    print("=" * 60)

    key = settings.anthropic_api_key
    if not key:
        print("  SKIP — ANTHROPIC_API_KEY not set in .env")
        return

    print(f"  API key: {key[:12]}...{key[-4:]}")
    print(f"  Model:   {settings.anthropic_model}")
    print()

    client = anthropic.Anthropic(api_key=key)

    try:
        resp = client.messages.create(
            model=settings.anthropic_model,
            max_tokens=50,
            messages=[{"role": "user", "content": "Say 'API connection OK' and nothing else."}],
        )
        text = resp.content[0].text
        print(f"  Response: {text}")
        print(f"  Usage:    input={resp.usage.input_tokens}, output={resp.usage.output_tokens}")
        print("  RESULT:   PASS")
    except anthropic.AuthenticationError as e:
        print(f"  Auth error: {e}")
        print("  RESULT:   FAIL — check your API key")
    except anthropic.BadRequestError as e:
        print(f"  Bad request: {e}")
        if "credit balance" in str(e):
            print("  RESULT:   FAIL — no API credits. Add credits at console.anthropic.com")
        else:
            print("  RESULT:   FAIL")
    except Exception as e:
        print(f"  Error: {e}")
        print("  RESULT:   FAIL")


if __name__ == "__main__":
    main()
