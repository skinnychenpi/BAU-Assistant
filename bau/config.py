"""
Central configuration — all settings come from environment variables.
Copy .env.example to .env and fill in your values.
"""

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )

    # ── Anthropic ──────────────────────────────────────────────────────────────
    anthropic_api_key: str
    anthropic_model: str = "claude-sonnet-4-20250514"
    agent_max_steps: int = 10

    # ── Gmail ──────────────────────────────────────────────────────────────────
    gmail_credentials_path: str = "./credentials/gmail_credentials.json"
    gmail_token_path: str = "./credentials/gmail_token.json"
    gmail_search_query: str = 'subject:"Airflow" after:2d'

    # ── Airflow ────────────────────────────────────────────────────────────────
    airflow_base_url: str
    airflow_username: str
    airflow_password: str

    # ── GitLab ─────────────────────────────────────────────────────────────────
    gitlab_base_url: str
    gitlab_token: str
    gitlab_default_project: str = "data/pipelines"

    # ── Internal APIs ──────────────────────────────────────────────────────────
    internal_report_api_url: str = "http://mock/api/reports"
    internal_report_api_token: str = "mock-token"
    internal_msg_api_url: str = "http://mock/api/messages"
    internal_msg_api_token: str = "mock-token"

    # ── Agent behaviour ────────────────────────────────────────────────────────
    overtime_threshold: float = 1.5


# Singleton — import this everywhere
settings = Settings()  # type: ignore[call-arg]
