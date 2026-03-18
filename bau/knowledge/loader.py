"""
Loads DAG configuration from the YAML file.
"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml

from bau.config import settings


@lru_cache(maxsize=1)
def load_dag_config(path: str | None = None) -> dict[str, Any]:
    """Load and cache the DAG config YAML. Returns empty dict if file missing."""
    config_path = Path(path or settings.dag_config_path)
    if not config_path.exists():
        return {}
    with open(config_path) as f:
        data = yaml.safe_load(f)
    return data or {}


def get_dag_info(dag_id: str, config: dict[str, Any] | None = None) -> dict[str, Any]:
    """Get configuration for a specific DAG. Returns empty dict if not found."""
    cfg = config if config is not None else load_dag_config()
    return cfg.get(dag_id, {})


def get_task_info(dag_id: str, task_id: str, config: dict[str, Any] | None = None) -> dict[str, Any]:
    """Get configuration for a specific task within a DAG."""
    dag_info = get_dag_info(dag_id, config)
    tasks = dag_info.get("tasks", {})
    return tasks.get(task_id, {})
