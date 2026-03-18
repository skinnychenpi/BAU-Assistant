"""
Log router — decides which log source to use and dispatches accordingly.

Reads dag_config.yaml to determine task type (spark/bash/python),
then routes to airflow_log_tool or yarn_log_tool.
"""

from __future__ import annotations

from bau.analysis.tools.airflow_log_tool import ProcessedLog, get_airflow_log
from bau.analysis.tools.yarn_log_tool import get_yarn_log
from bau.knowledge.loader import get_task_info


async def get_task_log(
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    config: dict | None = None,
) -> ProcessedLog:
    """
    Route to the correct log source based on task type in dag_config.yaml.

    - type: spark  → fetch from YARN using yarn_app_name
    - type: bash/python (or unknown) → fetch from Airflow

    Args:
        dag_id: The DAG identifier.
        dag_run_id: The DAG run identifier.
        task_id: The task identifier.
        config: Optional override for DAG config (useful for testing).
    """
    task_info = get_task_info(dag_id, task_id, config)
    task_type = task_info.get("type", "bash")

    if task_type == "spark":
        yarn_app_name = task_info.get("yarn_app_name")
        if not yarn_app_name:
            return ProcessedLog(
                raw_log="",
                exception=f"No yarn_app_name configured for {dag_id}.{task_id}",
                source="yarn",
            )
        return await get_yarn_log(yarn_app_name)

    # Default: bash, python, or any other type → Airflow
    return await get_airflow_log(dag_id, dag_run_id, task_id)
