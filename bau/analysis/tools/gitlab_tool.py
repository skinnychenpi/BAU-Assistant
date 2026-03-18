"""
Fetch source code from self-hosted GitLab.

Returns both the task source file and the DAG definition file,
giving the agent context about what the code does and how tasks relate.
"""

from __future__ import annotations

from dataclasses import dataclass

import httpx

from bau.config import settings


@dataclass
class SourceCodeResult:
    task_code: str | None = None
    task_file_path: str | None = None
    dag_code: str | None = None
    dag_file_path: str | None = None
    error: str | None = None


async def _fetch_file(project: str, file_path: str, ref: str = "main") -> str | None:
    """Fetch a single file from GitLab API."""
    base = settings.gitlab_base_url.rstrip("/")
    # GitLab API requires URL-encoded project path and file path
    encoded_project = project.replace("/", "%2F")
    encoded_path = file_path.replace("/", "%2F")
    url = f"{base}/api/v4/projects/{encoded_project}/repository/files/{encoded_path}/raw"

    async with httpx.AsyncClient() as client:
        resp = await client.get(
            url,
            params={"ref": ref},
            headers={"PRIVATE-TOKEN": settings.gitlab_token},
            timeout=30.0,
        )
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        return resp.text


async def _search_file(project: str, filename: str, ref: str = "main") -> str | None:
    """Search for a file by name in the GitLab repository."""
    base = settings.gitlab_base_url.rstrip("/")
    encoded_project = project.replace("/", "%2F")
    url = f"{base}/api/v4/projects/{encoded_project}/repository/tree"

    async with httpx.AsyncClient() as client:
        resp = await client.get(
            url,
            params={"ref": ref, "search": filename, "recursive": "true"},
            headers={"PRIVATE-TOKEN": settings.gitlab_token},
            timeout=30.0,
        )
        if resp.status_code != 200:
            return None
        items = resp.json()
        for item in items:
            if item.get("name") == filename or item.get("path", "").endswith(filename):
                return item.get("path")
    return None


async def get_source_code(
    dag_id: str,
    task_id: str,
    project: str | None = None,
    ref: str = "main",
) -> SourceCodeResult:
    """
    Fetch task source code and DAG definition from GitLab.

    Searches for files matching the dag_id and task_id patterns.
    Returns both the task file and the DAG definition file.

    Args:
        dag_id: The DAG identifier (used to find DAG definition file).
        task_id: The task identifier (used to find task source file).
        project: GitLab project path. Defaults to config value.
        ref: Git branch/tag. Defaults to "main".
    """
    proj = project or settings.gitlab_default_project
    result = SourceCodeResult()

    # Try to find the DAG definition file (usually named after dag_id)
    dag_filename = f"{dag_id}.py"
    dag_path = await _search_file(proj, dag_filename, ref)
    if dag_path:
        result.dag_file_path = dag_path
        result.dag_code = await _fetch_file(proj, dag_path, ref)

    # Try to find the task source file
    # Task files often follow patterns like: tasks/<task_id>.py or <dag_id>/<task_id>.py
    task_filename = f"{task_id}.py"
    task_path = await _search_file(proj, task_filename, ref)
    if task_path:
        result.task_file_path = task_path
        result.task_code = await _fetch_file(proj, task_path, ref)

    if not result.dag_code and not result.task_code:
        result.error = f"Could not find source files for dag={dag_id}, task={task_id}"

    return result
