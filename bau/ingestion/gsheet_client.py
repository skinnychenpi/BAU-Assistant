"""
Google Sheets client for fetching historical issues.

Reads from a Google Sheet with columns:
  Issue Date | Issue Description | Logs | Fix Action
"""

from __future__ import annotations

from dataclasses import dataclass

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

from bau.config import settings

SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]


@dataclass
class HistoricalIssue:
    issue_date: str
    description: str
    logs: str
    fix_action: str


def _get_sheets_service():
    """Build Google Sheets API service."""
    creds = Credentials.from_service_account_file(
        settings.gsheet_credentials_path,
        scopes=SCOPES,
    )
    return build("sheets", "v4", credentials=creds)


def fetch_all_issues(
    spreadsheet_id: str | None = None,
    sheet_name: str | None = None,
) -> list[HistoricalIssue]:
    """
    Fetch all rows from the historical issues Google Sheet.

    Expected columns (A-D):
      A: Issue Date
      B: Issue Description
      C: Logs
      D: Fix Action

    Args:
        spreadsheet_id: Override for the spreadsheet ID.
        sheet_name: Override for the sheet name.
    """
    sid = spreadsheet_id or settings.gsheet_spreadsheet_id
    sname = sheet_name or settings.gsheet_sheet_name

    service = _get_sheets_service()
    result = (
        service.spreadsheets()
        .values()
        .get(spreadsheetId=sid, range=f"{sname}!A2:D")
        .execute()
    )
    rows = result.get("values", [])

    issues: list[HistoricalIssue] = []
    for row in rows:
        # Pad short rows with empty strings
        padded = row + [""] * (4 - len(row))
        issues.append(
            HistoricalIssue(
                issue_date=padded[0],
                description=padded[1],
                logs=padded[2],
                fix_action=padded[3],
            )
        )
    return issues


def fetch_issues_for_dag(dag_id: str, **kwargs) -> list[HistoricalIssue]:
    """Filter historical issues to those mentioning a specific DAG ID."""
    all_issues = fetch_all_issues(**kwargs)
    return [
        issue
        for issue in all_issues
        if dag_id in issue.description or dag_id in issue.logs
    ]
