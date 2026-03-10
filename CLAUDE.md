# BAU-Assistant — Claude Code Context

## Project Goal
Automate the data engineer BAU (Business As Usual) workflow:
Read Airflow failure emails → Diagnose root cause → Generate report → Await human confirmation → Execute fix

## Tech Stack
- Python 3.11+
- Anthropic Claude API (Tool Use)
- Gmail API (read emails)
- Airflow REST API (fetch logs, trigger reruns)
- GitLab API (read job source code, self-hosted internal network)
- SQLite (state persistence)
- Internal REST API (push reports, send messages — mocked for now)

## Architecture Overview

```
Scheduler (cron)
    │
    ▼
Orchestrator (bau/orchestrator.py)
    │
    ├── Ingestion Pipeline
    │     ├── gmail_client.py     # Gmail API wrapper
    │     └── email_parser.py     # Pure regex parsing, no LLM
    │
    ├── Analysis Engine (LLM Core)
    │     ├── agent.py            # Tool Use loop, core Agent
    │     ├── tools/
    │     │   ├── airflow_tool.py # get_log, trigger_rerun
    │     │   ├── gitlab_tool.py  # get_file
    │     │   └── history_tool.py # get_historical_failures
    │     └── prompts.py          # System prompt management
    │
    ├── Report Generator
    │     ├── generator.py        # DiagnosisResult → payload
    │     └── internal_api.py     # Mock, to be replaced with real API
    │
    └── State Store (SQLite)
          ├── store.py            # CRUD
          └── models.py           # dataclass definitions

CLI entry: bau/cli.py
  bau run              # trigger full pipeline
  bau status           # view pending actions
  bau approve <id>     # approve an action
  bau reject <id>      # reject an action
```

## Core Data Structures

### FailedTaskReport (Ingestion layer output)
```python
@dataclass
class FailedTaskReport:
    dag_id: str
    dag_run_id: str
    scheduled_time: datetime
    start_date: datetime
    duration: timedelta
    avg_duration: timedelta
    is_overtime: bool
    failed_tasks: list[str]           # status == "failed"
    upstream_failed_tasks: list[str]  # status == "upstream_failed"
    root_cause_tasks: list[str]       # actual root cause tasks
```

### DiagnosisResult (Analysis layer output)
```python
@dataclass
class DiagnosisResult:
    dag_id: str
    dag_run_id: str
    root_cause_category: str  # data_malformed / network_error / code_bug / permission_error / unknown
    confidence: float
    evidence: list[str]
    suggested_actions: list[Action]
    reasoning_trace: list[dict]
```

### Action (smallest unit requiring human confirmation)
```python
@dataclass
class Action:
    action_type: str  # airflow_rerun / send_message / manual_only
    target: str
    params: dict
    reason: str
    requires_confirmation: bool = True
```

## Agent State Machine
```
IDLE → FETCH → ANALYZE → REPORT → AWAIT_HUMAN → EXECUTE → DONE
                                       ↑
                              process may exit here
                              resumes after user approval
```

## Key Design Decisions
1. **Regex for email parsing, not LLM**: format is fixed; LLM adds cost and non-determinism for no benefit
2. **SQLite for state persistence**: supports cross-process human-in-the-loop
3. **Native Anthropic Tool Use, not LangChain**: learning the underlying mechanism
4. **Execute stage requires human confirmation**: production operations must not be fully automated

## SQLite Schema
```sql
CREATE TABLE agent_runs (
    run_id TEXT PRIMARY KEY,
    triggered_at TIMESTAMP,
    status TEXT,
    summary JSON
);

CREATE TABLE pending_actions (
    action_id TEXT PRIMARY KEY,
    run_id TEXT,
    action_type TEXT,
    params JSON,
    reason TEXT,
    status TEXT,  -- PENDING/APPROVED/REJECTED/EXECUTED
    created_at TIMESTAMP
);

CREATE TABLE diagnosis_history (
    dag_id TEXT,
    run_id TEXT,
    diagnosed_at TIMESTAMP,
    root_cause_category TEXT,
    resolution TEXT,
    PRIMARY KEY (dag_id, run_id)
);
```

## Development Phases
- **Phase 1 (complete)**: Data layer — email_parser, airflow_tool, gitlab_tool, state store
- **Phase 2**: Agent Core — tool use loop, prompts, report generator
- **Phase 3**: Human-in-the-Loop — CLI, AWAIT_HUMAN state persistence, Airflow rerun
- **Phase 4**: Connect real Internal API, iterate on prompts

## Email Sample Format
```
Dag ID: data_data_pltingestion_email_demo_linear_dag
Dag Run ID: scheduled__2026-02-25T18:00:00+00:00     failed     Dag Run Link Click Here
Start date: 2026-02-26 01:59:49 SGT
Duration: 00:01:58 (avg:00:03:54)
     Task ID: collect     failed
     Task ID: parse     upstream_failed
```
- Email is HTML; only "Dag Run Link Click Here" is wrapped in an href; all other content is plain text
- `failed` task is the real root cause; `upstream_failed` is cascading failure
- Overtime check: duration > avg_duration * 1.5 (configurable)

## Environment Variables (see .env.example)
- ANTHROPIC_API_KEY
- GMAIL_CREDENTIALS_PATH
- AIRFLOW_BASE_URL / AIRFLOW_USERNAME / AIRFLOW_PASSWORD
- GITLAB_BASE_URL / GITLAB_TOKEN
- INTERNAL_API_BASE_URL / INTERNAL_API_TOKEN
- OVERTIME_THRESHOLD (default 1.5)
