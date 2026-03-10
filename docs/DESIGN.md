# BAU-Assistant System Design

## Background

The data engineer BAU (Business As Usual) workflow is highly repetitive:
every day requires manually checking Airflow failure emails → pulling logs → reading code → identifying the issue → writing a report → rerunning jobs → notifying stakeholders.

This project aims to automate this workflow using an AI Agent, and also serves as a learning vehicle for AI application programming.

---

## Requirements

| Item | Decision |
|------|----------|
| Email source | Gmail, sent automatically by Airflow daily |
| Airflow log access | Airflow REST API |
| Code repository | Company internal self-hosted GitLab, API token available |
| Report delivery | Internal REST API (mocked for now) |
| Message notifications | Internal REST API (mocked for now) |
| Agent autonomy | Semi-automatic: analysis and reporting are automatic; rerun and messaging require human confirmation |
| Airflow rerun | Agent proposes the action; human confirms before execution |
| Tech stack | Python-first |

---

## Overall Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        BAU-Assistant                            │
│                                                                 │
│   ┌─────────────┐    ┌─────────────────────────────────────┐   │
│   │  Scheduler  │───▶│           Orchestrator              │   │
│   │  (cron job) │    │        (Agent Core Loop)            │   │
│   └─────────────┘    └──────────────┬──────────────────────┘   │
│                                     │                           │
│              ┌──────────────────────┼──────────────────────┐   │
│              ▼                      ▼                       ▼   │
│   ┌─────────────────┐  ┌─────────────────┐  ┌───────────────┐  │
│   │  Ingestion      │  │  Analysis       │  │  Action       │  │
│   │  Pipeline       │  │  Engine         │  │  Layer        │  │
│   │                 │  │  (LLM Core)     │  │  (Human Loop) │  │
│   └────────┬────────┘  └────────┬────────┘  └──────┬────────┘  │
│            │                    │                   │           │
└────────────┼────────────────────┼───────────────────┼───────────┘
             │                    │                   │
     ┌───────▼──────┐    ┌────────▼───────┐   ┌──────▼────────┐
     │ Gmail API    │    │ GitLab API     │   │ Airflow API   │
     │ Airflow API  │    │ Airflow Log API│   │ (trigger run) │
     └──────────────┘    └────────────────┘   │ Internal API  │
                                              └───────────────┘
```

---

## Agent State Machine

```
        ┌─────────┐
        │  IDLE   │◀─────────────────────────────┐
        └────┬────┘                              │
             │ cron trigger / manual trigger     │
             ▼                                   │
        ┌─────────┐                              │
        │  FETCH  │  Read Gmail, parse failed job list    │
        └────┬────┘                              │
             │ no failures                       │
             ├──────────────────────────────────▶┘
             │ failures found
             ▼
        ┌─────────┐
        │ ANALYZE │  Fetch logs + GitLab code, LLM diagnoses
        └────┬────┘
             ▼
        ┌─────────┐
        │  REPORT │  Generate report, push to Internal API
        └────┬────┘
             ▼
        ┌──────────────┐
        │ AWAIT_HUMAN  │  Show proposed actions, wait for user confirmation (process may exit)
        └────┬─────────┘
             │ user approves
             ▼
        ┌─────────┐
        │ EXECUTE │  Call Airflow API / Messaging API
        └────┬────┘
             ▼
        ┌─────────┐
        │  DONE   │  Record result to state store
        └─────────┘
```

**Key design**: `AWAIT_HUMAN` is a persistent state. After the Agent writes the proposed actions to SQLite the process exits. The user runs `bau approve <id>` via CLI to confirm, and execution resumes. This is the **durable execution** pattern.

---

## Module Design

### Module 1 — Ingestion Pipeline

**Principle: email format is fixed, use regex not LLM**

Email sample:
```
Dag ID: data_data_pltingestion_email_demo_linear_dag
Dag Run ID: scheduled__2026-02-25T18:00:00+00:00     failed     Dag Run Link Click Here
Start date: 2026-02-26 01:59:49 SGT
Duration: 00:01:58 (avg:00:03:54)
     Task ID: collect     failed
     Task ID: parse     upstream_failed
```

Email is HTML. Only "Dag Run Link Click Here" is wrapped in an href; all other content is plain text.

Parsing rules:
- `failed` task = actual root cause
- `upstream_failed` task = cascading failure, not the root cause
- Overtime check: `duration > avg_duration * OVERTIME_THRESHOLD`

### Module 2 — Analysis Engine (the only place LLM is used)

Tool set:
```
get_airflow_log(dag_id, run_id, task_id)  → log text
get_gitlab_file(dag_id, task_id)          → source code content
get_historical_failures(dag_id)           → historical failure records
```

Diagnosis categories:
- `data_malformed`: data format issue
- `network_error`: network / connectivity issue
- `code_bug`: code logic error
- `permission_error`: permission issue
- `unknown`: cannot determine

Agent loop limit: MAX_STEPS = 10, to prevent infinite loops.

### Module 3 — State Store (SQLite)

Three tables:
- `agent_runs`: main record per run
- `pending_actions`: actions awaiting confirmation
- `diagnosis_history`: historical diagnoses (used by get_historical_failures)

---

## Key Trade-offs

| Decision | Chosen | Rejected | Reason |
|----------|--------|----------|--------|
| Email parsing | Regex | LLM | Fixed format; LLM is overkill |
| State persistence | SQLite | In-memory | Supports cross-process human-in-the-loop |
| Agent framework | Native Anthropic Tool Use | LangChain | Learning the underlying mechanism |
| Autonomy boundary | Analyze auto, Execute requires confirmation | Fully automatic | Production operations need human oversight |

---

## Development Roadmap

### Phase 1 — Data Layer (Week 1)
- `email_parser.py`: regex parsing + unit tests
- `airflow_tool.py`: REST API wrapper, fetch logs
- `gitlab_tool.py`: locate source file from dag_id
- `state/store.py`: SQLite schema + CRUD

Completion criteria: given an email, output a structured `FailedTaskReport` and fetch logs for each failed task

### Phase 2 — Agent Core (Week 2)
- `agent.py`: Tool Use loop
- `prompts.py`: diagnosis prompt
- `report/generator.py`: structured report generation
- `report/internal_api.py`: mock

Completion criteria: given a `FailedTaskReport`, output a `DiagnosisResult` supported by evidence

### Phase 3 — Human-in-the-Loop (Week 3)
- `cli.py`: run / status / approve / reject
- `AWAIT_HUMAN` state persistence and recovery
- Airflow rerun real API call

Completion criteria: full end-to-end flow runs successfully

### Phase 4 — Connect Real APIs (ongoing)
- Connect Internal API (report delivery)
- Connect Messaging API (notify data source owners)
- Iterate on prompts based on real usage
