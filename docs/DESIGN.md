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
| Spark log access | YARN ResourceManager REST API (pending infra confirmation) |
| Code repository | Company internal self-hosted GitLab, API token available |
| Job documentation | Confluence pages, mapped via YAML config |
| Historical issues | Google Sheets, accessed via Sheets API |
| Report delivery | Internal REST API (mocked for now) |
| Message notifications | Internal REST API (mocked for now) |
| Agent autonomy | Semi-automatic: analysis and reporting are automatic; rerun and messaging require human confirmation |
| Airflow rerun | Agent proposes the action; human confirms before execution |
| AI approach | Middle tier: LLM + tools + prompt engineering (no RAG yet) |
| Tech stack | Python-first |

---

## Overall Architecture

```
┌──────────────────────────────────────────────────────────────────────┐
│                          BAU-Assistant                                │
│                                                                      │
│  ┌─────────────┐    ┌──────────────────────────────────────────┐    │
│  │  Scheduler  │───▶│            Orchestrator                  │    │
│  │  (cron job) │    │  1. Pre-fetch log + history              │    │
│  └─────────────┘    │  2. Build initial prompt with context    │    │
│                     │  3. Run agent tool-use loop              │    │
│                     └──────────────────┬───────────────────────┘    │
│                                        │                            │
│         ┌──────────────────────────────┼──────────────────────┐     │
│         ▼                              ▼                      ▼     │
│  ┌──────────────┐  ┌──────────────────────────┐  ┌──────────────┐  │
│  │  Ingestion   │  │   Analysis Engine        │  │   Action     │  │
│  │  Pipeline    │  │   (LLM Core - Guided)    │  │   Layer      │  │
│  │              │  │                          │  │  (Human Loop)│  │
│  └──────┬───────┘  └──────────┬───────────────┘  └──────┬───────┘  │
│         │                     │                         │           │
└─────────┼─────────────────────┼─────────────────────────┼───────────┘
          │                     │                         │
  ┌───────▼──────┐   ┌─────────▼────────┐   ┌────────────▼──────────┐
  │ Gmail API    │   │ GitLab API       │   │ Airflow API           │
  │ GSheet API   │   │ Confluence API   │   │ (trigger rerun)       │
  │ Airflow API  │   │ Airflow Log API  │   │ Internal API (mock)   │
  │              │   │ YARN Log API     │   │                       │
  └──────────────┘   └──────────────────┘   └───────────────────────┘
```

---

## Knowledge Pipeline (Guided Approach)

The agent uses a **guided** context strategy to minimize LLM round-trips:

```
┌──────────────────────────────────────────────────────────────┐
│                      Orchestrator                            │
│                                                              │
│  1. Receive FailedTaskReport from email parser               │
│  2. Pre-fetch (parallel):                                    │
│     ├── Task log (via log_router → Airflow or YARN)          │
│     └── Historical issues (via GSheet API)                   │
│  3. Build initial prompt:                                    │
│     ├── System prompt (role + reasoning framework)           │
│     ├── FailedTaskReport (structured)                        │
│     ├── Log context (pre-fetched, pre-processed)             │
│     └── Historical issues (pre-fetched)                      │
│  4. Send to Agent with on-demand tools available             │
└──────────────────────┬───────────────────────────────────────┘
                       │
                       ▼
┌──────────────────────────────────────────────────────────────┐
│                Agent (Claude Tool-Use Loop)                   │
│                                                              │
│  Context already loaded:                                     │
│    ✓ FailedTaskReport                                        │
│    ✓ Processed log (exception + stack trace + last 150 lines)│
│    ✓ Historical issues for this DAG                          │
│                                                              │
│  On-demand tools:                                            │
│    ○ get_source_code(dag_id, task_id)                        │
│      → task source file + DAG definition from GitLab         │
│    ○ get_job_runbook(dag_id)                                 │
│      → Confluence page content via YAML mapping              │
│    ○ get_full_log(dag_id, run_id, task_id)                   │
│      → untruncated log (fallback when 150 lines not enough)  │
│                                                              │
│  Output: DiagnosisResult                                     │
└──────────────────────────────────────────────────────────────┘
```

Knowledge sources:
| Source | Content | Access Method | Pre-fetched? |
|--------|---------|---------------|--------------|
| Airflow logs | Runtime errors (bash/python tasks) | Airflow REST API | Yes |
| YARN logs | Runtime errors (Spark cluster-mode) | YARN ResourceManager API | Yes |
| Google Sheet | Historical issues & resolutions | Google Sheets API | Yes |
| GitLab repo | Job code + DAG definitions | GitLab API | No (on-demand) |
| Confluence | Per-job runbooks/business logic | Confluence REST API | No (on-demand) |

---

## Log Retrieval — Dual Source Architecture

Spark cluster-mode tasks store logs in YARN, not Airflow. Three independent modules handle this:

```
log_router.py (Module 3 — Dispatcher)
    │
    ├── Read dag_config.yaml
    │   └── Determine task type: spark | bash | python
    │
    ├── type: bash/python ──► airflow_log_tool.py (Module 1)
    │                          └── Airflow REST API → processed log
    │
    └── type: spark ────────► yarn_log_tool.py (Module 2)
                               └── YARN ResourceManager API → processed log
```

- **`airflow_log_tool.py`** — Single responsibility: fetch from Airflow REST API
- **`yarn_log_tool.py`** — Single responsibility: fetch from YARN ResourceManager API
- **`log_router.py`** — Reads YAML config, dispatches to the correct tool

Log pre-processing (both tools):
- Extract exception message
- Extract stack trace
- Keep last 150 lines
- `get_full_log` available as fallback for agent

---

## DAG Configuration File

`bau/knowledge/dag_config.yaml` — version-controlled metadata per DAG:

```yaml
data_pltingestion_email_demo_linear_dag:
  confluence_url: "https://confluence.yourco.com/pages/12345"
  tasks:
    collect:
      type: spark              # spark | bash | python
      yarn_app_name: "pltingestion_email_collect"
    parse:
      type: bash               # log stays in Airflow
    transform:
      type: spark
      yarn_app_name: "pltingestion_email_transform"
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
        │  FETCH  │  Read Gmail, parse failed job list
        └────┬────┘                              │
             │ no failures                       │
             ├──────────────────────────────────▶┘
             │ failures found
             ▼
        ┌─────────┐
        │ ANALYZE │  Pre-fetch log + history, run agent
        └────┬────┘
             ▼
        ┌─────────┐
        │  REPORT │  Generate report, push to Internal API
        └────┬────┘
             ▼
        ┌──────────────┐
        │ AWAIT_HUMAN  │  Show proposed actions, wait for confirmation (process may exit)
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

**Guided approach**: orchestrator pre-fetches log + historical issues, agent calls additional tools on-demand.

Pre-fetched context (always included):
- Task log (via log_router → airflow or yarn)
- Historical issues (via GSheet API)

On-demand tools (agent decides):
```
get_source_code(dag_id, task_id)   → task code + DAG definition from GitLab
get_job_runbook(dag_id)            → Confluence page via YAML mapping
get_full_log(dag_id, run_id, task_id) → untruncated log (fallback)
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
- `diagnosis_history`: historical diagnoses

---

## Key Trade-offs

| Decision | Chosen | Rejected | Reason |
|----------|--------|----------|--------|
| Email parsing | Regex | LLM | Fixed format; LLM is overkill |
| State persistence | SQLite | In-memory | Supports cross-process human-in-the-loop |
| Agent framework | Native Anthropic Tool Use | LangChain | Learning the underlying mechanism |
| Autonomy boundary | Analyze auto, Execute requires confirmation | Fully automatic | Production operations need human oversight |
| Context strategy | Guided (pre-fetch + on-demand) | Fully autonomous | Saves 2 LLM round-trips, always need log + history |
| Log module design | 3 independent modules + router | Single monolithic tool | Clean separation, testable, extensible |
| DAG metadata | YAML config file in repo | Confluence index page | Version-controlled, no API call for mapping |
| AI complexity tier | Middle (LLM + tools + prompts) | RAG / pattern matching | Handles repetitive patterns well, escalates novel issues |

---

## File Structure

```
bau/
├── __init__.py
├── config.py                      # Pydantic settings
├── cli.py                         # CLI entry point [Phase 3]
├── orchestrator.py                # Main loop [Phase 2]
│
├── ingestion/
│   ├── __init__.py
│   ├── email_parser.py            # ✓ Regex email parsing
│   ├── gmail_client.py            # Gmail API wrapper [Phase 1]
│   └── gsheet_client.py           # Google Sheets API [Phase 2]
│
├── analysis/
│   ├── __init__.py
│   ├── agent.py                   # Tool-use loop [Phase 2]
│   ├── prompts.py                 # System prompt [Phase 2]
│   └── tools/
│       ├── __init__.py
│       ├── airflow_log_tool.py    # Fetch log from Airflow [Phase 2]
│       ├── yarn_log_tool.py       # Fetch log from YARN [Phase 2]
│       ├── log_router.py          # Route to correct log source [Phase 2]
│       ├── gitlab_tool.py         # Task code + DAG definition [Phase 2]
│       ├── confluence_tool.py     # Job runbook via YAML [Phase 2]
│       └── history_tool.py        # Historical issues from GSheet [Phase 2]
│
├── knowledge/
│   └── dag_config.yaml            # DAG metadata: confluence URLs,
│                                  # task types, YARN app names
│
├── report/
│   ├── __init__.py
│   ├── generator.py               # DiagnosisResult → payload [Phase 2]
│   └── internal_api.py            # Mock API client [Phase 2]
│
└── state/
    ├── __init__.py
    ├── models.py                  # ✓ Data models
    └── store.py                   # ✓ SQLite CRUD

tests/
├── __init__.py
├── conftest.py                    # Shared fixtures [Phase 2]
├── test_email_parser.py           # ✓ 8 passing tests
├── test_store.py                  # [Phase 2]
├── test_log_router.py             # [Phase 2]
├── test_airflow_log_tool.py       # [Phase 2]
├── test_yarn_log_tool.py          # [Phase 2]
├── test_agent.py                  # [Phase 2]
└── fixtures/
    └── sample_email.txt           # ✓ Test fixture
```

---

## Development Roadmap

### Phase 1 — Data Layer (partial, in progress)
- ✓ `email_parser.py`: regex parsing + unit tests
- ✓ `state/store.py`: SQLite schema + CRUD
- ✓ `state/models.py`: data models
- `gmail_client.py`: Gmail API wrapper

Completion criteria: given an email, output a structured `FailedTaskReport`

### Phase 2 — Agent Core (next)
- `dag_config.yaml`: DAG metadata configuration
- `airflow_log_tool.py`: Airflow REST API log fetching
- `yarn_log_tool.py`: YARN ResourceManager API log fetching
- `log_router.py`: route to correct log source
- `gitlab_tool.py`: fetch task code + DAG definition
- `confluence_tool.py`: fetch runbook via YAML mapping
- `gsheet_client.py` + `history_tool.py`: historical issues from Google Sheets
- `agent.py`: Tool Use loop (guided approach)
- `prompts.py`: system prompt + reasoning framework
- `report/generator.py`: structured report generation
- `report/internal_api.py`: mock
- Unit tests for all new modules

Completion criteria: given a `FailedTaskReport`, output a `DiagnosisResult` supported by evidence

### Phase 3 — Human-in-the-Loop
- `cli.py`: run / status / approve / reject
- `AWAIT_HUMAN` state persistence and recovery
- Airflow rerun real API call

Completion criteria: full end-to-end flow runs successfully

### Phase 4 — Connect Real APIs (ongoing)
- Connect Internal API (report delivery)
- Connect Messaging API (notify data source owners)
- Iterate on prompts based on real usage
- Consider RAG knowledge base for deeper Confluence/runbook understanding
