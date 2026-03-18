# BAU-Assistant 系统设计文档

## 背景

数据工程师的 BAU（Business As Usual）流程高度繁琐：
每天需要手动检查 Airflow 失败邮件 → 拉 log → 看代码 → 定位问题 → 写报告 → 重跑作业 → 通知相关人员

本项目目标是用 AI Agent 自动化这个流程，同时作为 AI 应用编程的学习载体。

---

## 需求确认

| 项目 | 决策 |
|------|------|
| 邮件来源 | Gmail，Airflow 每天自动发送 |
| Airflow log 访问 | Airflow REST API |
| Spark log 访问 | YARN ResourceManager REST API（待 infra 团队确认） |
| 代码仓库 | 公司内网 self-hosted GitLab，有 API token |
| 作业文档 | Confluence 页面，通过 YAML 配置映射 |
| 历史问题 | Google Sheets，通过 Sheets API 访问 |
| 报告推送 | 内部自建 REST API（目前先 mock） |
| 消息通知 | 内部自建 REST API（目前先 mock） |
| Agent 自主性 | 半自动：分析和 report 自动，重跑和发消息需人工确认 |
| Airflow 重跑 | Agent 给方案，人工确认后执行 |
| AI 方案选择 | 中间层：LLM + 工具 + 提示词工程（暂不用 RAG） |
| 技术栈 | Python 为主 |

---

## 整体架构

```
┌──────────────────────────────────────────────────────────────────────┐
│                          BAU-Assistant                                │
│                                                                      │
│  ┌─────────────┐    ┌──────────────────────────────────────────┐    │
│  │  Scheduler  │───▶│            Orchestrator                  │    │
│  │  (cron job) │    │  1. 预取 log + 历史问题                     │    │
│  └─────────────┘    │  2. 构建带上下文的初始 prompt                │    │
│                     │  3. 运行 agent tool-use 循环               │    │
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

## 知识管道（Guided 方案）

Agent 使用 **Guided** 上下文策略，减少 LLM 往返次数：

```
Orchestrator 预取（总是需要的）:
  ├── 任务日志（通过 log_router → Airflow 或 YARN）
  └── 历史问题（通过 GSheet API）

Agent 按需调用（自行决定）:
  ├── get_source_code(dag_id, task_id) → GitLab 代码
  ├── get_job_runbook(dag_id)          → Confluence 文档
  └── get_full_log(dag_id, run_id, task_id) → 完整日志
```

知识来源:
| 来源 | 内容 | 访问方式 | 是否预取 |
|------|------|----------|----------|
| Airflow 日志 | 运行时错误（bash/python 任务） | Airflow REST API | 是 |
| YARN 日志 | 运行时错误（Spark cluster 模式） | YARN ResourceManager API | 是 |
| Google Sheet | 历史问题与解决方案 | Google Sheets API | 是 |
| GitLab 仓库 | 作业代码 + DAG 定义 | GitLab API | 否（按需） |
| Confluence | 作业说明文档 | Confluence REST API | 否（按需） |

---

## 日志获取 — 双源架构

Spark cluster 模式任务的日志在 YARN，不在 Airflow。三个独立模块处理：

```
log_router.py（模块3 — 调度器）
    │
    ├── 读取 dag_config.yaml
    │   └── 判断任务类型：spark | bash | python
    │
    ├── bash/python ──► airflow_log_tool.py（模块1）
    │                    └── Airflow REST API → 处理后的日志
    │
    └── spark ──────► yarn_log_tool.py（模块2）
                       └── YARN ResourceManager API → 处理后的日志
```

- **`airflow_log_tool.py`** — 单一职责：从 Airflow REST API 获取日志
- **`yarn_log_tool.py`** — 单一职责：从 YARN ResourceManager API 获取日志
- **`log_router.py`** — 读取 YAML 配置，分发到正确的日志工具

日志预处理（两个工具都执行）：
- 提取异常信息
- 提取堆栈跟踪
- 保留最后 150 行
- `get_full_log` 作为回退方案供 Agent 使用

---

## DAG 配置文件

`bau/knowledge/dag_config.yaml` — 版本控制的 DAG 元数据：

```yaml
data_pltingestion_email_demo_linear_dag:
  confluence_url: "https://confluence.yourco.com/pages/12345"
  tasks:
    collect:
      type: spark              # spark | bash | python
      yarn_app_name: "pltingestion_email_collect"
    parse:
      type: bash               # 日志在 Airflow
    transform:
      type: spark
      yarn_app_name: "pltingestion_email_transform"
```

---

## Agent 状态机

```
        ┌─────────┐
        │  IDLE   │◀─────────────────────────────┐
        └────┬────┘                              │
             │ cron trigger / manual trigger     │
             ▼                                   │
        ┌─────────┐                              │
        │  FETCH  │  读 Gmail，解析失败作业列表
        └────┬────┘                              │
             │ 无失败作业                          │
             ├──────────────────────────────────▶┘
             │ 有失败作业
             ▼
        ┌─────────┐
        │ ANALYZE │  预取 log + 历史问题，运行 Agent
        └────┬────┘
             ▼
        ┌─────────┐
        │  REPORT │  生成 report，推送 Internal API
        └────┬────┘
             ▼
        ┌──────────────┐
        │ AWAIT_HUMAN  │  展示方案，等待用户确认（进程可退出）
        └────┬─────────┘
             │ 用户 approve
             ▼
        ┌─────────┐
        │ EXECUTE │  调用 Airflow API / Messaging API
        └────┬────┘
             ▼
        ┌─────────┐
        │  DONE   │  记录结果到 state store
        └─────────┘
```

**关键设计**：`AWAIT_HUMAN` 是持久化状态。Agent 把方案写入 SQLite 后进程退出，
用户通过 CLI `bau approve <id>` 确认后重新恢复执行。这是 **durable execution** 模式。

---

## 模块设计

### Module 1 — Ingestion Pipeline

**原则：邮件格式固定，用 regex 而不是 LLM 解析**

邮件样本：
```
Dag ID: data_data_pltingestion_email_demo_linear_dag
Dag Run ID: scheduled__2026-02-25T18:00:00+00:00     failed     Dag Run Link Click Here
Start date: 2026-02-26 01:59:49 SGT
Duration: 00:01:58 (avg:00:03:54)
     Task ID: collect     failed
     Task ID: parse     upstream_failed
```

解析规则：
- `failed` task = 真正的根因
- `upstream_failed` task = 连锁失败，不是根因
- 超时判断：`duration > avg_duration * OVERTIME_THRESHOLD`

### Module 2 — Analysis Engine（LLM 唯一使用处）

**Guided 方案**：Orchestrator 预取 log + 历史问题，Agent 按需调用其他工具。

预取上下文（总是包含）：
- 任务日志（通过 log_router → Airflow 或 YARN）
- 历史问题（通过 GSheet API）

按需工具（Agent 自行决定）：
```
get_source_code(dag_id, task_id)        → GitLab 任务代码 + DAG 定义
get_job_runbook(dag_id)                 → Confluence 页面（通过 YAML 映射）
get_full_log(dag_id, run_id, task_id)   → 完整日志（回退方案）
```

诊断分类：
- `data_malformed`：数据格式问题
- `network_error`：网络/连接问题
- `code_bug`：代码逻辑错误
- `permission_error`：权限问题
- `unknown`：无法确定

Agent Loop 限制：MAX_STEPS = 10，防止无限循环。

### Module 3 — State Store（SQLite）

三张表：
- `agent_runs`：每次运行主记录
- `pending_actions`：待确认的操作
- `diagnosis_history`：历史诊断

---

## 关键 Trade-off

| 决策 | 选择 | 放弃 | 原因 |
|------|------|------|------|
| 邮件解析 | Regex | LLM | 格式固定，LLM 是杀鸡用牛刀 |
| 状态持久化 | SQLite | 纯内存 / PostgreSQL | 支持跨进程 human-in-the-loop；单服务器，少量用户 |
| Agent 框架 | 原生 Anthropic Tool Use | LangChain | 学习底层机制 |
| 自主性边界 | Analyze 自动，Execute 需确认 | 全自动 | 生产操作需要人工把关 |
| 上下文策略 | Guided（预取 + 按需） | 全自主 | 节省 2 次 LLM 往返，log + 历史总是需要的 |
| 日志模块设计 | 3 个独立模块 + 路由器 | 单一整体工具 | 职责清晰，可测试，可扩展 |
| DAG 元数据 | 仓库内 YAML 配置文件 | Confluence 索引页 | 版本控制，无需 API 调用获取映射 |
| AI 复杂度 | 中间层（LLM + 工具 + 提示词） | RAG / 模式匹配 | 处理重复模式能力强，新问题可升级给人工 |
| 部署方式 | Docker + docker-compose | 裸机部署 | 一次测试，到处部署；方便团队成员使用 |
| 调度方式 | APScheduler（进程内） | Celery / 外部 cron | 简单，无需额外基础设施 |

---

## 文件结构

```
BAU-Assistant/
├── Dockerfile                     # ✓ python:3.11-slim
├── docker-compose.yml             # ✓ 数据卷、环境变量、重启策略
├── .dockerignore                  # ✓ 排除测试、文档、密钥
├── pyproject.toml                 # ✓ 所有依赖
├── .env.example                   # ✓ 所有环境变量文档
│
├── bau/
│   ├── __init__.py
│   ├── config.py                  # ✓ Pydantic 配置
│   ├── cli.py                     # ✓ CLI：run/status/approve/reject/serve
│   ├── orchestrator.py            # ✓ 流水线主循环
│   │
│   ├── ingestion/
│   │   ├── __init__.py
│   │   ├── email_parser.py        # ✓ Regex 邮件解析
│   │   ├── gmail_client.py        # Gmail API 封装 [Phase 3]
│   │   └── gsheet_client.py       # ✓ Google Sheets API
│   │
│   ├── analysis/
│   │   ├── __init__.py
│   │   ├── agent.py               # ✓ Tool-use 循环（Guided 方案）
│   │   ├── prompts.py             # ✓ 系统提示词 + 工具定义
│   │   └── tools/
│   │       ├── __init__.py
│   │       ├── airflow_log_tool.py # ✓ Airflow 日志 + 预处理
│   │       ├── yarn_log_tool.py    # ✓ YARN 日志（待 infra 确认）
│   │       ├── log_router.py       # ✓ 日志源路由
│   │       ├── gitlab_tool.py      # ✓ 任务代码 + DAG 定义
│   │       ├── confluence_tool.py  # ✓ 通过 YAML 获取作业文档
│   │       └── history_tool.py     # ✓ 从 GSheet 获取历史问题
│   │
│   ├── knowledge/
│   │   ├── __init__.py
│   │   ├── loader.py              # ✓ YAML 配置加载器
│   │   └── dag_config.yaml        # ✓ DAG 元数据模板
│   │
│   ├── report/
│   │   ├── __init__.py
│   │   ├── generator.py           # ✓ 报告生成（payload + 文本）
│   │   └── internal_api.py        # ✓ Mock API 客户端
│   │
│   └── state/
│       ├── __init__.py
│       ├── models.py              # ✓ 数据模型
│       └── store.py               # ✓ SQLite CRUD
│
└── tests/                         # 85 个测试，全部通过
    ├── __init__.py
    ├── conftest.py                # ✓ 共享 fixtures
    ├── test_email_parser.py       # ✓ 8 个测试
    ├── test_store.py              # ✓ 11 个测试
    ├── test_log_tools.py          # ✓ 14 个测试
    ├── test_knowledge_loader.py   # ✓ 7 个测试
    ├── test_confluence_tool.py    # ✓ 10 个测试
    ├── test_prompts.py            # ✓ 8 个测试
    ├── test_agent.py              # ✓ 10 个测试
    ├── test_history_tool.py       # ✓ 5 个测试
    ├── test_report_generator.py   # ✓ 7 个测试
    └── fixtures/
        └── sample_email.txt       # ✓ 测试数据
```

---

## 部署

Docker 部署到开发服务器（公司内网）。

```bash
# 构建
docker compose build

# 作为长驻服务运行（默认每 60 分钟）
docker compose up -d

# 手动触发流水线
docker compose run --rm bau-assistant run

# 查看待处理操作
docker compose run --rm bau-assistant status

# 审批 / 拒绝
docker compose run --rm bau-assistant approve <action_id>
docker compose run --rm bau-assistant reject <action_id>
```

数据卷：
- `bau-data` — 持久化 SQLite 数据库
- `./credentials` — OAuth 凭证文件（只读挂载）

未来：FastAPI Web UI 供团队成员使用（Phase B）。

---

## 开发 Roadmap

### Phase 1 — 数据层（已完成）
- ✓ `email_parser.py`：regex 解析 + 8 个单元测试
- ✓ `state/store.py`：SQLite schema + CRUD + 11 个单元测试
- ✓ `state/models.py`：数据模型

### Phase 2 — Agent Core（已完成）
- ✓ 所有工具：airflow_log、yarn_log、log_router、gitlab、confluence、history
- ✓ `agent.py`：Tool Use 循环（Guided 方案）
- ✓ `prompts.py`：系统提示词 + 推理框架 + 工具定义
- ✓ `report/generator.py` + `internal_api.py`：报告生成
- ✓ `knowledge/loader.py` + `dag_config.yaml`：DAG 元数据
- ✓ 77 个新单元测试（共 85 个，全部通过）

### Phase 2.5 — Docker + CLI（已完成）
- ✓ `Dockerfile` + `docker-compose.yml`：容器化部署
- ✓ `cli.py`：run / status / approve / reject / serve
- ✓ `orchestrator.py`：完整流水线循环
- ✓ APScheduler 长驻服务模式

### Phase 3 — 端到端集成（下一步）
- `gmail_client.py`：Gmail API 封装
- 接入真实 Airflow/YARN/GitLab/Confluence API
- 审批后执行 Airflow rerun
- 用真实数据端到端测试

完成标准：在开发服务器上完整跑通端到端流程

### Phase 4 — 接真实 API（持续）
- 接 Internal API（报告推送）
- 接 Messaging API（通知 data source owner）
- 基于实际使用迭代 prompt
- 考虑 RAG 知识库以深入理解 Confluence/runbook

### Phase B — Web UI（未来）
- FastAPI 服务器 + REST 端点
- Web UI 供团队成员查看状态、审批/拒绝操作
- 向更多团队成员推广
