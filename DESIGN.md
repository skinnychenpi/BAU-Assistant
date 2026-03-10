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
| 代码仓库 | 公司内网 self-hosted GitLab，有 API token |
| 报告推送 | 内部自建 REST API（目前先 mock） |
| 消息通知 | 内部自建 REST API（目前先 mock） |
| Agent 自主性 | 半自动：分析和 report 自动，重跑和发消息需人工确认 |
| Airflow 重跑 | Agent 给方案，人工确认后执行 |
| 技术栈 | Python 为主 |

---

## 整体架构

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

## Agent 状态机

```
        ┌─────────┐
        │  IDLE   │◀─────────────────────────────┐
        └────┬────┘                              │
             │ cron trigger / manual trigger     │
             ▼                                   │
        ┌─────────┐                              │
        │  FETCH  │  读 Gmail，解析失败作业列表       │
        └────┬────┘                              │
             │ 无失败作业                          │
             ├──────────────────────────────────▶┘
             │ 有失败作业
             ▼
        ┌─────────┐
        │ ANALYZE │  拉 Log + GitLab 代码，LLM 诊断
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

工具集：
```
get_airflow_log(dag_id, run_id, task_id)  → log text
get_gitlab_file(dag_id, task_id)          → 代码内容
get_historical_failures(dag_id)           → 历史失败记录
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
- `diagnosis_history`：历史诊断（用于 get_historical_failures）

---

## 关键 Trade-off

| 决策 | 选择 | 放弃 | 原因 |
|------|------|------|------|
| 邮件解析 | Regex | LLM | 格式固定，LLM 是杀鸡用牛刀 |
| 状态持久化 | SQLite | 纯内存 | 支持跨进程 human-in-the-loop |
| Agent 框架 | 原生 Anthropic Tool Use | LangChain | 学习底层机制 |
| 自主性边界 | Analyze 自动，Execute 需确认 | 全自动 | 生产操作需要人工把关 |

---

## 开发 Roadmap

### Phase 1 — 数据层（Week 1）
- `email_parser.py`：regex 解析 + 单元测试
- `airflow_tool.py`：REST API 封装，能拉 log
- `gitlab_tool.py`：根据 dag_id 找代码文件
- `state/store.py`：SQLite schema + CRUD

完成标准：给定一封邮件，输出结构化 `FailedTaskReport`，并能拉到每个 failed task 的 log

### Phase 2 — Agent Core（Week 2）
- `agent.py`：Tool Use loop
- `prompts.py`：诊断 prompt
- `report/generator.py`：结构化报告生成
- `report/internal_api.py`：mock

完成标准：给定 `FailedTaskReport`，输出有 evidence 支撑的 `DiagnosisResult`

### Phase 3 — Human-in-the-Loop（Week 3）
- `cli.py`：run / status / approve / reject
- `AWAIT_HUMAN` 状态持久化和恢复
- Airflow rerun 真实调用

完成标准：完整跑通端到端流程

### Phase 4 — 接真实 API（持续）
- 接 Internal API（报告推送）
- 接 Messaging API（通知 data source owner）
- 基于实际使用迭代 prompt
