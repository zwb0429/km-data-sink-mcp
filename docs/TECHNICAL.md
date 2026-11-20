# km-data-sink-mcp 技术文档

> 版本：V1.0.2 ｜ 适用读者：接入本服务的开发者、二次开发与运维人员

## 背景介绍 📖

随着 AI 编码助手与定时任务型 Agent（WorkBuddy、Claude Desktop、Cursor 等）的普及，越来越多的"文档"由 AI 产出：每日 AI 新闻汇总、定时生成的日报周报、采集整理的资料等。这些文档通常只存在于聊天窗口或临时文件中，存在三个典型痛点：

1. **易丢失**：聊天记录滚动、会话结束后文档难以找回，无版本沉淀。
2. **不可查**：无法按任务名、业务日期、格式等维度检索历史文档。
3. **不可信**：定时任务"看起来跑完了"，但没有人能证明数据真的写进去了。

MCP（Model Context Protocol）解决了"AI 客户端如何调用外部能力"的问题，但**持久化落库这一环仍需每个使用者自行开发**：建表、去重、事务、重试、留痕、安全……每个项目都要重复造轮子。

km-data-sink-mcp 就是补上这一环的通用组件：

- 对**使用者**：改一份 YAML 配置即可让任意 MCP 客户端把文档可靠落库，零代码。
- 对**二次开发者**：一个不依赖 Spring 生态、结构清晰的 MCP Server 完整范例（配置加载、方言抽象、事务设计、并发控制、工具封装全部齐备）。

---

## 核心概念 💡

### 什么是 km-data-sink-mcp

一个基于 MCP 官方 Java SDK（2.1.0，经典 server + stdio 传输）的**数据落库服务器**。它以 stdio 子进程方式被 MCP 客户端拉起，对外暴露 7 个工具（tool）；内部把「文档全文 + 元数据」按 YAML 声明的表结构写入 MySQL 或 SQLite。

### 为什么存在（设计目标）

| 目标 | 手段 |
|---|---|
| 零代码接入新落库任务 | 配置即 schema：表/列/类型/来源/唯一键全部 YAML 声明 |
| 写入不丢、不重、不留半条数据 | 三段事务边界（T1/T2/T3）+ 幂等 upsert + 有限重试 |
| 执行可追溯 | `mcp_task_execution` 执行记录表，RUNNING/SUCCESS/FAILED 全程留痕 |
| 安全 | 表/列白名单、参数化绑定、排序字段白名单、密码环境变量注入 |
| 轻量 | 仅 JDK 17 + MCP SDK + HikariCP + Logback，不引入 Spring/MyBatis/JPA |

### 解决的核心问题

- **AI 文档的持久化与检索**：`save_document` 写入全文，`get_document`/`list_documents` 查回。
- **幂等性**：同一任务重复执行，通过 `upsert_key` 判重——覆盖模式只留最新，历史模式逐次留痕。
- **执行可信**：每次定时任务有唯一 `run_id`，开始时置 RUNNING，成功/失败同事务/补偿事务更新终态，事后可审计。

### 关键术语

| 术语 | 含义 |
|---|---|
| 连接（connection） | YAML 中 `connections` 下的一条数据库配置，工具调用时按名引用 |
| 白名单表（allowlist） | 仅允许写入声明的表，未声明的表一律返回 `INVALID_TABLE` |
| mode | `overwrite`（同键覆盖保留最新）/ `history`（每次执行一条新记录） |
| upsert_key | 唯一的冲突判断键（单组），先查后写的依据 |
| unique_keys | 辅助唯一索引（可多组），冲突即拒绝（`UNIQUE_KEY_CONFLICT`），不参与隐式更新 |
| source | 列值来源：调用参数 / `meta.xxx` 嵌套路径 / `system.uuid` / `system.now` |
| run_id | 一次执行的唯一编号，推荐 `{任务名}_{业务日期}_{HHmmss}` |
| document_id | 服务端生成的 UUID（VARCHAR(36)），overwrite 更新时保留原值 |

---

## 工作原理 🔧

### 总体执行流程

```
AI 客户端 →(stdio JSON-RPC)→ DataSinkServer → 工具处理器 → 引擎组件 → HikariCP → MySQL/SQLite
```

启动时序（`DataSinkServer.java`）：

```
解析 --config 参数 → ConfigLoader 加载 YAML + ConfigValidator 校验
→ 构建 ConnectionManager（连接配置，不建池）
→ 组装 7 个工具 → McpServer.sync(stdio).build()   ← 先就绪，握手不被 DB 阻塞
→ SchemaManager.init()（每连接建/校验执行记录表，失败仅告警不退出）
→ Thread.currentThread().join() 阻塞主线程，stdio 循环处理请求
```

> 设计点：MCP 客户端对 `initialize` 握手有超时限制，而远程 MySQL 连接池冷启动可达 15s+。所以**先起 server 后初始化 DB**，握手永不超时；DB 初始化失败时 server 保持运行，可通过 `test_connection` 排查。

### save_document 写入链路（核心）

```
参数校验（connection/table 必填、format 仅 html/md）
→ 表白名单检查（INVALID_TABLE）→ 列映射 ValueResolver（source 取值 + 类型收敛）
→ 内容字节校验（UTF-8 字节数 ≤ content.max_size_mb，超限 CONTENT_TOO_LARGE 不截断）
→ 自动建表 SchemaManager.ensureTable（首次写入时按 YAML DDL 建表 + created_at 索引）
→ T1：ExecutionRecorder 置 RUNNING（独立事务，带 SQLITE_BUSY 重试）
→ T2（同一事务）：
     UpsertWriter 先 SELECT（按 upsert_key）→ 命中则 UPDATE（保留原 document_id）否则 INSERT
     → ExecutionRecorder 置 SUCCESS
→ 异常路径：T3 独立补偿事务置 FAILED（error_code + error_message）
→ 返回统一信封 {success, code, message, data{document_id, operation, ...}}
```

### 字段映射（ValueResolver）

每列的 `source` 决定值从哪来：

| source | 取值 | 示例 |
|---|---|---|
| `task_name` / `run_id` / `business_date` / `format` / `content` | 工具调用参数同名键 | `source: task_name` |
| `meta.xxx`（支持 `a.b.c` 嵌套） | 调用参数 `meta` 对象内的路径取值 | `source: meta.article_id` |
| `system.uuid` | 服务端生成 UUID（document_id 系统列默认） | 自动加入 |
| `system.now` | 服务端当前时间（created_at 系统列默认，MySQL/SQLite 各自格式化） | 自动加入 |

类型收敛按目标列类型做基础转换（DECIMAL/INT/BIGINT/BOOLEAN 等），不做格式强校验，原文入库。

### 事务边界设计

| 事务 | 内容 | 为什么独立 |
|---|---|---|
| T1 | 插入 RUNNING 执行记录 | 任务开始即留痕；即使后续失败也有据可查 |
| T2 | 文档 upsert + 置 SUCCESS | 文档与状态**原子一致**：成功必见文档，见文档必成功 |
| T3 | 置 FAILED（含错误码/信息） | T2 失败时 T1 的 RUNNING 记录需要补偿收口；T3 自身失败仅告警不重抛 |

重试策略（`db.max_retry` / `retry_backoff_ms`）：仅对可重试异常（SQLite BUSY/LOCKED、连接类异常）做有限重试；业务性失败（唯一键冲突、参数错误）不重试。

### SQLite 多进程并发控制

SQLite 同一瞬间只允许一个写者，多客户端各自拉起 server 进程并发写同一 db 文件时：

```
单进程连接池强制 1（pool_size 配置被忽略）
→ WAL 模式（读写不互斥）+ busy_timeout 5000ms
→ SQLITE_BUSY/LOCKED 指数退避有限重试（T1 与 T2 都有）
→ 压测：6 进程并发 ×30 写 = 180/180 成功
```

### 查询性能设计（list_documents）

```
两段查询：先查轻量键列表（document_id + 业务键 + 摘要），按需再回表
→ 列表不含 content 全文，避免大字段拖慢分页
→ COUNT 按需执行：末页（返回行数 < page_size）跳过 COUNT
→ idx_order_by_<表名> (created_at) 索引：新表自动创建，旧表手动补（见 README）
→ 实测 p50 212ms → 37ms（5.7×）
```

### 安全机制

- **表白名单**：不在 `tables.allowlist` 的表一律拒绝。
- **列白名单**：INSERT/UPDATE 仅涉及 YAML 声明的列；`order_by` 只允许表列名，防注入。
- **参数化绑定**：全部 SQL 用 PreparedStatement，无字符串拼接值。
- **密码不落盘**：`password_env` 引用环境变量名；`list_connections` 输出脱敏（不含连接串）。
- **错误脱敏**：业务失败返回统一错误码与安全消息，不向客户端暴露堆栈。

---

## 安装步骤 📦

### 前置要求

- JDK 17+
- Maven 3.8+
- MySQL 8.x（可选，用 SQLite 可跳过）

### 构建

```bash
mvn package
```

产出 `target/km-data-sink-mcp.jar`（shade fat jar，含全部依赖，单文件分发）。

### 配置

```bash
cp config.yaml.example config.yaml
```

必须修改的三处（详见 README 第 2 步）：数据库连接、白名单表、密码环境变量。

```powershell
# Windows 设置密码环境变量
[Environment]::SetEnvironmentVariable("SINK_DB_PASSWORD", "真实密码", "User")
```

```bash
# Linux/macOS
export SINK_DB_PASSWORD=真实密码
```

### 验证安装

```bash
python scripts/e2e_test.py     # SQLite 端到端 12 步断言，全绿即安装成功
```

---

## 使用示例 🚀

### 接入 MCP 客户端

```json
{
  "mcpServers": {
    "km-data-sink-mcp": {
      "type": "stdio",
      "command": "java",
      "args": ["-jar", "/path/to/km-data-sink-mcp.jar", "--config", "/path/to/config.yaml"]
    }
  }
}
```

### 让 AI 完成一次完整落库

对客户端直接下达自然语言指令即可，AI 会按顺序调用工具：

```text
1. 列出可用连接并测试连接
2. 为任务 daily_ai_news 创建执行记录，run_id = daily_ai_news_20261120_090000
3. 把下面这篇 markdown 日报保存到 ai_news 表（format=md，meta 里带上 article_id 和 title）：
   ……（文档内容）……
4. 查一下这次执行的最终状态
```

对应的工具调用序列（等价于 AI 自动完成的操作）：

```jsonc
// 1. 确认连接
{"name": "test_connection", "arguments": {"connection": "default"}}

// 2. 开始执行留痕（T1）
{"name": "start_execution", "arguments": {
  "connection": "default", "task_name": "daily_ai_news",
  "run_id": "daily_ai_news_20261120_090000"}}

// 3. 文档落库（T2）
{"name": "save_document", "arguments": {
  "connection": "default", "table": "ai_news",
  "task_name": "daily_ai_news", "run_id": "daily_ai_news_20261120_090000",
  "business_date": "2026-11-20", "format": "md",
  "content": "# 今日 AI 新闻\n\n……",
  "meta": {"article_id": "a-20261120-01", "title": "今日 AI 新闻"}}}

// 4. 查询执行状态
{"name": "get_execution", "arguments": {
  "connection": "default", "task_name": "daily_ai_news",
  "run_id": "daily_ai_news_20261120_090000"}}
```

### 查询已落库文档

```jsonc
// 按 document_id 精确查回全文
{"name": "get_document", "arguments": {
  "connection": "default", "table": "ai_news",
  "document_id": "93b99b04-b226-468b-bc8a-bca1e3020f6d"}}

// 分页列表（不含全文），按业务日期过滤
{"name": "list_documents", "arguments": {
  "connection": "default", "table": "ai_news",
  "task_name": "daily_ai_news",
  "business_date_from": "2026-11-01", "business_date_to": "2026-11-20",
  "page": 1, "page_size": 20, "order_by": "created_at", "order_dir": "desc"}}
```

### 返回信封说明

所有工具返回统一结构：

```jsonc
{
  "success": true,                    // 收到 true 才算成功
  "code": "SUCCESS",                  // 错误码，见下节
  "message": "文档保存成功",
  "data": {
    "document_id": "93b99b04-...",
    "operation": "INSERT",            // INSERT / UPDATE（overwrite 命中）
    "table": "ai_news",
    "affected_rows": 1
  }
}
```

业务失败时 tool result 带 `isError(true)`，但结构不变——AI 客户端可直接读 `code`/`message` 决定重试或上报。

---

## API 参考 📚

### 工具一览（7 个）

| 工具 | 必填参数 | 可选参数 | 幂等性 |
|---|---|---|---|
| `list_connections` | 无 | 无 | 只读 |
| `test_connection` | `connection` | 无 | 只读 |
| `start_execution` | `connection` `task_name` `run_id` | `started_at` | 状态机幂等（见下） |
| `save_document` | `connection` `table` `task_name` `run_id` `content` `format` | `business_date` `meta` | upsert 幂等 |
| `get_document` | `connection` `table` | `document_id` 或完整 upsert_key 组合 | 只读 |
| `list_documents` | `connection` `table` | `task_name` `business_date_from/to` `format` `page` `page_size` `order_by` `order_dir` | 只读 |
| `get_execution` | `connection` `task_name` `run_id` | 无 | 只读 |

### start_execution 状态机

| 当前状态 | 行为 | 返回 |
|---|---|---|
| 无记录 | INSERT RUNNING | `SUCCESS` |
| RUNNING | 不动 | `EXECUTION_ALREADY_RUNNING` |
| SUCCESS | 不动（防覆盖已成功结果） | `EXECUTION_ALREADY_SUCCESS` |
| FAILED | 复用原记录，重置为 RUNNING | `SUCCESS` |

### save_document 行为矩阵

| 表 mode | upsert_key 命中 | 动作 | document_id |
|---|---|---|---|
| overwrite | 命中 | UPDATE 该行 | 保留原值 |
| overwrite | 未命中 | INSERT | 新生成 |
| history | 命中（同 task+run+format） | UPDATE 该行 | 保留原值 |
| history | 未命中 | INSERT 新行 | 新生成 |

辅助 `unique_keys` 在任何模式下冲突均拒绝（`UNIQUE_KEY_CONFLICT`），不静默更新。

### 错误码

| 错误码 | 场景 | 处理建议 |
|---|---|---|
| `SUCCESS` | 成功 | — |
| `INVALID_ARGUMENT` | 缺参数 / 格式错 / 必填列缺失 | 修正参数后重调 |
| `INVALID_CONNECTION` | 连接名不存在 | `list_connections` 核对 |
| `INVALID_TABLE` | 表不在白名单 | 改表名或加配置 |
| `INVALID_COLUMN` / `INVALID_QUERY_CONDITION` | 列非法 / 查询条件不完整 | 补全 upsert_key 组合 |
| `NON_UNIQUE_RESULT` | 按业务键查到多行 | 用 document_id 精确查 |
| `UNIQUE_KEY_CONFLICT` | 辅助唯一键冲突 | 换 article_id 等辅助键值 |
| `CONTENT_TOO_LARGE` | 超过 max_size_mb | 拆分文档或调大配置 |
| `DOCUMENT_NOT_FOUND` / `EXECUTION_NOT_FOUND` | 查无此文档/执行 | 核对 ID |
| `EXECUTION_ALREADY_RUNNING` / `EXECUTION_ALREADY_SUCCESS` | 重复开始执行 | 直接用 get_execution 看状态 |
| `DATABASE_CONNECTION_ERROR` / `DATABASE_TRANSACTION_ERROR` | 连接/事务失败 | test_connection 排查后重试 |
| `DATABASE_LOCK_TIMEOUT` | SQLite 写锁超时（重试耗尽） | 降低并发或延长 busy_timeout |
| `SCHEMA_VALIDATION_FAILED` | 执行记录表结构异常 | 删除该表让服务重建 |
| `INTERNAL_ERROR` | 未分类异常 | 看服务端日志 `logs/` |

### 数据库表结构

业务表 = YAML 声明的列 + 两个系统强制列：

| 系统列 | 类型 | 来源 | 说明 |
|---|---|---|---|
| `document_id` | VARCHAR(36) | `system.uuid` | 文档唯一 ID |
| `created_at` | DATETIME | `system.now` | 写入时间；自动建 `idx_order_by_<表名>` 索引 |

执行记录表 `mcp_task_execution`（每连接一张）：

| 列 | 类型 | 说明 |
|---|---|---|
| `id` | BIGINT 自增主键 | — |
| `task_name` / `run_id` | VARCHAR(128) | 联合唯一 `uk_task_run` |
| `status` | VARCHAR(20) | RUNNING / SUCCESS / FAILED |
| `started_at` / `finished_at` | DATETIME | 起止时间 |
| `error_code` / `error_message` | VARCHAR(64) / TEXT | 失败信息 |
| `document_id` | VARCHAR(36) | 关联写入的文档 |
| `table_name` | VARCHAR(128) | 写入的目标表 |
| `created_at` | DATETIME | 记录创建时间 |

---

## 部署指南 🛫

### 方式一：本地进程（默认形态）

MCP 客户端直接拉起 jar，适合个人使用。见 README「用户上手四步」。

### 方式二：Docker 常驻（服务器）

```bash
cd docker
cp ../target/km-data-sink-mcp.jar .          # jar 放进构建上下文
export SINK_DB_PASSWORD=你的MySQL密码
docker compose up -d --build
docker logs -f km-data-sink-mcp              # 出现"连接 mysql 执行记录表就绪"即成功
```

要点：

- `docker/docker-compose.yml` 加入 mysql 容器所在的外部网络（示例为 `kmdoker_default`），host 直接用容器名 `mysql`
- 密码通过环境变量 `SINK_DB_PASSWORD` 注入（compose 中引用 `${SINK_DB_PASSWORD:?}`，未设置拒绝启动）
- 日志持久化挂载 `./logs:/app/logs`
- healthcheck 检查主进程存活（stdio 服务无 HTTP 端点，不能用旁路 JVM 探针）

### 方式三：远程 MCP 接入（SSH 通道）

客户端与服务器不同机时，用 SSH 把 stdio 通道转发到容器：

```json
{
  "mcpServers": {
    "km-data-sink-mcp-remote": {
      "command": "ssh",
      "args": [
        "-o", "BatchMode=yes",
        "用户@服务器IP", "-p", "22",
        "docker exec -i km-data-sink-mcp java -jar /app/km-data-sink-mcp.jar --config /app/config.yaml"
      ]
    }
  }
}
```

前置：配置本机到服务器的 SSH 免密登录（公钥写入服务器 `~/.ssh/authorized_keys`）；`BatchMode=yes` 保证密钥失效时快速报错而非卡死。

---

## 常见问题 ❓

**Q1：启动报"缺少密码（password_env 未设置且 password 为空）"？**
`password_env` 的值是环境变量的**名字**（如 `SINK_DB_PASSWORD`），程序读取该变量的值作为密码。最容易犯的错是把密码本身填进 `password_env`。设置环境变量后 Windows 需完全重启 IDE/客户端才生效。

**Q2：客户端连接显示 Connection closed？**
按序排查：① `java -jar km-data-sink-mcp.jar --config config.yaml` 手工运行看是否报配置错误；② JDK 版本是否 17+；③ 日志（stderr 或 `logs/`）中连接初始化失败的根因链；④ MCP 客户端超时设置是否过短（首启建表可能需数秒）。

**Q3：写入返回 UNIQUE_KEY_CONFLICT？**
辅助唯一索引（`unique_keys`）冲突，如 `ai_news` 的 `article_id` 已存在。这是**拒绝而非覆盖**的设计：换辅助键值，或确认是否重复采集。

**Q4：overwrite 模式重复保存为什么 document_id 不变？**
覆盖模式命中 upsert_key 时执行 UPDATE 且保留原 document_id，保证外部引用稳定。想要新 ID 用 history 模式。

**Q5：SQLite 多个客户端同时写会锁库吗？**
WAL + busy_timeout 5000ms + 有限重试已覆盖常规并发（实测 6 进程 180/180 成功）。极端高并发下返回 `DATABASE_LOCK_TIMEOUT`，可调大 `sqlite.busy_timeout_ms` 或降低并发。

**Q6：旧表 list_documents 查询慢？**
V1.0.2 之前的表没有 `idx_order_by_<表名>` 索引，手动补建（SQL 见 README「验证与排查」）。新表自动创建。

**Q7：如何新增一张落库表？**
在 `tables.allowlist` 追加一段表声明（列/来源/模式/upsert_key），重启服务即可。`auto_create: true` 时表会在首次写入时自动创建。

**Q8：怎么确认定时任务真的成功了？**
以 `mcp_task_execution` 表为准：`SELECT * FROM mcp_task_execution WHERE task_name='xxx' ORDER BY created_at DESC`，status=SUCCESS 且 finished_at 非空即成功；也可让 AI 调 `get_execution`。

**Q9：文档最大能多大？**
默认 5MB（UTF-8 字节），超限返回 `CONTENT_TOO_LARGE` 且**不截断不落库**。可在 `content.max_size_mb` 调整。

**Q10：密码会出现在日志或 list_connections 里吗？**
不会。日志只输出连接名与失败根因；`list_connections` 返回连接名 + 类型 + 库名，不含密码与连接串。

---

## 附录

### 配置项速查

| 配置键 | 默认 | 说明 |
|---|---|---|
| `connections.<名>.type` | — | `mysql` / `sqlite` |
| `connections.<名>.host/port/database/username` | — | MySQL 必填 |
| `connections.<名>.password_env` | — | 密码环境变量名（与 `password` 二选一，优先环境变量） |
| `connections.<名>.pool_size` | 5 | SQLite 强制 1 |
| `connections.<名>.query_timeout_s` | 30 | 单语句超时 |
| `connections.<名>.sqlite.busy_timeout_ms` | 5000 | SQLite 锁等待 |
| `connections.<名>.sqlite.wal_mode` | true | WAL 模式 |
| `content.max_size_mb` | 5 | 单篇文档上限（UTF-8 字节） |
| `db.max_retry` | 3 | 可重试异常的最大重试次数 |
| `db.retry_backoff_ms` | 200 | 重试退避基数 |
| `query.default_page_size` / `max_page_size` | 20 / 100 | 分页默认与上限 |

### 相关文档

- 快速上手：[README.md](../README.md)
- 配置模板：[config.yaml.example](../config.yaml.example)
- Docker 部署：[docker/](../docker/)
