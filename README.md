# km-data-sink-mcp

通用数据落库 MCP Server（V1.0.2）。任意支持 MCP 的 AI 客户端（WorkBuddy / Claude Desktop / Cursor / VS Code）可通过 stdio 调用，把采集/生成的 `.html` / `.md` 文档全文可靠保存到配置的数据库中。

- **技术栈**：Java 17+、MCP 官方 SDK 2.1.0（经典 server + stdio）、JDBC + HikariCP、YAML 配置驱动
- **数据库**：SQLite + MySQL（V1.0 正式支持；PostgreSQL / SQL Server 为 P2 扩展位）
- **不引入** Spring Boot / MyBatis / JPA

## 核心特性

| 特性 | 说明 |
|---|---|
| 配置即 schema | 数据库连接、表结构、字段来源、类型、唯一键全部 YAML 声明，新增任务类型无需改代码 |
| 幂等 upsert | 单组 `upsert_key` 先查后写；辅助 `unique_keys` 冲突即拒绝（`UNIQUE_KEY_CONFLICT`） |
| 执行记录 | `mcp_task_execution` 表（每连接独立），RUNNING / SUCCESS / FAILED 全程留痕 |
| 事务一致性 | T1（RUNNING）/T2（文档+SUCCESS）/T3（FAILED）事务边界明确，失败不留半条数据 |
| document_id | 服务端 UUID 生成（VARCHAR(36)），overwrite 模式更新保留原值 |
| 内容限制 | 单篇文档上限 `content.max_size_mb`（默认 5MB，UTF-8 字节），超限拒绝不截断 |
| SQLite 并发 | 单进程连接池强制 1；多进程靠 WAL + busy_timeout + 有限重试缓解 |
| 安全 | 表/列白名单、参数化绑定、排序字段白名单、密码环境变量注入、错误信息脱敏 |

## 构建与运行

```bash
mvn package          # 产出 target/km-data-sink-mcp.jar（fat jar）
java -jar target/km-data-sink-mcp.jar --config /path/to/config.yaml
```

配置文件：复制 `config.yaml.example` 为 `config.yaml`，按需修改；MySQL 密码通过环境变量 `SINK_DB_PASSWORD` 注入。

## MCP 工具（7 个）

| 工具 | 作用 |
|---|---|
| `start_execution` | 创建/重置任务执行记录（RUNNING，独立事务） |
| `save_document` | 文档落库 + 自动建表 + 执行记录更新（同事务） |
| `get_document` | 按 `document_id` 或完整 `upsert_key` 查回完整文档 |
| `list_documents` | 分页查询文档列表（不含全文，只返回摘要 + 元数据） |
| `get_execution` | 查询任务执行记录 |
| `test_connection` | 测试连接，返回类型 + 版本 |
| `list_connections` | 列出可用连接名（脱敏） |

所有工具返回统一信封：`{success, code, message, data}`；业务失败返回 `isError(true)` 的 tool result，不向客户端抛异常。

### 错误码

`SUCCESS` / `INVALID_ARGUMENT` / `INVALID_CONNECTION` / `INVALID_TABLE` / `INVALID_COLUMN` / `INVALID_QUERY_CONDITION` / `NON_UNIQUE_RESULT` / `UNIQUE_KEY_CONFLICT` / `CONTENT_TOO_LARGE` / `DOCUMENT_NOT_FOUND` / `EXECUTION_ALREADY_RUNNING` / `EXECUTION_ALREADY_SUCCESS` / `EXECUTION_NOT_FOUND` / `DATABASE_CONNECTION_ERROR` / `DATABASE_TRANSACTION_ERROR` / `DATABASE_LOCK_TIMEOUT` / `SCHEMA_VALIDATION_FAILED` / `INTERNAL_ERROR`

## 定时任务约定

1. 任务开始先调用 `start_execution`：`run_id = {task_name}_{business_date}_{HHmmss}`（每次执行唯一，含时间戳）。
2. 获取并整理数据后调用 `save_document`：`format` 按内容类型指定 `html`/`md`，`content` 为完整文档，`meta` 放结构化元数据。
3. 收到 `success: true` 才算完成；失败时如实返回错误。
4. 同一天重新生成时用**新的 `run_id`**：`overwrite` 模式自动覆盖同日旧文档，`history` 模式保留历史版本。

## 多端接入

WorkBuddy（`~/.workbuddy/mcp.json`）：

```json
{
  "mcpServers": {
    "km-data-sink-mcp": {
      "type": "stdio",
      "command": "java",
      "args": ["-jar", "D:/work/km-data-sink-mcp/target/km-data-sink-mcp.jar", "--config", "D:/work/km-data-sink-mcp/config.yaml"]
    }
  }
}
```

Claude Desktop / Cursor / VS Code 配置一致（去掉 `type` 字段即可）。

## 测试

```bash
mvn test                                # SQLite 全用例（T1–T38 SQLite 部分）
mvn test -Dtest=MySqlIntegrationTest    # MySQL 用例（需本地 MySQL，见测试内说明）
```

## 日志

日志只写 `System.err` 与 `logs/km-data-sink-mcp.log`，绝不污染 `System.out`（stdio 协议通道）。
