package com.example.datasink.tools;

import com.example.datasink.common.ToolResult;
import com.example.datasink.config.AppConfig;
import com.example.datasink.config.ConnectionConfig;
import com.example.datasink.config.TableConfig;
import com.example.datasink.db.ConnectionManager;
import com.example.datasink.engine.DataSinkException;
import com.example.datasink.engine.ErrorCode;
import com.example.datasink.engine.ExecutionRecorder;
import com.example.datasink.engine.SchemaManager;
import com.example.datasink.engine.UpsertWriter;
import com.example.datasink.engine.ValueResolver;
import io.modelcontextprotocol.server.McpServerFeatures.SyncToolSpecification;
import io.modelcontextprotocol.spec.McpSchema.CallToolResult;
import io.modelcontextprotocol.spec.McpSchema.Tool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * save_document（核心工具）：文档落库 + 自动建表 + 执行记录更新（同事务 T2）。
 * 失败补偿：T3 独立事务更新 FAILED。
 */
public class SaveDocumentTool {

    private static final Logger log = LoggerFactory.getLogger(SaveDocumentTool.class);

    private final ConnectionManager manager;
    private final SchemaManager schemaManager;
    private final UpsertWriter writer;
    private final ExecutionRecorder recorder;
    private final ValueResolver resolver = new ValueResolver();
    private final AppConfig appConfig;

    public SaveDocumentTool(ConnectionManager manager, SchemaManager schemaManager,
                            UpsertWriter writer, ExecutionRecorder recorder, AppConfig appConfig) {
        this.manager = manager;
        this.schemaManager = schemaManager;
        this.writer = writer;
        this.recorder = recorder;
        this.appConfig = appConfig;
    }

    public SyncToolSpecification specification() {
        Map<String, Object> formatSchema = Map.of(
                "type", "string",
                "description", "文档格式: html / md");
        Map<String, Object> metaSchema = Map.of(
                "type", "object",
                "description", "元数据对象，供字段映射 meta.xxx 抽取",
                "additionalProperties", true);
        Tool tool = Tool.builder("save_document", Map.of(
                        "type", "object",
                        "properties", new LinkedHashMap<>(Map.of(
                                "connection", Map.of("type", "string", "description", "连接名"),
                                "table", Map.of("type", "string", "description", "表名（须在 tables.allowlist 白名单中）"),
                                "task_name", Map.of("type", "string", "description", "任务名称"),
                                "run_id", Map.of("type", "string", "description", "执行编号（每次执行唯一）"),
                                "business_date", Map.of("type", "string",
                                        "description", "业务日期 yyyy-MM-dd（覆盖模式必填）"),
                                "format", formatSchema,
                                "content", Map.of("type", "string", "description", "文档全文"),
                                "meta", metaSchema)),
                        "required", List.of("connection", "table", "task_name", "run_id", "content", "format")))
                .description("将 .html/.md 文档全文保存到配置的数据库表（自动建表、幂等 upsert、执行记录同事务更新）。"
                        + "收到 success=true 才算保存成功")
                .build();
        return SyncToolSpecification.builder()
                .tool(tool)
                .callHandler((exchange, request) -> {
                    try {
                        return handle(request.arguments());
                    } catch (DataSinkException e) {
                        // 失败补偿 T3：FAILED 独立事务
                        markFailedIfPossible(request.arguments(), e);
                        return ToolSupport.error(ToolResult.failure(e.getCode(), e.getMessage()));
                    } catch (Exception e) {
                        log.error("save_document 未预期异常", e);
                        markFailedIfPossible(request.arguments(), new DataSinkException(
                                ErrorCode.INTERNAL_ERROR, "内部错误"));
                        return ToolSupport.internalError(e);
                    }
                })
                .build();
    }

    private CallToolResult handle(Map<String, Object> args) {
        // ---- 参数校验 ----
        String connection = TestConnectionTool.str(args, "connection");
        String tableName = TestConnectionTool.str(args, "table");
        String taskName = TestConnectionTool.str(args, "task_name");
        String runId = TestConnectionTool.str(args, "run_id");
        String businessDate = TestConnectionTool.str(args, "business_date");
        String format = TestConnectionTool.str(args, "format");
        String content = TestConnectionTool.str(args, "content");

        if (connection == null || connection.isBlank()) {
            return err(ErrorCode.INVALID_ARGUMENT, "缺少必填参数: connection");
        }
        if (!manager.hasConnection(connection)) {
            return err(ErrorCode.INVALID_CONNECTION, "连接不存在: " + connection);
        }
        ConnectionConfig connCfg = manager.connectionConfig(connection);
        if (!connCfg.getType().equalsIgnoreCase(manager.dialect(connection).dbType())) {
            return err(ErrorCode.INVALID_CONNECTION, "连接类型不匹配: " + connection);
        }
        if (tableName == null || tableName.isBlank()) {
            return err(ErrorCode.INVALID_ARGUMENT, "缺少必填参数: table");
        }
        TableConfig table = appConfig.getTables().findTable(tableName);
        if (table == null) {
            return err(ErrorCode.INVALID_TABLE, "表不在白名单中: " + tableName);
        }
        if (taskName == null || taskName.isBlank()) {
            return err(ErrorCode.INVALID_ARGUMENT, "缺少必填参数: task_name");
        }
        if (runId == null || runId.isBlank()) {
            return err(ErrorCode.INVALID_ARGUMENT, "缺少必填参数: run_id");
        }
        if (content == null || content.isEmpty()) {
            return err(ErrorCode.INVALID_ARGUMENT, "缺少必填参数: content");
        }
        if (format == null || format.isBlank()) {
            return err(ErrorCode.INVALID_ARGUMENT, "缺少必填参数: format");
        }
        format = format.toLowerCase();
        if (!format.equals("html") && !format.equals("md")) {
            return err(ErrorCode.INVALID_ARGUMENT, "format 仅允许 html/md，当前: " + format);
        }
        // 内容长度限制（P0-⑥：UTF-8 字节，超限拒绝，不静默截断）
        long maxBytes = appConfig.getContent().maxBytes();
        long byteLen = content.getBytes(StandardCharsets.UTF_8).length;
        if (byteLen > maxBytes) {
            return err(ErrorCode.CONTENT_TOO_LARGE,
                    String.format("文档超过上限（%d 字节 > %d 字节，%.2f MB 上限）", byteLen, maxBytes,
                            appConfig.getContent().getMaxSizeMb()));
        }
        // 覆盖模式 business_date 必填（upsert_key 含 business_date 时）
        if (table.getUpsertKey().contains("business_date")
                && (businessDate == null || businessDate.isBlank())) {
            return err(ErrorCode.INVALID_ARGUMENT,
                    "覆盖模式（upsert_key 含 business_date）business_date 必填");
        }
        if (businessDate != null && !businessDate.isBlank()
                && !businessDate.matches("\\d{4}-\\d{2}-\\d{2}")) {
            return err(ErrorCode.INVALID_ARGUMENT, "business_date 格式须为 yyyy-MM-dd");
        }

        // ---- 执行记录：隐式创建 RUNNING（D10：兼容未显式 start_execution） ----
        try {
            recorder.getExecution(connection, taskName, runId);
        } catch (DataSinkException e) {
            if (e.getCode() == ErrorCode.EXECUTION_NOT_FOUND) {
                recorder.startExecution(connection, taskName, runId, null);
            } else {
                throw e;
            }
        }

        // ---- 表就绪：auto_create 建表或结构校验 ----
        table.ensureSystemColumns(); // 系统强制列兜底（方案 4.1），建表 DDL 包含 document_id/created_at
        ensureTable(connection, table);

        // ---- 值解析 ----
        Map<String, Object> resolveArgs = new LinkedHashMap<>(args);
        if (businessDate != null) {
            resolveArgs.put("business_date", businessDate);
        }
        resolveArgs.put("format", format);
        resolveArgs.put("content", content);
        Map<String, Object> row = resolver.resolve(table, resolveArgs, manager.dialect(connection));

        // ---- T2：写入 + SUCCESS 同事务 ----
        UpsertWriter.SaveResult result = writer.save(connection, table, row, taskName, runId);

        Map<String, Object> data = new LinkedHashMap<>();
        data.put("document_id", result.documentId());
        data.put("task_name", taskName);
        data.put("run_id", runId);
        data.put("format", format);
        data.put("table", tableName);
        data.put("affected_rows", 1); // 归一化：覆盖/插入均返回 1
        data.put("operation", result.updated() ? "update" : "insert");
        return ToolSupport.ok(ToolResult.success("文档保存成功", data));
    }

    /** 自动建表或结构校验（P1-⑨） */
    private void ensureTable(String connection, TableConfig table) {
        boolean autoCreate = appConfig.getTables().isAutoCreate();
        List<String> diffs = schemaManager.validateBusinessTable(connection, table);
        if (diffs.size() == 1 && diffs.get(0).contains("不存在")) {
            if (autoCreate) {
                schemaManager.ensureBusinessTable(connection, table);
                return;
            }
            throw new DataSinkException(ErrorCode.SCHEMA_VALIDATION_FAILED,
                    "表不存在且 auto_create=false: " + table.getName());
        }
        if (!diffs.isEmpty()) {
            throw new DataSinkException(ErrorCode.SCHEMA_VALIDATION_FAILED,
                    "表结构校验失败（禁止写入）: " + String.join("; ", diffs));
        }
    }

    /** 系统强制列兜底逻辑已迁移至 TableConfig.ensureSystemColumns() */

    private void markFailedIfPossible(Map<String, Object> args, DataSinkException e) {
        try {
            String connection = TestConnectionTool.str(args, "connection");
            String taskName = TestConnectionTool.str(args, "task_name");
            String runId = TestConnectionTool.str(args, "run_id");
            if (connection != null && taskName != null && runId != null && manager.hasConnection(connection)) {
                recorder.markFailed(connection, taskName, runId, e.getCode().name(), e.getMessage());
            }
        } catch (Exception ex) {
            log.error("T3 FAILED 更新异常（保留待恢复标记）", ex);
        }
    }

    private CallToolResult err(ErrorCode code, String msg) {
        return ToolSupport.error(ToolResult.failure(code, msg));
    }
}
