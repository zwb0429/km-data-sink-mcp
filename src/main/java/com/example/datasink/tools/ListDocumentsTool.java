package com.example.datasink.tools;

import com.example.datasink.common.ToolResult;
import com.example.datasink.config.AppConfig;
import com.example.datasink.config.TableConfig;
import com.example.datasink.db.ConnectionManager;
import com.example.datasink.engine.DataSinkException;
import com.example.datasink.engine.DocumentQueryService;
import com.example.datasink.engine.ErrorCode;
import io.modelcontextprotocol.server.McpServerFeatures.SyncToolSpecification;
import io.modelcontextprotocol.spec.McpSchema.CallToolResult;
import io.modelcontextprotocol.spec.McpSchema.Tool;

import java.util.List;
import java.util.Map;

/**
 * list_documents：分页查询文档列表（不含全文，P1-⑪）。
 */
public class ListDocumentsTool {

    private final ConnectionManager manager;
    private final DocumentQueryService queryService;
    private final AppConfig appConfig;

    public ListDocumentsTool(ConnectionManager manager, DocumentQueryService queryService, AppConfig appConfig) {
        this.manager = manager;
        this.queryService = queryService;
        this.appConfig = appConfig;
    }

    public SyncToolSpecification specification() {
        Tool tool = Tool.builder("list_documents", Map.of(
                        "type", "object",
                        "properties", new java.util.LinkedHashMap<>(Map.ofEntries(
                                Map.entry("connection", Map.of("type", "string", "description", "连接名")),
                                Map.entry("table", Map.of("type", "string", "description", "表名")),
                                Map.entry("task_name", Map.of("type", "string", "description", "任务名筛选")),
                                Map.entry("business_date_from", Map.of("type", "string",
                                        "description", "业务日期范围起 yyyy-MM-dd")),
                                Map.entry("business_date_to", Map.of("type", "string",
                                        "description", "业务日期范围止 yyyy-MM-dd")),
                                Map.entry("format", Map.of("type", "string", "description", "格式筛选 html/md")),
                                Map.entry("page", Map.of("type", "integer", "description", "页码，从 1 开始，默认 1")),
                                Map.entry("page_size", Map.of("type", "integer",
                                        "description", "每页条数，默认 20，最大 100")),
                                Map.entry("order_by", Map.of("type", "string",
                                        "description", "排序字段（须在表 columns 白名单中），默认 created_at")),
                                Map.entry("order_dir", Map.of("type", "string", "description", "asc/desc，默认 desc")))),
                        "required", List.of("connection", "table")))
                .description("分页查询文档列表（不含 content 全文，返回摘要 + 元数据含 document_id）")
                .build();
        return SyncToolSpecification.builder()
                .tool(tool)
                .callHandler((exchange, request) -> {
                    try {
                        return handle(request.arguments());
                    } catch (DataSinkException e) {
                        return ToolSupport.error(ToolResult.failure(e.getCode(), e.getMessage()));
                    } catch (Exception e) {
                        return ToolSupport.internalError(e);
                    }
                })
                .build();
    }

    private CallToolResult handle(Map<String, Object> args) {
        String connection = TestConnectionTool.str(args, "connection");
        String tableName = TestConnectionTool.str(args, "table");
        if (connection == null || connection.isBlank()) {
            return ToolSupport.error(ToolResult.failure(ErrorCode.INVALID_ARGUMENT, "缺少必填参数: connection"));
        }
        if (!manager.hasConnection(connection)) {
            return ToolSupport.error(ToolResult.failure(ErrorCode.INVALID_CONNECTION, "连接不存在: " + connection));
        }
        if (tableName == null || tableName.isBlank()) {
            return ToolSupport.error(ToolResult.failure(ErrorCode.INVALID_ARGUMENT, "缺少必填参数: table"));
        }
        TableConfig table = appConfig.getTables().findTable(tableName);
        if (table == null) {
            return ToolSupport.error(ToolResult.failure(ErrorCode.INVALID_TABLE, "表不在白名单中: " + tableName));
        }
        Map<String, Object> data = queryService.listDocuments(connection, table, args);
        return ToolSupport.ok(ToolResult.success("查询成功", data));
    }
}
