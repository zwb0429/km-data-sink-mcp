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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * get_document：按 document_id 或业务唯一键查回完整文档（P1-⑥）。
 */
public class GetDocumentTool {

    private final ConnectionManager manager;
    private final DocumentQueryService queryService;
    private final AppConfig appConfig;

    public GetDocumentTool(ConnectionManager manager, DocumentQueryService queryService, AppConfig appConfig) {
        this.manager = manager;
        this.queryService = queryService;
        this.appConfig = appConfig;
    }

    public SyncToolSpecification specification() {
        Tool tool = Tool.builder("get_document", Map.of(
                        "type", "object",
                        "properties", new java.util.LinkedHashMap<>(Map.ofEntries(
                                Map.entry("connection", Map.of("type", "string", "description", "连接名")),
                                Map.entry("table", Map.of("type", "string", "description", "表名")),
                                Map.entry("document_id", Map.of("type", "string",
                                        "description", "文档 ID（优先使用）")),
                                Map.entry("task_name", Map.of("type", "string",
                                        "description", "业务键查询: 任务名（需与其余 upsert_key 字段一起提供）")),
                                Map.entry("business_date", Map.of("type", "string", "description", "业务日期")),
                                Map.entry("format", Map.of("type", "string", "description", "html/md")))),
                        "required", List.of("connection", "table")))
                .description("查回完整文档。优先 document_id；否则必须提供完整 upsert_key 字段组合，"
                        + "条件不完整返回 INVALID_QUERY_CONDITION，结果不唯一返回 NON_UNIQUE_RESULT")
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
        String documentId = TestConnectionTool.str(args, "document_id");

        if (connection == null || connection.isBlank()) {
            return err(ErrorCode.INVALID_ARGUMENT, "缺少必填参数: connection");
        }
        if (!manager.hasConnection(connection)) {
            return err(ErrorCode.INVALID_CONNECTION, "连接不存在: " + connection);
        }
        if (tableName == null || tableName.isBlank()) {
            return err(ErrorCode.INVALID_ARGUMENT, "缺少必填参数: table");
        }
        TableConfig table = appConfig.getTables().findTable(tableName);
        if (table == null) {
            return err(ErrorCode.INVALID_TABLE, "表不在白名单中: " + tableName);
        }

        Map<String, Object> doc;
        if (documentId != null && !documentId.isBlank()) {
            // 规则 1：优先 document_id
            doc = queryService.getByDocumentId(connection, table, documentId);
        } else {
            // 规则 2：业务唯一键（必须完整 upsert_key 组合；不完整由服务抛 INVALID_QUERY_CONDITION）
            doc = queryService.getByBusinessKey(connection, table, args);
        }
        return ToolSupport.ok(ToolResult.success("查询成功", doc));
    }

    private CallToolResult err(ErrorCode code, String msg) {
        return ToolSupport.error(ToolResult.failure(code, msg));
    }
}
