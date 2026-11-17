package com.example.datasink.tools;

import com.example.datasink.common.ToolResult;
import com.example.datasink.db.ConnectionManager;
import com.example.datasink.engine.DataSinkException;
import io.modelcontextprotocol.server.McpServerFeatures.SyncToolSpecification;
import io.modelcontextprotocol.spec.McpSchema.CallToolResult;
import io.modelcontextprotocol.spec.McpSchema.Tool;

import java.util.List;
import java.util.Map;

/**
 * test_connection：测试指定数据库连接是否可用，返回类型 + 版本（脱敏）。
 */
public class TestConnectionTool {

    private final ConnectionManager manager;

    public TestConnectionTool(ConnectionManager manager) {
        this.manager = manager;
    }

    public SyncToolSpecification specification() {
        Tool tool = Tool.builder("test_connection", Map.of(
                        "type", "object",
                        "properties", Map.of(
                                "connection", Map.of("type", "string", "description", "连接名")),
                        "required", List.of("connection")))
                .description("测试指定数据库连接是否可用，返回数据库类型与版本")
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
        String connection = str(args, "connection");
        if (connection == null || connection.isBlank()) {
            return ToolSupport.error(ToolResult.failure(
                    com.example.datasink.engine.ErrorCode.INVALID_ARGUMENT, "缺少必填参数: connection"));
        }
        if (!manager.hasConnection(connection)) {
            return ToolSupport.error(ToolResult.failure(
                    com.example.datasink.engine.ErrorCode.INVALID_CONNECTION,
                    "连接不存在: " + connection + "（可用连接见 list_connections）"));
        }
        String version = manager.testConnection(connection);
        return ToolSupport.ok(ToolResult.success("连接可用",
                Map.of("connection", connection, "database", version)));
    }

    static String str(Map<String, Object> args, String key) {
        Object v = args.get(key);
        return v == null ? null : String.valueOf(v);
    }
}
