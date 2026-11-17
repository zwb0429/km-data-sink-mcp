package com.example.datasink.tools;

import com.example.datasink.common.ToolResult;
import com.example.datasink.config.ConnectionConfig;
import com.example.datasink.db.ConnectionManager;
import com.example.datasink.engine.DataSinkException;
import com.example.datasink.engine.ErrorCode;
import io.modelcontextprotocol.server.McpServerFeatures.SyncToolSpecification;
import io.modelcontextprotocol.spec.McpSchema.CallToolResult;
import io.modelcontextprotocol.spec.McpSchema.Tool;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * list_connections：列出可用连接名（脱敏，不返回密码与完整连接串）。
 */
public class ListConnectionsTool {

    private final ConnectionManager manager;

    public ListConnectionsTool(ConnectionManager manager) {
        this.manager = manager;
    }

    public SyncToolSpecification specification() {
        Tool tool = Tool.builder("list_connections", Map.of(
                        "type", "object",
                        "properties", Map.of(),
                        "required", List.of()))
                .description("列出可用的数据库连接名（脱敏，不含密码与连接串）")
                .build();
        return SyncToolSpecification.builder()
                .tool(tool)
                .callHandler((exchange, request) -> {
                    try {
                        return ok();
                    } catch (DataSinkException e) {
                        return ToolSupport.error(ToolResult.failure(e.getCode(), e.getMessage()));
                    } catch (Exception e) {
                        return ToolSupport.internalError(e);
                    }
                })
                .build();
    }

    private CallToolResult ok() {
        List<Map<String, Object>> items = new ArrayList<>();
        // 用 TreeMap 保证输出顺序稳定
        new TreeMap<>(manager.appConfig().getConnections()).forEach((name, conn) -> {
            items.add(Map.of(
                    "name", name,
                    "type", conn.getType(),
                    "database", descDb(conn)));
        });
        return ToolSupport.ok(ToolResult.success("可用连接列表", Map.of("connections", items)));
    }

    /** 脱敏描述：MySQL 只显示 database 名；SQLite 只显示文件名 */
    private String descDb(ConnectionConfig conn) {
        if (ConnectionConfig.TYPE_MYSQL.equalsIgnoreCase(conn.getType())) {
            return conn.getDatabase() == null ? "" : conn.getDatabase();
        }
        if (conn.getPath() == null) {
            return "";
        }
        int idx = Math.max(conn.getPath().lastIndexOf('/'), conn.getPath().lastIndexOf('\\'));
        return idx >= 0 ? conn.getPath().substring(idx + 1) : conn.getPath();
    }
}
