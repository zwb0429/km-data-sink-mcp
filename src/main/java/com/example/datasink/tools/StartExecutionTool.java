package com.example.datasink.tools;

import com.example.datasink.common.ToolResult;
import com.example.datasink.db.ConnectionManager;
import com.example.datasink.engine.DataSinkException;
import com.example.datasink.engine.ErrorCode;
import com.example.datasink.engine.ExecutionRecorder;
import io.modelcontextprotocol.server.McpServerFeatures.SyncToolSpecification;
import io.modelcontextprotocol.spec.McpSchema.CallToolResult;
import io.modelcontextprotocol.spec.McpSchema.Tool;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * start_execution：创建/重置任务执行记录（RUNNING，独立事务 T1）。
 */
public class StartExecutionTool {

    private final ConnectionManager manager;
    private final ExecutionRecorder recorder;

    public StartExecutionTool(ConnectionManager manager, ExecutionRecorder recorder) {
        this.manager = manager;
        this.recorder = recorder;
    }

    public SyncToolSpecification specification() {
        Tool tool = Tool.builder("start_execution", Map.of(
                        "type", "object",
                        "properties", Map.of(
                                "connection", Map.of("type", "string", "description", "连接名"),
                                "task_name", Map.of("type", "string", "description", "任务名称"),
                                "run_id", Map.of("type", "string",
                                        "description", "执行编号，每次执行唯一（推荐 {task_name}_{business_date}_{HHmmss}）"),
                                "started_at", Map.of("type", "string",
                                        "description", "开始时间（可选，省略取服务端当前时间）")),
                        "required", List.of("connection", "task_name", "run_id")))
                .description("创建任务执行记录（RUNNING 状态，独立事务）。已 RUNNING 返回 EXECUTION_ALREADY_RUNNING；"
                        + "已 SUCCESS 返回 EXECUTION_ALREADY_SUCCESS；已 FAILED 复用原记录重置为 RUNNING")
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
        String taskName = TestConnectionTool.str(args, "task_name");
        String runId = TestConnectionTool.str(args, "run_id");
        String startedAt = TestConnectionTool.str(args, "started_at");

        if (connection == null || connection.isBlank()) {
            return err(ErrorCode.INVALID_ARGUMENT, "缺少必填参数: connection");
        }
        if (taskName == null || taskName.isBlank()) {
            return err(ErrorCode.INVALID_ARGUMENT, "缺少必填参数: task_name");
        }
        if (runId == null || runId.isBlank()) {
            return err(ErrorCode.INVALID_ARGUMENT, "缺少必填参数: run_id");
        }
        if (!manager.hasConnection(connection)) {
            return err(ErrorCode.INVALID_CONNECTION, "连接不存在: " + connection);
        }

        ExecutionRecorder.Execution e = recorder.startExecution(connection, taskName, runId, startedAt);
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("task_name", e.taskName());
        data.put("run_id", e.runId());
        data.put("status", e.status());
        data.put("started_at", e.startedAt());
        return ToolSupport.ok(ToolResult.success("执行记录已创建（RUNNING）", data));
    }

    private CallToolResult err(ErrorCode code, String msg) {
        return ToolSupport.error(ToolResult.failure(code, msg));
    }
}
