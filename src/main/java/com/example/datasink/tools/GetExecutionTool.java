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
 * get_execution：查询任务执行记录（状态、起止时间、错误信息、document_id）。
 */
public class GetExecutionTool {

    private final ConnectionManager manager;
    private final ExecutionRecorder recorder;

    public GetExecutionTool(ConnectionManager manager, ExecutionRecorder recorder) {
        this.manager = manager;
        this.recorder = recorder;
    }

    public SyncToolSpecification specification() {
        Tool tool = Tool.builder("get_execution", Map.of(
                        "type", "object",
                        "properties", Map.of(
                                "connection", Map.of("type", "string", "description", "连接名"),
                                "task_name", Map.of("type", "string", "description", "任务名称"),
                                "run_id", Map.of("type", "string", "description", "执行编号")),
                        "required", List.of("connection", "task_name", "run_id")))
                .description("查询任务执行记录（状态/起止时间/错误信息/document_id）。查不到返回 EXECUTION_NOT_FOUND")
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
        if (connection == null || connection.isBlank()) {
            return err(ErrorCode.INVALID_ARGUMENT, "缺少必填参数: connection");
        }
        if (!manager.hasConnection(connection)) {
            return err(ErrorCode.INVALID_CONNECTION, "连接不存在: " + connection);
        }
        if (taskName == null || taskName.isBlank()) {
            return err(ErrorCode.INVALID_ARGUMENT, "缺少必填参数: task_name");
        }
        if (runId == null || runId.isBlank()) {
            return err(ErrorCode.INVALID_ARGUMENT, "缺少必填参数: run_id");
        }
        ExecutionRecorder.Execution e = recorder.getExecution(connection, taskName, runId);
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("task_name", e.taskName());
        data.put("run_id", e.runId());
        data.put("status", e.status());
        data.put("started_at", e.startedAt());
        data.put("finished_at", e.finishedAt());
        data.put("error_code", e.errorCode());
        data.put("error_message", e.errorMessage());
        data.put("document_id", e.documentId());
        data.put("table_name", e.tableName());
        return ToolSupport.ok(ToolResult.success("查询成功", data));
    }

    private CallToolResult err(ErrorCode code, String msg) {
        return ToolSupport.error(ToolResult.failure(code, msg));
    }
}
