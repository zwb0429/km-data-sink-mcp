package com.example.datasink.common;

import com.example.datasink.engine.ErrorCode;

import java.util.Map;

/**
 * 统一返回信封（success / code / message / data）。
 * 通过 CallToolResult 的 structuredContent 输出，同时附带文本内容兼容旧客户端。
 */
public record ToolResult(boolean success, String code, String message, Object data) {

    public static ToolResult success(String message, Object data) {
        return new ToolResult(true, ErrorCode.SUCCESS.name(), message, data);
    }

    public static ToolResult failure(ErrorCode code, String message) {
        return new ToolResult(false, code.name(), message, null);
    }

    public static ToolResult failure(ErrorCode code, String message, Object data) {
        return new ToolResult(false, code.name(), message, data);
    }

    /** 转为结构化 Map（用于 structuredContent） */
    public Map<String, Object> toMap() {
        return new java.util.LinkedHashMap<>(Map.of(
                "success", success,
                "code", code,
                "message", message,
                "data", data == null ? Map.of() : data));
    }
}
