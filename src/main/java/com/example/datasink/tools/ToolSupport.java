package com.example.datasink.tools;

import com.example.datasink.common.ToolResult;
import com.example.datasink.engine.DataSinkException;
import io.modelcontextprotocol.spec.McpSchema.CallToolResult;

import java.util.Map;

/**
 * 工具层公共辅助：业务失败必须返回 isError(true) 的 CallToolResult，不得向客户端抛异常。
 */
final class ToolSupport {

    private ToolSupport() {
    }

    /** 成功结果 */
    static CallToolResult ok(ToolResult result) {
        return CallToolResult.builder()
                .structuredContent(result.toMap())
                .addTextContent(toJson(result.toMap()))
                .build();
    }

    /** 失败结果（isError=true，不抛异常给客户端） */
    static CallToolResult error(ToolResult result) {
        return CallToolResult.builder()
                .isError(true)
                .structuredContent(result.toMap())
                .addTextContent(toJson(result.toMap()))
                .build();
    }

    /** 未预期异常兜底：统一转 INTERNAL_ERROR，堆栈只写日志（由调用方日志） */
    static CallToolResult internalError(Throwable t) {
        ToolResult result = ToolResult.failure(
                DataSinkException.class.isInstance(t)
                        ? ((DataSinkException) t).getCode()
                        : com.example.datasink.engine.ErrorCode.INTERNAL_ERROR,
                safeMessage(t));
        return CallToolResult.builder()
                .isError(true)
                .structuredContent(result.toMap())
                .addTextContent(toJson(result.toMap()))
                .build();
    }

    private static String safeMessage(Throwable t) {
        String msg = t.getMessage() == null ? t.getClass().getSimpleName() : t.getMessage();
        // 不暴露敏感信息
        return msg.replaceAll("jdbc:[^\\s]+", "<jdbc-url>")
                .replaceAll("password[=:][^\\s&]+", "password=<masked>");
    }

    /** 简单 JSON 序列化（避免依赖 SDK 内部 Jackson 3 API 细节） */
    static String toJson(Map<String, Object> map) {
        StringBuilder sb = new StringBuilder();
        appendValue(sb, map);
        return sb.toString();
    }

    private static void appendValue(StringBuilder sb, Object v) {
        if (v == null) {
            sb.append("null");
        } else if (v instanceof Map<?, ?> m) {
            sb.append('{');
            boolean first = true;
            for (Map.Entry<?, ?> e : m.entrySet()) {
                if (!first) {
                    sb.append(',');
                }
                first = false;
                appendValue(sb, String.valueOf(e.getKey()));
                sb.append(':');
                appendValue(sb, e.getValue());
            }
            sb.append('}');
        } else if (v instanceof Iterable<?> it) {
            sb.append('[');
            boolean first = true;
            for (Object o : it) {
                if (!first) {
                    sb.append(',');
                }
                first = false;
                appendValue(sb, o);
            }
            sb.append(']');
        } else if (v instanceof Number || v instanceof Boolean) {
            sb.append(v);
        } else {
            sb.append('"').append(escape(String.valueOf(v))).append('"');
        }
    }

    private static String escape(String s) {
        StringBuilder sb = new StringBuilder();
        for (char c : s.toCharArray()) {
            switch (c) {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                default -> {
                    if (c < 0x20) {
                        sb.append(String.format("\\u%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
                }
            }
        }
        return sb.toString();
    }
}
