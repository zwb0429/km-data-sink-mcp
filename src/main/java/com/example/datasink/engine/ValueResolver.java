package com.example.datasink.engine;

import com.example.datasink.config.ColumnConfig;
import com.example.datasink.config.TableConfig;
import com.example.datasink.db.Dialect;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 字段映射解析（方案 8.1）：source 语法 → 取值。
 * 未在 source 声明的 meta 字段忽略，防注入额外列。
 */
public class ValueResolver {

    /**
     * 根据表定义解析所有列值。
     *
     * @param table        表配置
     * @param args         工具调用参数（含 content/format/task_name/run_id/business_date/meta）
     * @param dialect      方言（system.now 落库格式）
     * @return 列名 → 值（有序，按 columns 声明顺序）
     * @throws DataSinkException INVALID_ARGUMENT（必填字段缺失/为空）
     */
    public Map<String, Object> resolve(TableConfig table, Map<String, Object> args, Dialect dialect) {
        Map<String, Object> row = new LinkedHashMap<>();
        @SuppressWarnings("unchecked")
        Map<String, Object> meta = args.get("meta") instanceof Map<?, ?> m
                ? (Map<String, Object>) m : Map.of();

        for (ColumnConfig col : table.getColumns()) {
            Object value = resolveOne(col, args, meta, dialect);
            if (value == null && !col.isNullable()) {
                // 系统列不会为 null；调用方列 null 且 NOT NULL → INVALID_ARGUMENT
                throw new DataSinkException(ErrorCode.INVALID_ARGUMENT,
                        "必填字段缺失或为空: " + col.getColumn() + "（source=" + col.getSource() + "）");
            }
            row.put(col.getColumn(), value);
        }
        return row;
    }

    private Object resolveOne(ColumnConfig col, Map<String, Object> args,
                              Map<String, Object> meta, Dialect dialect) {
        String src = col.getSource();
        if (ColumnConfig.SOURCE_SYSTEM_UUID.equals(src)) {
            return DocumentIdGenerator.generate();
        }
        if (ColumnConfig.SOURCE_SYSTEM_NOW.equals(src)) {
            return dialect.formatNow();
        }
        if (col.isTopLevelSource()) {
            return coerce(col, args.get(src));
        }
        if (col.isMetaSource()) {
            String path = src.substring("meta.".length());
            Object v = lookupPath(meta, path);
            return coerce(col, v);
        }
        throw new DataSinkException(ErrorCode.INVALID_COLUMN,
                "列 " + col.getColumn() + " 的 source 非法: " + src);
    }

    /** 支持嵌套路径 a.b.c */
    @SuppressWarnings("unchecked")
    private Object lookupPath(Map<String, Object> meta, String path) {
        String[] parts = path.split("\\.");
        Object cur = meta;
        for (String p : parts) {
            if (!(cur instanceof Map<?, ?> m)) {
                return null;
            }
            cur = m.get(p);
        }
        return cur;
    }

    /** 按通用类型做基础类型收敛（不做格式强校验，原文入库） */
    private Object coerce(ColumnConfig col, Object raw) {
        if (raw == null) {
            return null;
        }
        String t = col.getType() == null ? "" : col.getType().toUpperCase();
        if (t.startsWith("DECIMAL")) {
            if (raw instanceof Number n) {
                return n;
            }
            return new java.math.BigDecimal(raw.toString());
        }
        if (t.equals("INT")) {
            if (raw instanceof Number n) {
                return n.intValue();
            }
            return Integer.parseInt(raw.toString());
        }
        if (t.equals("BIGINT")) {
            if (raw instanceof Number n) {
                return n.longValue();
            }
            return Long.parseLong(raw.toString());
        }
        if (t.equals("BOOLEAN")) {
            if (raw instanceof Boolean b) {
                return b;
            }
            return Boolean.parseBoolean(raw.toString());
        }
        if (t.equals("DATE")) {
            if (raw instanceof String s && !s.isBlank()) {
                // 归一化 ISO 日期 yyyy-MM-dd
                return LocalDate.parse(s.trim()).toString();
            }
            return raw;
        }
        if (t.equals("TIMESTAMP")) {
            if (raw instanceof String s && !s.isBlank()) {
                // 归一化 yyyy-MM-dd HH:mm:ss
                LocalDateTime dt = s.contains("T")
                        ? LocalDateTime.parse(s.trim())
                        : LocalDateTime.parse(s.trim(), DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
                return dt.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
            }
            return raw;
        }
        return raw.toString();
    }
}
