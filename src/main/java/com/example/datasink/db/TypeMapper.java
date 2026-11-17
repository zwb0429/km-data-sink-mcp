package com.example.datasink.db;

import com.example.datasink.config.ColumnConfig;

import java.util.Locale;

/**
 * 通用类型 → 方言 DDL 类型映射（见方案 9.2）。
 */
public final class TypeMapper {

    private TypeMapper() {
    }

    /** 通用类型是否合法（与 ConfigValidator 一致的第二道防线） */
    public static boolean isSupported(String genericType) {
        if (genericType == null) {
            return false;
        }
        String t = genericType.trim().toUpperCase(Locale.ROOT);
        return t.equals("TEXT") || t.equals("LONG_TEXT") || t.equals("INT") || t.equals("BIGINT")
                || t.equals("DATE") || t.equals("TIMESTAMP") || t.equals("BOOLEAN")
                || t.matches("VARCHAR\\((\\d+)\\)") || t.matches("DECIMAL\\((\\d+)\\s*,\\s*(\\d+)\\)");
    }

    /** 通用类型 → MySQL DDL 类型 */
    public static String toMySql(String genericType) {
        String t = normalize(genericType);
        return switch (t) {
            case "TEXT", "LONG_TEXT" -> "TEXT".equals(t) ? "TEXT" : "LONGTEXT";
            case "INT" -> "INT";
            case "BIGINT" -> "BIGINT";
            case "DATE" -> "DATE";
            case "TIMESTAMP" -> "DATETIME";
            case "BOOLEAN" -> "TINYINT(1)";
            default -> t; // VARCHAR(n)、DECIMAL(p,s) 保持原样
        };
    }

    /** 通用类型 → SQLite DDL 类型 */
    public static String toSqlite(String genericType) {
        String t = normalize(genericType);
        if (t.matches("DECIMAL\\((\\d+)\\s*,\\s*(\\d+)\\)")) {
            return t.replaceFirst("DECIMAL", "NUMERIC"); // 方案 9.2：DECIMAL(p,s) → NUMERIC(p,s)
        }
        return switch (t) {
            case "LONG_TEXT" -> "TEXT";
            case "INT" -> "INTEGER";
            case "BIGINT" -> "INTEGER";
            case "TIMESTAMP" -> "DATETIME";
            case "BOOLEAN" -> "INTEGER";
            default -> t; // TEXT/VARCHAR(n)/DATE 保持原样
        };
    }

    /** 解析通用类型为 JDBC 类型（java.sql.Types） */
    public static int toJdbcType(ColumnConfig col) {
        String t = normalize(col.getType());
        if (t.startsWith("VARCHAR")) {
            return java.sql.Types.VARCHAR;
        }
        if (t.startsWith("DECIMAL")) {
            return java.sql.Types.DECIMAL;
        }
        return switch (t) {
            case "TEXT", "LONG_TEXT" -> java.sql.Types.LONGVARCHAR;
            case "INT" -> java.sql.Types.INTEGER;
            case "BIGINT" -> java.sql.Types.BIGINT;
            case "DATE" -> java.sql.Types.DATE;
            case "TIMESTAMP" -> java.sql.Types.TIMESTAMP;
            case "BOOLEAN" -> java.sql.Types.BOOLEAN;
            default -> java.sql.Types.VARCHAR;
        };
    }

    private static String normalize(String genericType) {
        return genericType == null ? "" : genericType.trim().toUpperCase(Locale.ROOT);
    }
}
