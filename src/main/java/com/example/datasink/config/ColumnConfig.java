package com.example.datasink.config;

/**
 * 列映射定义。
 */
public class ColumnConfig {

    /** 系统强制列：服务端生成 UUID */
    public static final String SOURCE_SYSTEM_UUID = "system.uuid";
    /** 系统强制列：服务端生成当前时间 */
    public static final String SOURCE_SYSTEM_NOW = "system.now";

    /** 允许的调用方顶层 source */
    private static final java.util.Set<String> TOP_LEVEL_SOURCES =
            java.util.Set.of("content", "format", "task_name", "run_id", "business_date");

    private String column;
    /** 通用类型：TEXT / LONG_TEXT / VARCHAR(n) / INT / BIGINT / DECIMAL(p,s) / DATE / TIMESTAMP / BOOLEAN */
    private String type;
    /** 取值来源：content / format / task_name / run_id / business_date / meta.xxx / system.uuid / system.now */
    private String source;
    /** 默认 true */
    private boolean nullable = true;
    /** 列注释（仅 MySQL 建表生效，可选） */
    private String comment;

    public String getColumn() {
        return column;
    }

    public void setColumn(String column) {
        this.column = column;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public boolean isNullable() {
        return nullable;
    }

    public void setNullable(boolean nullable) {
        this.nullable = nullable;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    /** 是否系统来源列（不由调用方提供） */
    public boolean isSystemSource() {
        return SOURCE_SYSTEM_UUID.equals(source) || SOURCE_SYSTEM_NOW.equals(source);
    }

    /** 是否调用方顶层参数来源 */
    public boolean isTopLevelSource() {
        return source != null && TOP_LEVEL_SOURCES.contains(source);
    }

    /** 是否 meta.xxx 嵌套来源 */
    public boolean isMetaSource() {
        return source != null && source.startsWith("meta.");
    }
}
