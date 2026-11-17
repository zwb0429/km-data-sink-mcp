package com.example.datasink.config;

import java.util.ArrayList;
import java.util.List;

/**
 * 表映射定义（含 upsert_key、unique_keys、mode）。
 */
public class TableConfig {

    public static final String MODE_OVERWRITE = "overwrite";
    public static final String MODE_HISTORY = "history";

    private String name;
    private String mode = MODE_OVERWRITE;
    private List<ColumnConfig> columns = new ArrayList<>();
    /** 唯一的 upsert 主键（单组） */
    private List<String> upsertKey = new ArrayList<>();
    /** 辅助唯一索引（可选、可多组），仅作唯一约束，不参与 upsert */
    private List<List<String>> uniqueKeys = new ArrayList<>();

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getMode() {
        return mode;
    }

    public void setMode(String mode) {
        this.mode = mode;
    }

    public List<ColumnConfig> getColumns() {
        return columns;
    }

    public void setColumns(List<ColumnConfig> columns) {
        this.columns = columns;
    }

    public List<String> getUpsertKey() {
        return upsertKey;
    }

    public void setUpsertKey(List<String> upsertKey) {
        this.upsertKey = upsertKey;
    }

    public List<List<String>> getUniqueKeys() {
        return uniqueKeys;
    }

    public void setUniqueKeys(List<List<String>> uniqueKeys) {
        this.uniqueKeys = uniqueKeys;
    }

    public ColumnConfig findColumn(String columnName) {
        if (columnName == null) {
            return null;
        }
        for (ColumnConfig c : columns) {
            if (columnName.equals(c.getColumn())) {
                return c;
            }
        }
        return null;
    }

    public boolean isOverwriteMode() {
        return MODE_OVERWRITE.equalsIgnoreCase(mode);
    }

    /**
     * 系统强制列兜底（方案 4.1）：document_id（system.uuid）/ created_at（system.now）
     * 无论配置是否声明，引擎自动加入。幂等。
     */
    public void ensureSystemColumns() {
        if (findColumn("document_id") == null) {
            ColumnConfig c = new ColumnConfig();
            c.setColumn("document_id");
            c.setType("VARCHAR(36)");
            c.setSource(ColumnConfig.SOURCE_SYSTEM_UUID);
            c.setNullable(false);
            columns.add(c);
        }
        if (findColumn("created_at") == null) {
            ColumnConfig c = new ColumnConfig();
            c.setColumn("created_at");
            c.setType("TIMESTAMP");
            c.setSource(ColumnConfig.SOURCE_SYSTEM_NOW);
            c.setNullable(false);
            columns.add(c);
        }
    }
}
