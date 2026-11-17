package com.example.datasink.config;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 顶层配置模型（连接 + 表 + 限制 + 重试 + 分页）。
 */
public class AppConfig {

    private Map<String, ConnectionConfig> connections = new LinkedHashMap<>();
    private ContentConfig content = new ContentConfig();
    private DbConfig db = new DbConfig();
    private QueryConfig query = new QueryConfig();
    private TablesConfig tables = new TablesConfig();

    public Map<String, ConnectionConfig> getConnections() {
        return connections;
    }

    public void setConnections(Map<String, ConnectionConfig> connections) {
        this.connections = connections;
    }

    public ContentConfig getContent() {
        return content;
    }

    public void setContent(ContentConfig content) {
        this.content = content;
    }

    public DbConfig getDb() {
        return db;
    }

    public void setDb(DbConfig db) {
        this.db = db;
    }

    public QueryConfig getQuery() {
        return query;
    }

    public void setQuery(QueryConfig query) {
        this.query = query;
    }

    public TablesConfig getTables() {
        return tables;
    }

    public void setTables(TablesConfig tables) {
        this.tables = tables;
    }

    /** 内容长度限制配置 */
    public static class ContentConfig {
        /** 单篇文档上限（MB，按 UTF-8 字节计），默认 5 */
        private double maxSizeMb = 5.0;

        public double getMaxSizeMb() {
            return maxSizeMb;
        }

        public void setMaxSizeMb(double maxSizeMb) {
            this.maxSizeMb = maxSizeMb;
        }

        public long maxBytes() {
            return (long) (maxSizeMb * 1024L * 1024L);
        }
    }

    /** 重试与事务配置 */
    public static class DbConfig {
        private int maxRetry = 3;
        private int retryBackoffMs = 200;
        private int queryTimeoutS = 30;
        private int poolSize = 5;

        public int getMaxRetry() {
            return maxRetry;
        }

        public void setMaxRetry(int maxRetry) {
            this.maxRetry = maxRetry;
        }

        public int getRetryBackoffMs() {
            return retryBackoffMs;
        }

        public void setRetryBackoffMs(int retryBackoffMs) {
            this.retryBackoffMs = retryBackoffMs;
        }

        public int getQueryTimeoutS() {
            return queryTimeoutS;
        }

        public void setQueryTimeoutS(int queryTimeoutS) {
            this.queryTimeoutS = queryTimeoutS;
        }

        public int getPoolSize() {
            return poolSize;
        }

        public void setPoolSize(int poolSize) {
            this.poolSize = poolSize;
        }
    }

    /** 分页配置 */
    public static class QueryConfig {
        private int defaultPageSize = 20;
        private int maxPageSize = 100;

        public int getDefaultPageSize() {
            return defaultPageSize;
        }

        public void setDefaultPageSize(int defaultPageSize) {
            this.defaultPageSize = defaultPageSize;
        }

        public int getMaxPageSize() {
            return maxPageSize;
        }

        public void setMaxPageSize(int maxPageSize) {
            this.maxPageSize = maxPageSize;
        }
    }

    /** 数据表配置 */
    public static class TablesConfig {
        private boolean autoCreate = true;
        private List<TableConfig> allowlist = new ArrayList<>();

        public boolean isAutoCreate() {
            return autoCreate;
        }

        public void setAutoCreate(boolean autoCreate) {
            this.autoCreate = autoCreate;
        }

        public List<TableConfig> getAllowlist() {
            return allowlist;
        }

        public void setAllowlist(List<TableConfig> allowlist) {
            this.allowlist = allowlist;
        }

        public TableConfig findTable(String name) {
            if (name == null) {
                return null;
            }
            for (TableConfig t : allowlist) {
                if (name.equals(t.getName())) {
                    return t;
                }
            }
            return null;
        }
    }
}
