package com.example.datasink.db;

import com.example.datasink.config.ConnectionConfig;
import com.example.datasink.config.TableConfig;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * 数据库方言接口（P1-⑧）。统一屏蔽 SQLite/MySQL 差异。
 * 禁止业务服务层（UpsertWriter、SchemaManager 之外）直接拼接数据库专属 SQL。
 */
public interface Dialect {

    String dbType();

    /** JDBC URL 拼接 */
    String buildJdbcUrl(ConnectionConfig config);

    /** 建表 SQL（含唯一索引、document_id 唯一索引） */
    String buildCreateTableSql(TableConfig table);

    /** 时间字段 system.now 落库值格式 */
    String formatNow();

    /**
     * 表结构检查：比较列类型/NOT NULL/唯一索引。
     * 返回差异列表；空列表表示结构与配置一致。
     */
    List<String> checkSchema(Connection conn, TableConfig table) throws SQLException;

    /**
     * 唯一键冲突异常判定（方案 9.1-9）：
     * 返回冲突的唯一索引名/列信息。若异常由唯一索引冲突引起返回 true。
     */
    boolean isUniqueViolation(SQLException e);

    /**
     * 判定冲突列集合是否命中 upsert_key（区分 upsert_key 冲突 vs 辅助键冲突）。
     * 返回冲突的唯一索引列组合；无法判定时返回 null。
     */
    List<String> conflictColumns(SQLException e);

    /** 是否可重试异常（连接超时、SQLITE_BUSY、临时网络错误等） */
    boolean isRetryable(SQLException e);

    /** 锁/超时类异常（SQLITE_BUSY 重试耗尽、查询超时） */
    boolean isLockTimeout(SQLException e);

    /** 连接建立后的初始化（SQLite PRAGMA；MySQL 空实现） */
    default void initConnection(Connection conn) throws SQLException {
        // 默认无操作
    }

    // ---------- 元数据辅助（默认实现，供 checkSchema 使用） ----------

    /** 读取表列的 DDL 类型名（元数据 TYPE_NAME） */
    default List<String> describeColumns(Connection conn, String table) throws SQLException {
        List<String> result = new ArrayList<>();
        DatabaseMetaData md = conn.getMetaData();
        try (ResultSet rs = md.getColumns(null, null, table, "%")) {
            while (rs.next()) {
                String col = rs.getString("COLUMN_NAME");
                String typeName = rs.getString("TYPE_NAME");
                int nullable = rs.getInt("NULLABLE");
                result.add(col + "|" + typeName + "|nullable=" + (nullable == DatabaseMetaData.columnNullable));
            }
        }
        return result;
    }

    /** 读取表上的唯一索引列组合 */
    default List<List<String>> describeUniqueIndexes(Connection conn, String table) throws SQLException {
        List<List<String>> result = new ArrayList<>();
        DatabaseMetaData md = conn.getMetaData();
        try (ResultSet rs = md.getIndexInfo(null, null, table, true, false)) {
            List<String> current = null;
            String currentName = null;
            while (rs.next()) {
                String indexName = rs.getString("INDEX_NAME");
                String column = rs.getString("COLUMN_NAME");
                if (column == null) {
                    continue;
                }
                if (!indexName.equals(currentName)) {
                    if (current != null && !current.isEmpty()) {
                        result.add(current);
                    }
                    current = new ArrayList<>();
                    currentName = indexName;
                }
                current.add(column);
            }
            if (current != null && !current.isEmpty()) {
                result.add(current);
            }
        }
        return result;
    }

    static String normalizeType(String type) {
        return type == null ? "" : type.trim().toUpperCase(Locale.ROOT);
    }
}
