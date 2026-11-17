package com.example.datasink.engine;

import com.example.datasink.config.ConnectionConfig;
import com.example.datasink.config.TableConfig;
import com.example.datasink.db.ConnectionManager;
import com.example.datasink.db.Dialect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * 参数化 upsert（P0-① 核心，方案 8.4）：
 * - overwrite/history 统一采用"先查后写 + 唯一冲突区分"，保证 SQLite 与 MySQL 行为一致。
 * - 命中 upsert_key → UPDATE（不更新 document_id，保留原值）。
 * - 未命中 → INSERT（含新 document_id）；INSERT 触发唯一冲突时区分来源：
 *   upsert_key 冲突（并发）→ 转 UPDATE；辅助键冲突 → 回滚并返回 UNIQUE_KEY_CONFLICT。
 * - T2 事务：业务文档写入 + SUCCESS 更新同事务。
 * - 可重试异常有限重试（db.max_retry / db.retry_backoff_ms）。
 */
public class UpsertWriter {

    private static final Logger log = LoggerFactory.getLogger(UpsertWriter.class);

    private final ConnectionManager manager;
    private final ExecutionRecorder recorder;
    private final int maxRetry;
    private final long retryBackoffMs;

    public UpsertWriter(ConnectionManager manager, ExecutionRecorder recorder,
                        int maxRetry, long retryBackoffMs) {
        this.manager = manager;
        this.recorder = recorder;
        this.maxRetry = Math.max(1, maxRetry);
        this.retryBackoffMs = Math.max(0, retryBackoffMs);
    }

    /**
     * 文档落库（save_document 核心）。
     *
     * @param row 列名 → 值（由 ValueResolver 解析）
     * @return 保存结果：document_id + 是否为更新
     */
    public SaveResult save(String connection, TableConfig table, Map<String, Object> row,
                           String taskName, String runId) {
        Dialect dialect = manager.dialect(connection);
        int attempt = 0;
        SQLException last = null;
        while (attempt < maxRetry) {
            attempt++;
            try (Connection conn = manager.getConnection(connection)) {
                conn.setAutoCommit(false);
                try {
                    // ---- T2：业务写入 + SUCCESS 同事务 ----
                    boolean updated = doUpsert(conn, table, row, dialect);
                    // doUpsert 可能将 row 的 document_id 替换为原值（overwrite 保留原 document_id）
                    String documentId = String.valueOf(row.get("document_id"));
                    recorder.markSuccess(conn, taskName, runId, documentId, table.getName(), dialect);
                    conn.commit();
                    return new SaveResult(documentId, updated);
                } catch (SQLException e) {
                    conn.rollback();
                    if (isAuxUniqueConflict(e, table, dialect)) {
                        // 辅助唯一键冲突：拒绝写入，不重试
                        throw new DataSinkException(ErrorCode.UNIQUE_KEY_CONFLICT,
                                "辅助唯一键冲突，已拒绝写入: 表 " + table.getName()
                                        + " 冲突键 (" + String.join(", ", conflictCols(e, table, dialect)) + ")");
                    }
                    throw e;
                } catch (DataSinkException e) {
                    rollbackQuietly(conn);
                    throw e;
                } finally {
                    restoreAutoCommit(conn);
                }
            } catch (SQLException e) {
                // 可重试异常有限重试
                last = e;
                if (!dialect.isRetryable(e) || attempt >= maxRetry) {
                    throw translate(e, connection, "save_document 失败");
                }
                log.warn("save_document 可重试异常（第 {} 次）: {}", attempt, e.getMessage());
                sleepQuietly(retryBackoffMs * attempt);
            }
        }
        // 理论不可达
        throw translate(last, connection, "save_document 失败");
    }

    /**
     * 先查后写。
     * @return true=UPDATE（覆盖原记录）；false=INSERT
     */
    private boolean doUpsert(Connection conn, TableConfig table, Map<String, Object> row,
                             Dialect dialect) throws SQLException {
        List<String> key = table.getUpsertKey();
        // 1. 先按 upsert_key 查询目标记录
        boolean exists = existsByUpsertKey(conn, table, key, row);
        if (exists) {
            // 2. 命中 → UPDATE，不更新 document_id（保留原值）
            updateByUpsertKey(conn, table, key, row, dialect);
            // 查回原 document_id，写入 row（供执行记录关联）
            String originalDocId = queryDocumentId(conn, table, key, row);
            if (originalDocId != null) {
                row.put("document_id", originalDocId);
            }
            return true;
        }
        // 3. 未命中 → INSERT（含新 document_id）
        try {
            insertRow(conn, table, row, dialect);
            return false;
        } catch (SQLException e) {
            if (dialect.isUniqueViolation(e)) {
                List<String> conflict = conflictCols(e, table, dialect);
                if (conflict != null && !conflict.isEmpty() && !sameGroup(conflict, key)) {
                    // 辅助键冲突 → 抛出，外层转 UNIQUE_KEY_CONFLICT
                    throw e;
                }
                // upsert_key 冲突（并发写入）：转 UPDATE
                updateByUpsertKey(conn, table, key, row, dialect);
                String originalDocId = queryDocumentId(conn, table, key, row);
                if (originalDocId != null) {
                    row.put("document_id", originalDocId);
                }
                return true;
            }
            throw e;
        }
    }

    private boolean existsByUpsertKey(Connection conn, TableConfig table, List<String> key,
                                      Map<String, Object> row) throws SQLException {
        String sql = "SELECT 1 FROM " + qi(table.getName(), table) + " WHERE " + whereKey(key, table);
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            bindKey(ps, key, row, table, 1);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }
        }
    }

    private String queryDocumentId(Connection conn, TableConfig table, List<String> key,
                                   Map<String, Object> row) throws SQLException {
        // document_id 列由系统强制存在；若配置未显式声明，引擎已自动加入
        String sql = "SELECT document_id FROM " + qi(table.getName(), table) + " WHERE " + whereKey(key, table);
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            bindKey(ps, key, row, table, 1);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getString(1) : null;
            }
        }
    }

    private void updateByUpsertKey(Connection conn, TableConfig table, List<String> key,
                                   Map<String, Object> row, Dialect dialect) throws SQLException {
        // 更新列 = 除 upsert_key 列与 document_id 外的全部列
        List<String> updateCols = table.getColumns().stream()
                .map(com.example.datasink.config.ColumnConfig::getColumn)
                .filter(c -> !key.contains(c))
                .filter(c -> !"document_id".equalsIgnoreCase(c))
                .collect(Collectors.toList());
        if (updateCols.isEmpty()) {
            return; // 仅键列，无需更新
        }
        String sql = "UPDATE " + qi(table.getName(), table) + " SET "
                + updateCols.stream().map(c -> qi(c, table) + " = ?").collect(Collectors.joining(", "))
                + " WHERE " + whereKey(key, table);
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            int idx = 1;
            for (String col : updateCols) {
                ps.setObject(idx++, row.get(col));
            }
            bindKey(ps, key, row, table, idx);
            ps.executeUpdate();
        }
    }

    private void insertRow(Connection conn, TableConfig table, Map<String, Object> row,
                           Dialect dialect) throws SQLException {
        List<String> cols = table.getColumns().stream()
                .map(com.example.datasink.config.ColumnConfig::getColumn)
                .collect(Collectors.toList());
        String sql = "INSERT INTO " + qi(table.getName(), table) + " ("
                + cols.stream().map(c -> qi(c, table)).collect(Collectors.joining(", "))
                + ") VALUES ("
                + cols.stream().map(c -> "?").collect(Collectors.joining(", "))
                + ")";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            int idx = 1;
            for (String col : cols) {
                ps.setObject(idx++, row.get(col));
            }
            ps.executeUpdate();
        }
    }

    private String whereKey(List<String> key, TableConfig table) {
        return key.stream().map(c -> qi(c, table) + " = ?").collect(Collectors.joining(" AND "));
    }

    private void bindKey(PreparedStatement ps, List<String> key, Map<String, Object> row,
                         TableConfig table, int startIdx) throws SQLException {
        int idx = startIdx;
        for (String col : key) {
            ps.setObject(idx++, row.get(col));
        }
    }

    /** 判断异常是否为辅助唯一键冲突（非 upsert_key） */
    private boolean isAuxUniqueConflict(SQLException e, TableConfig table, Dialect dialect) {
        if (!dialect.isUniqueViolation(e)) {
            return false;
        }
        List<String> conflict = conflictCols(e, table, dialect);
        if (conflict == null || conflict.isEmpty()) {
            return false; // 无法判定 → 保守视为 upsert_key 冲突
        }
        return !sameGroup(conflict, table.getUpsertKey());
    }

    /** 解析冲突列（SQLite 从消息取列名；MySQL 从索引名映射） */
    private List<String> conflictCols(SQLException e, TableConfig table, Dialect dialect) {
        List<String> cols = dialect.conflictColumns(e);
        if (cols == null) {
            return null;
        }
        // MySQL 返回的是索引名（uk_upsert / uk_aux_N）
        if (cols.size() == 1 && cols.get(0).startsWith("uk_")) {
            String idx = cols.get(0);
            if (idx.equalsIgnoreCase("uk_upsert")) {
                return new ArrayList<>(table.getUpsertKey());
            }
            if (idx.startsWith("uk_aux_")) {
                int n;
                try {
                    n = Integer.parseInt(idx.substring("uk_aux_".length()));
                } catch (NumberFormatException ex) {
                    return cols;
                }
                if (table.getUniqueKeys() != null && n >= 1 && n <= table.getUniqueKeys().size()) {
                    return new ArrayList<>(table.getUniqueKeys().get(n - 1));
                }
            }
            return cols;
        }
        return cols;
    }

    private boolean sameGroup(List<String> a, List<String> b) {
        if (a.size() != b.size()) {
            return false;
        }
        Set<String> sa = a.stream().map(String::toLowerCase).collect(Collectors.toSet());
        Set<String> sb = b.stream().map(String::toLowerCase).collect(Collectors.toSet());
        return sa.equals(sb);
    }

    /** SQLite 方言使用的列名引用（方括号/引号安全：列名来自白名单，直接拼接已校验标识符） */
    private String qi(String identifier, TableConfig table) {
        return identifier; // 标识符已经 ConfigValidator 校验（[A-Za-z][A-Za-z0-9_]*），无注入风险
    }

    private DataSinkException translate(SQLException e, String connection, String action) {
        Dialect dialect = manager.dialect(connection);
        if (dialect.isLockTimeout(e)) {
            return new DataSinkException(ErrorCode.DATABASE_LOCK_TIMEOUT, action + "：数据库锁/超时", e);
        }
        if (dialect.isRetryable(e)) {
            return new DataSinkException(ErrorCode.DATABASE_CONNECTION_ERROR, action + "：数据库临时性错误", e);
        }
        return new DataSinkException(ErrorCode.DATABASE_TRANSACTION_ERROR, action + "：数据库事务错误", e);
    }

    private void rollbackQuietly(Connection conn) {
        try {
            conn.rollback();
        } catch (SQLException ignore) {
            // 忽略
        }
    }

    private void restoreAutoCommit(Connection conn) {
        try {
            conn.setAutoCommit(true);
        } catch (SQLException ignore) {
            // 忽略
        }
    }

    private void sleepQuietly(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
    }

    /** 保存结果 */
    public record SaveResult(String documentId, boolean updated) {
    }
}
