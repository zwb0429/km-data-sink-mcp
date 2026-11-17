package com.example.datasink.engine;

import com.example.datasink.db.ConnectionManager;
import com.example.datasink.db.Dialect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

/**
 * 执行记录读写（mcp_task_execution，RUNNING/SUCCESS/FAILED）。
 * T1（RUNNING）/T3（FAILED）独立事务；T2（SUCCESS）与业务文档同事务（由 UpsertWriter 调用）。
 */
public class ExecutionRecorder {

    private static final Logger log = LoggerFactory.getLogger(ExecutionRecorder.class);

    public static final String STATUS_RUNNING = "RUNNING";
    public static final String STATUS_SUCCESS = "SUCCESS";
    public static final String STATUS_FAILED = "FAILED";

    private final ConnectionManager manager;

    public ExecutionRecorder(ConnectionManager manager) {
        this.manager = manager;
    }

    /** 执行记录 */
    public record Execution(String taskName, String runId, String status,
                            String startedAt, String finishedAt, String errorCode,
                            String errorMessage, String documentId, String tableName) {
    }

    /**
     * start_execution（T1 独立事务）：INSERT RUNNING。
     * 已存在 RUNNING → EXECUTION_ALREADY_RUNNING；已存在 SUCCESS → EXECUTION_ALREADY_SUCCESS；
     * 已存在 FAILED → 复用原记录，重置为 RUNNING。
     */
    public Execution startExecution(String connection, String taskName, String runId, String startedAt) {
        Dialect dialect = manager.dialect(connection);
        try (Connection conn = manager.getConnection(connection)) {
            conn.setAutoCommit(false);
            try {
                Execution existing = findExecution(conn, taskName, runId);
                if (existing != null) {
                    switch (existing.status()) {
                        case STATUS_RUNNING:
                            conn.rollback();
                            throw new DataSinkException(ErrorCode.EXECUTION_ALREADY_RUNNING,
                                    "执行记录已处于 RUNNING 状态: task=" + taskName + ", run_id=" + runId);
                        case STATUS_SUCCESS:
                            conn.rollback();
                            throw new DataSinkException(ErrorCode.EXECUTION_ALREADY_SUCCESS,
                                    "执行记录已 SUCCESS: task=" + taskName + ", run_id=" + runId
                                            + "。若需重跑请使用新的 run_id。");
                        default: // FAILED → 复用，重置为 RUNNING
                            resetToRunning(conn, taskName, runId, startedAt, dialect);
                            conn.commit();
                            return findExecution(conn, taskName, runId);
                    }
                }
                insertRunning(conn, taskName, runId, startedAt, dialect);
                conn.commit();
                return findExecution(conn, taskName, runId);
            } catch (DataSinkException e) {
                rollbackQuietly(conn);
                throw e;
            } catch (SQLException e) {
                conn.rollback();
                throw translate(e, connection, "start_execution 失败");
            } finally {
                restoreAutoCommit(conn);
            }
        } catch (SQLException e) {
            throw translate(e, connection, "start_execution 失败");
        }
    }

    /**
     * save_document 内部（T2 同事务，由 UpsertWriter 在业务事务内调用）：
     * 更新执行记录为 SUCCESS（+document_id、finished_at）。
     * 不允许因执行记录更新失败而吞掉业务成功——由调用方在同一事务中提交/回滚。
     */
    public void markSuccess(Connection conn, String taskName, String runId,
                            String documentId, String tableName, Dialect dialect) throws SQLException {
        String sql = dialect.dbType().equals("mysql") || dialect.dbType().equals("sqlite")
                ? "UPDATE " + SchemaManager.EXECUTION_TABLE
                + " SET status=?, finished_at=?, error_code=NULL, error_message=NULL, document_id=?, table_name=?"
                + " WHERE task_name=? AND run_id=?"
                : null;
        // 兼容隐式创建：无记录时插入一条 RUNNING 再置 SUCCESS
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, STATUS_SUCCESS);
            ps.setString(2, now(dialect));
            ps.setString(3, documentId);
            ps.setString(4, tableName);
            ps.setString(5, taskName);
            ps.setString(6, runId);
            int updated = ps.executeUpdate();
            if (updated == 0) {
                insertRunning(conn, taskName, runId, now(dialect), dialect);
                markSuccess(conn, taskName, runId, documentId, tableName, dialect);
            }
        }
    }

    /**
     * 失败补偿（T3 独立事务）：更新 FAILED（+error_code、error_message、finished_at）。
     * 若记录不存在（例如未 start_execution 直接 save_document 失败），插入一条 FAILED 记录。
     */
    public void markFailed(String connection, String taskName, String runId,
                           String errorCode, String errorMessage) {
        Dialect dialect = manager.dialect(connection);
        try (Connection conn = manager.getConnection(connection)) {
            conn.setAutoCommit(false);
            try {
                String sql = "UPDATE " + SchemaManager.EXECUTION_TABLE
                        + " SET status=?, finished_at=?, error_code=?, error_message=?"
                        + " WHERE task_name=? AND run_id=?";
                try (PreparedStatement ps = conn.prepareStatement(sql)) {
                    ps.setString(1, STATUS_FAILED);
                    ps.setString(2, now(dialect));
                    ps.setString(3, errorCode);
                    ps.setString(4, sanitize(errorMessage));
                    ps.setString(5, taskName);
                    ps.setString(6, runId);
                    int updated = ps.executeUpdate();
                    if (updated == 0) {
                        insertFailed(conn, taskName, runId, errorCode, errorMessage, dialect);
                    }
                }
                conn.commit();
            } catch (SQLException e) {
                conn.rollback();
                // T3 也失败：记录服务端错误日志，保留待恢复标记（允许 get_execution 查看）
                log.error("T3 FAILED 更新失败（待恢复）: task={}, run_id={}, dbError={}",
                        taskName, runId, e.getMessage());
            } finally {
                restoreAutoCommit(conn);
            }
        } catch (SQLException e) {
            log.error("T3 FAILED 更新连接失败（待恢复）: task={}, run_id={}, dbError={}",
                    taskName, runId, e.getMessage());
        }
    }

    /** 查询单条执行记录；不存在 → EXECUTION_NOT_FOUND */
    public Execution getExecution(String connection, String taskName, String runId) {
        try (Connection conn = manager.getConnection(connection)) {
            Execution e = findExecution(conn, taskName, runId);
            if (e == null) {
                throw new DataSinkException(ErrorCode.EXECUTION_NOT_FOUND,
                        "执行记录不存在: task=" + taskName + ", run_id=" + runId);
            }
            return e;
        } catch (SQLException e) {
            throw translate(e, connection, "get_execution 失败");
        }
    }

    // ---------- 内部 ----------

    private Execution findExecution(Connection conn, String taskName, String runId) throws SQLException {
        String sql = "SELECT status, started_at, finished_at, error_code, error_message, document_id, table_name"
                + " FROM " + SchemaManager.EXECUTION_TABLE
                + " WHERE task_name=? AND run_id=?";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, taskName);
            ps.setString(2, runId);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return new Execution(taskName, runId,
                            rs.getString("status"),
                            rs.getString("started_at"),
                            rs.getString("finished_at"),
                            rs.getString("error_code"),
                            rs.getString("error_message"),
                            rs.getString("document_id"),
                            rs.getString("table_name"));
                }
                return null;
            }
        }
    }

    private void insertRunning(Connection conn, String taskName, String runId,
                               String startedAt, Dialect dialect) throws SQLException {
        String sql = "INSERT INTO " + SchemaManager.EXECUTION_TABLE
                + " (task_name, run_id, status, started_at, created_at) VALUES (?,?,?,?,?)";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, taskName);
            ps.setString(2, runId);
            ps.setString(3, STATUS_RUNNING);
            ps.setString(4, startedAt != null && !startedAt.isBlank() ? startedAt : now(dialect));
            ps.setString(5, now(dialect));
            ps.executeUpdate();
        }
    }

    private void insertFailed(Connection conn, String taskName, String runId,
                              String errorCode, String errorMessage, Dialect dialect) throws SQLException {
        String sql = "INSERT INTO " + SchemaManager.EXECUTION_TABLE
                + " (task_name, run_id, status, started_at, finished_at, error_code, error_message, created_at)"
                + " VALUES (?,?,?,?,?,?,?,?)";
        String t = now(dialect);
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, taskName);
            ps.setString(2, runId);
            ps.setString(3, STATUS_FAILED);
            ps.setString(4, t);
            ps.setString(5, t);
            ps.setString(6, errorCode);
            ps.setString(7, sanitize(errorMessage));
            ps.setString(8, t);
            ps.executeUpdate();
        }
    }

    private void resetToRunning(Connection conn, String taskName, String runId,
                                String startedAt, Dialect dialect) throws SQLException {
        String sql = "UPDATE " + SchemaManager.EXECUTION_TABLE
                + " SET status=?, started_at=?, finished_at=NULL, error_code=NULL, error_message=NULL,"
                + " document_id=NULL, table_name=NULL WHERE task_name=? AND run_id=?";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, STATUS_RUNNING);
            ps.setString(2, startedAt != null && !startedAt.isBlank() ? startedAt : now(dialect));
            ps.setString(3, taskName);
            ps.setString(4, runId);
            ps.executeUpdate();
        }
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

    /** 错误信息脱敏：去除可能出现的 jdbc: 连接串与密码片段 */
    private String sanitize(String message) {
        if (message == null) {
            return null;
        }
        return message.replaceAll("jdbc:[^\\s]+", "<jdbc-url>")
                .replaceAll("password[=:][^\\s&]+", "password=<masked>");
    }

    private String now(Dialect dialect) {
        return dialect.formatNow();
    }

    /** 异常 → 统一 DataSinkException（区分锁超时/事务错误/连接错误） */
    private DataSinkException translate(SQLException e, String connection, String action) {
        Dialect dialect = manager.dialect(connection);
        if (dialect.isLockTimeout(e)) {
            return new DataSinkException(ErrorCode.DATABASE_LOCK_TIMEOUT,
                    action + "：数据库锁/超时", e);
        }
        if (dialect.isRetryable(e)) {
            return new DataSinkException(ErrorCode.DATABASE_CONNECTION_ERROR,
                    action + "：数据库临时性错误", e);
        }
        return new DataSinkException(ErrorCode.DATABASE_TRANSACTION_ERROR,
                action + "：数据库事务错误", e);
    }
}
