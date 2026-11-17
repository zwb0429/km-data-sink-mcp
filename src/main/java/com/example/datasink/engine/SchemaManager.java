package com.example.datasink.engine;

import com.example.datasink.db.ConnectionManager;
import com.example.datasink.db.Dialect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * 表结构管理：执行记录表创建/校验、业务表自动建表、结构校验（P1-⑨）。
 * 多进程建表：CREATE TABLE IF NOT EXISTS + 捕获"表已存在"异常重试 + 进程内锁。
 */
public class SchemaManager {

    private static final Logger log = LoggerFactory.getLogger(SchemaManager.class);

    /** 执行记录表固定名（D5） */
    public static final String EXECUTION_TABLE = "mcp_task_execution";

    /** 执行记录表结构（每连接独立维护，见方案 4.2） */
    private static final String EXECUTION_TABLE_DDL_PREFIX =
            "CREATE TABLE IF NOT EXISTS " + EXECUTION_TABLE + " (";

    private final ConnectionManager manager;
    /** 单进程并发建表锁（P1-⑦-7） */
    private final Object createTableLock = new Object();

    public SchemaManager(ConnectionManager manager) {
        this.manager = manager;
    }

    /**
     * 服务启动时对每个连接执行：建执行记录表 + 校验执行记录表结构。
     * 业务表启动时只校验存在性（auto_create=true 则跳过，首次写入建表）。
     */
    public void init() {
        for (String connName : manager.appConfig().getConnections().keySet()) {
            Dialect dialect = manager.dialect(connName);
            try (Connection conn = manager.getConnection(connName)) {
                createExecutionTable(conn, dialect);
                verifyExecutionTable(conn, dialect);
                log.info("连接 {} 执行记录表就绪", connName);
            } catch (SQLException e) {
                log.error("连接 {} 初始化执行记录表失败: {}", connName, e.getMessage());
                throw new DataSinkException(ErrorCode.DATABASE_CONNECTION_ERROR,
                        "初始化执行记录表失败（连接: " + connName + "）", e);
            }
        }
    }

    /** 执行记录表 DDL（方言差异：MySQL 用 UNIQUE KEY，SQLite 用 UNIQUE 约束） */
    private String executionTableDdl(Dialect dialect) {
        if ("mysql".equals(dialect.dbType())) {
            return EXECUTION_TABLE_DDL_PREFIX
                    + " id BIGINT AUTO_INCREMENT PRIMARY KEY,\n"
                    + "  task_name VARCHAR(128) NOT NULL,\n"
                    + "  run_id VARCHAR(128) NOT NULL,\n"
                    + "  status VARCHAR(20) NOT NULL,\n"
                    + "  started_at DATETIME NOT NULL,\n"
                    + "  finished_at DATETIME NULL,\n"
                    + "  error_code VARCHAR(64) NULL,\n"
                    + "  error_message TEXT NULL,\n"
                    + "  document_id VARCHAR(36) NULL,\n"
                    + "  table_name VARCHAR(128) NULL,\n"
                    + "  created_at DATETIME NOT NULL,\n"
                    + "  UNIQUE KEY uk_task_run (task_name, run_id),\n"
                    + "  KEY idx_task_created (task_name, created_at)\n"
                    + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4";
        }
        return EXECUTION_TABLE_DDL_PREFIX
                + " id INTEGER PRIMARY KEY AUTOINCREMENT,\n"
                + "  task_name VARCHAR(128) NOT NULL,\n"
                + "  run_id VARCHAR(128) NOT NULL,\n"
                + "  status VARCHAR(20) NOT NULL,\n"
                + "  started_at DATETIME NOT NULL,\n"
                + "  finished_at DATETIME NULL,\n"
                + "  error_code VARCHAR(64) NULL,\n"
                + "  error_message TEXT NULL,\n"
                + "  document_id VARCHAR(36) NULL,\n"
                + "  table_name VARCHAR(128) NULL,\n"
                + "  created_at DATETIME NOT NULL,\n"
                + "  CONSTRAINT uk_task_run UNIQUE (task_name, run_id)\n"
                + ")";
    }

    private void createExecutionTable(Connection conn, Dialect dialect) throws SQLException {
        String ddl = executionTableDdl(dialect);
        synchronized (createTableLock) {
            int attempts = 0;
            while (true) {
                try (var st = conn.createStatement()) {
                    st.execute(ddl);
                    return;
                } catch (SQLException e) {
                    // 表已存在 → 直接通过（多进程并发建表场景）
                    if (isTableAlreadyExists(e, dialect)) {
                        return;
                    }
                    attempts++;
                    if (attempts >= 3 || !dialect.isRetryable(e)) {
                        throw e;
                    }
                    sleepQuietly(200L * attempts);
                }
            }
        }
    }

    private boolean isTableAlreadyExists(SQLException e, Dialect dialect) {
        SQLException cur = e;
        while (cur != null) {
            String msg = cur.getMessage() == null ? "" : cur.getMessage().toLowerCase();
            if ("mysql".equals(dialect.dbType())) {
                // 1050 表已存在
                if (cur.getErrorCode() == 1050 || msg.contains("already exists")) {
                    return true;
                }
            } else {
                if (msg.contains("already exists")) {
                    return true;
                }
            }
            cur = cur.getNextException();
        }
        return false;
    }

    /** 执行记录表结构校验（启动即拦截，结构异常禁止执行） */
    private void verifyExecutionTable(Connection conn, Dialect dialect) throws SQLException {
        List<String> actual = dialect.describeColumns(conn, EXECUTION_TABLE);
        List<String> expected = List.of(
                "id", "task_name", "run_id", "status", "started_at", "finished_at",
                "error_code", "error_message", "document_id", "table_name", "created_at");
        List<String> actualNames = new ArrayList<>();
        for (String desc : actual) {
            actualNames.add(desc.split("\\|", -1)[0].toLowerCase());
        }
        List<String> missing = new ArrayList<>();
        for (String exp : expected) {
            if (!actualNames.contains(exp)) {
                missing.add(exp);
            }
        }
        if (!missing.isEmpty()) {
            throw new DataSinkException(ErrorCode.SCHEMA_VALIDATION_FAILED,
                    "执行记录表 " + EXECUTION_TABLE + " 结构异常，缺少字段: " + String.join(", ", missing));
        }
    }

    /**
     * 业务表首次写入时自动建表（auto_create=true）。
     * 表已存在 → 不 ALTER，返回 false（转结构校验流程）。
     */
    public boolean ensureBusinessTable(String connectionName, com.example.datasink.config.TableConfig table) {
        Dialect dialect = manager.dialect(connectionName);
        synchronized (createTableLock) {
            try (Connection conn = manager.getConnection(connectionName)) {
                try (var st = conn.createStatement()) {
                    st.execute(dialect.buildCreateTableSql(table));
                    return true;
                } catch (SQLException e) {
                    if (isTableAlreadyExists(e, dialect)) {
                        return false;
                    }
                    throw e;
                }
            } catch (SQLException e) {
                log.error("自动建表失败（连接 {}，表 {}）: {}", connectionName, table.getName(), e.getMessage());
                throw new DataSinkException(ErrorCode.DATABASE_TRANSACTION_ERROR,
                        "自动建表失败: 表 " + table.getName(), e);
            }
        }
    }

    /**
     * 业务表结构校验（启动或写入前）：结构不符 → 返回差异列表，禁止继续写入。
     */
    public List<String> validateBusinessTable(String connectionName, com.example.datasink.config.TableConfig table) {
        Dialect dialect = manager.dialect(connectionName);
        try (Connection conn = manager.getConnection(connectionName)) {
            return dialect.checkSchema(conn, table);
        } catch (SQLException e) {
            throw new DataSinkException(ErrorCode.DATABASE_CONNECTION_ERROR,
                    "读取表结构失败: " + table.getName(), e);
        }
    }

    private static void sleepQuietly(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
    }
}
