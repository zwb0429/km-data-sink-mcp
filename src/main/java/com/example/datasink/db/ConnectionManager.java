package com.example.datasink.db;

import com.example.datasink.config.AppConfig;
import com.example.datasink.config.ConnectionConfig;
import com.example.datasink.engine.DataSinkException;
import com.example.datasink.engine.ErrorCode;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

/**
 * HikariCP 连接池管理：按连接名维护连接池，惰性初始化，关闭时释放。
 * SQLite 单进程连接池强制 1（忽略配置，P1-⑦）。
 */
public class ConnectionManager {

    private static final Logger log = LoggerFactory.getLogger(ConnectionManager.class);

    private final AppConfig config;
    private final Map<String, Dialect> dialects = new LinkedHashMap<>();
    private final Map<String, HikariDataSource> pools = new HashMap<>();
    /** 惰性初始化锁（单连接名粒度） */
    private final Object poolLock = new Object();

    public ConnectionManager(AppConfig config) {
        this.config = config;
        config.getConnections().forEach((name, conn) -> {
            if (ConnectionConfig.TYPE_MYSQL.equalsIgnoreCase(conn.getType())) {
                dialects.put(name, new MySqlDialect());
            } else if (ConnectionConfig.TYPE_SQLITE.equalsIgnoreCase(conn.getType())) {
                dialects.put(name, new SqliteDialect());
            }
        });
    }

    public Dialect dialect(String connectionName) {
        Dialect d = dialects.get(connectionName);
        if (d == null) {
            throw new DataSinkException(ErrorCode.INVALID_CONNECTION, "连接不存在: " + connectionName);
        }
        return d;
    }

    public ConnectionConfig connectionConfig(String connectionName) {
        ConnectionConfig c = config.getConnections().get(connectionName);
        if (c == null) {
            throw new DataSinkException(ErrorCode.INVALID_CONNECTION, "连接不存在: " + connectionName);
        }
        return c;
    }

    public AppConfig appConfig() {
        return config;
    }

    /** 是否存在指定连接 */
    public boolean hasConnection(String connectionName) {
        return connectionName != null && config.getConnections().containsKey(connectionName);
    }

    /**
     * 获取连接（惰性初始化连接池）。
     */
    public Connection getConnection(String connectionName) {
        HikariDataSource ds = pool(connectionName);
        try {
            Connection conn = ds.getConnection();
            int timeout = resolveQueryTimeout(connectionName);
            if (timeout > 0) {
                conn.setNetworkTimeout(Runnable::run, timeout * 1000);
            }
            return conn;
        } catch (SQLException e) {
            log.error("获取连接失败: {} - {}", connectionName, e.getMessage());
            throw new DataSinkException(ErrorCode.DATABASE_CONNECTION_ERROR,
                    "数据库连接失败（连接: " + connectionName + "）", e);
        }
    }

    private int resolveQueryTimeout(String connectionName) {
        ConnectionConfig c = connectionConfig(connectionName);
        if (c.getQueryTimeoutS() != null) {
            return c.getQueryTimeoutS();
        }
        return config.getDb().getQueryTimeoutS();
    }

    private HikariDataSource pool(String connectionName) {
        HikariDataSource ds = pools.get(connectionName);
        if (ds != null) {
            return ds;
        }
        synchronized (poolLock) {
            ds = pools.get(connectionName);
            if (ds != null) {
                return ds;
            }
            ConnectionConfig c = connectionConfig(connectionName);
            Dialect dialect = dialect(connectionName);
            ds = createPool(c, dialect);
            pools.put(connectionName, ds);
            return ds;
        }
    }

    private HikariDataSource createPool(ConnectionConfig c, Dialect dialect) {
        HikariConfig hc = new HikariConfig();
        hc.setJdbcUrl(dialect.buildJdbcUrl(c));
        int poolSize;
        if (ConnectionConfig.TYPE_SQLITE.equalsIgnoreCase(c.getType())) {
            poolSize = 1; // SQLite 单进程强制 1
            hc.setMaximumPoolSize(1);
            Properties props = new Properties();
            props.setProperty("busy_timeout", String.valueOf(c.getSqlite().getBusyTimeoutMs()));
            // WAL 由 init 时 PRAGMA 设置（对整个数据库文件生效，一次即可）
            hc.setConnectionInitSql(null);
            hc.setDataSourceProperties(props);
        } else {
            int global = config.getDb().getPoolSize();
            poolSize = c.getPoolSize() != null ? c.getPoolSize() : global;
            hc.setMaximumPoolSize(Math.max(1, poolSize));
            hc.setUsername(c.getUsername());
            hc.setPassword(c.resolvePassword());
        }
        hc.setMinimumIdle(Math.max(1, poolSize));
        hc.setPoolName("km-sink-" + c.getName());
        hc.setConnectionTimeout(10_000);
        hc.setInitializationFailTimeout(-1); // 惰性：不在建池时强校验连通性
        hc.setAutoCommit(true);

        HikariDataSource created = new HikariDataSource(hc);
        // SQLite：对首个连接执行 WAL 等初始化 PRAGMA
        if (ConnectionConfig.TYPE_SQLITE.equalsIgnoreCase(c.getType())) {
            try (Connection conn = created.getConnection()) {
                initSqlite(conn, c);
            } catch (SQLException e) {
                log.warn("SQLite 初始化 PRAGMA 失败（连接 {}）: {}", c.getName(), e.getMessage());
            }
        }
        return created;
    }

    private void initSqlite(Connection conn, ConnectionConfig c) throws SQLException {
        try (var st = conn.createStatement()) {
            if (c.getSqlite().isWalMode()) {
                st.execute("PRAGMA journal_mode=WAL;");
            }
            st.execute("PRAGMA busy_timeout = " + c.getSqlite().getBusyTimeoutMs() + ";");
            st.execute("PRAGMA foreign_keys = ON;");
            // 验证 WAL 生效
            if (c.getSqlite().isWalMode()) {
                try (var rs = st.executeQuery("PRAGMA journal_mode;")) {
                    if (rs.next()) {
                        String mode = rs.getString(1);
                        if (!"wal".equalsIgnoreCase(mode)) {
                            log.warn("SQLite WAL 未生效（连接 {}），当前模式: {}", c.getName(), mode);
                        }
                    }
                }
            }
        }
    }

    /**
     * 测试连接可用性，返回数据库版本描述（不含敏感信息）。
     */
    public String testConnection(String connectionName) {
        Dialect dialect = dialect(connectionName);
        try (Connection conn = getConnection(connectionName)) {
            var md = conn.getMetaData();
            return dialect.dbType() + " " + md.getDatabaseProductVersion();
        } catch (SQLException e) {
            throw new DataSinkException(ErrorCode.DATABASE_CONNECTION_ERROR,
                    "连接测试失败（连接: " + connectionName + "）", e);
        }
    }

    /**
     * 关闭所有连接池，释放资源。
     */
    public void close() {
        synchronized (poolLock) {
            for (Map.Entry<String, HikariDataSource> e : pools.entrySet()) {
                try {
                    e.getValue().close();
                } catch (Exception ex) {
                    log.warn("关闭连接池失败: {} - {}", e.getKey(), ex.getMessage());
                }
            }
            pools.clear();
        }
    }
}
