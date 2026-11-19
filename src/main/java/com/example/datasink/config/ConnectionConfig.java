package com.example.datasink.config;

/**
 * 连接定义。
 */
public class ConnectionConfig {

    public static final String TYPE_MYSQL = "mysql";
    public static final String TYPE_SQLITE = "sqlite";

    /** 连接名（Map key），此处为 yaml 中显式命名字段时使用 */
    private String name;

    /** mysql / sqlite */
    private String type;

    // MySQL 专用
    private String host;
    private Integer port;
    private String database;
    private String username;
    private String passwordEnv;
    /** 明文密码（不推荐，优先 password_env） */
    private String password;

    // SQLite 专用
    private String path;
    private SqliteConfig sqlite = new SqliteConfig();

    // 通用（可覆盖全局 db 配置）
    private Integer poolSize;
    private Integer queryTimeoutS;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public Integer getPort() {
        return port;
    }

    public void setPort(Integer port) {
        this.port = port;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPasswordEnv() {
        return passwordEnv;
    }

    public void setPasswordEnv(String passwordEnv) {
        this.passwordEnv = passwordEnv;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public SqliteConfig getSqlite() {
        return sqlite;
    }

    public void setSqlite(SqliteConfig sqlite) {
        this.sqlite = sqlite;
    }

    public Integer getPoolSize() {
        return poolSize;
    }

    public void setPoolSize(Integer poolSize) {
        this.poolSize = poolSize;
    }

    public Integer getQueryTimeoutS() {
        return queryTimeoutS;
    }

    public void setQueryTimeoutS(Integer queryTimeoutS) {
        this.queryTimeoutS = queryTimeoutS;
    }

    /** SQLite 专属配置 */
    public static class SqliteConfig {
        private int busyTimeoutMs = 5000;
        private boolean walMode = true;

        public int getBusyTimeoutMs() {
            return busyTimeoutMs;
        }

        public void setBusyTimeoutMs(int busyTimeoutMs) {
            this.busyTimeoutMs = busyTimeoutMs;
        }

        public boolean isWalMode() {
            return walMode;
        }

        public void setWalMode(boolean walMode) {
            this.walMode = walMode;
        }
    }

    /** 解析后的实际密码：password_env 优先（变量存在时），不存在则回退明文 password */
    public String resolvePassword() {
        if (passwordEnv != null && !passwordEnv.isBlank()) {
            String v = System.getenv(passwordEnv);
            if (v != null && !v.isEmpty()) {
                return v;
            }
        }
        return password == null ? "" : password;
    }

    /** password_env 指定的环境变量是否已设置 */
    public boolean isPasswordEnvMissing() {
        return passwordEnv != null && !passwordEnv.isBlank()
                && System.getenv(passwordEnv) == null && (password == null || password.isBlank());
    }
}
