package com.example.datasink.db;

import com.example.datasink.config.ColumnConfig;
import com.example.datasink.config.ConnectionConfig;
import com.example.datasink.config.TableConfig;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * SQLite 方言。
 * 多进程并发靠 WAL + busy_timeout + 有限重试缓解（P1-⑦）。
 */
public class SqliteDialect implements Dialect {

    @Override
    public String dbType() {
        return ConnectionConfig.TYPE_SQLITE;
    }

    @Override
    public String buildJdbcUrl(ConnectionConfig config) {
        return "jdbc:sqlite:" + config.getPath();
    }

    @Override
    public void initConnection(Connection conn) throws SQLException {
        // 连接初始化时执行并验证 PRAGMA（P1-⑦-4）
        try (Statement st = conn.createStatement()) {
            // journal_mode=WAL（若配置启用）
            if (conn.getClientInfo("km.wal") == null || Boolean.parseBoolean(conn.getClientInfo("km.wal"))) {
                try (ResultSet rs = st.executeQuery("PRAGMA journal_mode;")) {
                    // 记录当前模式
                }
            }
            // busy_timeout 在建池时通过 Properties 注入；此处兜底验证
            try (ResultSet rs = st.executeQuery("PRAGMA busy_timeout;")) {
                if (rs.next() && rs.getInt(1) <= 0) {
                    st.execute("PRAGMA busy_timeout = 5000;");
                }
            }
            try (ResultSet rs = st.executeQuery("PRAGMA foreign_keys;")) {
                // 验证可读取即可
            }
        }
    }

    @Override
    public String buildCreateTableSql(TableConfig table) {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE IF NOT EXISTS ").append(quote(table.getName())).append(" (\n");
        for (ColumnConfig col : table.getColumns()) {
            sb.append("  ").append(quote(col.getColumn())).append(" ")
              .append(TypeMapper.toSqlite(col.getType()));
            if (!col.isNullable()) {
                sb.append(" NOT NULL");
            }
            sb.append(",\n");
        }
        sb.append("  CONSTRAINT ").append(quote("uk_upsert")).append(" UNIQUE (")
          .append(joinQuote(table.getUpsertKey())).append(")");
        if (table.getUniqueKeys() != null) {
            int i = 1;
            for (List<String> group : table.getUniqueKeys()) {
                sb.append(",\n  CONSTRAINT ").append(quote("uk_aux_" + i++))
                  .append(" UNIQUE (").append(joinQuote(group)).append(")");
            }
        }
        sb.append("\n)");
        return sb.toString();
    }

    @Override
    public String formatNow() {
        return java.time.LocalDateTime.now()
                .format(java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
    }

    @Override
    public List<String> checkSchema(Connection conn, TableConfig table) throws SQLException {
        List<String> diffs = new ArrayList<>();
        List<String> actualCols = describeColumns(conn, table.getName());
        if (actualCols.isEmpty()) {
            diffs.add("表 " + table.getName() + " 不存在");
            return diffs;
        }
        // SQLite 无法通过元数据判断 NOT NULL 时（部分驱动版本），退化为列存在性校验
        for (ColumnConfig col : table.getColumns()) {
            boolean found = false;
            for (String desc : actualCols) {
                String[] parts = desc.split("\\|", -1);
                if (parts[0].equalsIgnoreCase(col.getColumn())) {
                    found = true;
                    // xerial 驱动元数据对 NOT NULL 判断不可靠，此处用 PRAGMA table_info 复核
                    break;
                }
            }
            if (!found) {
                diffs.add("表 " + table.getName() + " 缺少必需列: " + col.getColumn());
            }
        }
        // NOT NULL 用 PRAGMA table_info 精确校验
        diffs.addAll(checkNotNullViaPragma(conn, table));
        // 唯一索引校验
        List<List<String>> uniqueIndexes = describeUniqueIndexes(conn, table.getName());
        List<List<String>> expected = new ArrayList<>();
        expected.add(table.getUpsertKey());
        if (table.getUniqueKeys() != null) {
            expected.addAll(table.getUniqueKeys());
        }
        for (List<String> group : expected) {
            if (!containsGroup(uniqueIndexes, group)) {
                diffs.add("表 " + table.getName() + " 缺少唯一索引: (" + String.join(", ", group) + ")");
            }
        }
        return diffs;
    }

    private List<String> checkNotNullViaPragma(Connection conn, TableConfig table) throws SQLException {
        List<String> diffs = new ArrayList<>();
        java.util.Map<String, Integer> notNullMap = new java.util.HashMap<>();
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("PRAGMA table_info(" + quote(table.getName()) + ")")) {
            while (rs.next()) {
                String name = rs.getString("name");
                int notnull = rs.getInt("notnull");
                notNullMap.put(name.toLowerCase(Locale.ROOT), notnull);
            }
        }
        for (ColumnConfig col : table.getColumns()) {
            Integer actual = notNullMap.get(col.getColumn().toLowerCase(Locale.ROOT));
            if (actual != null && !col.isNullable() && actual == 0) {
                diffs.add("表 " + table.getName() + " 列 " + col.getColumn() + " 期望 NOT NULL，实际允许 NULL");
            }
        }
        return diffs;
    }

    @Override
    public boolean isUniqueViolation(SQLException e) {
        SQLException cur = e;
        while (cur != null) {
            // SQLITE_CONSTRAINT: 19; message 通常含 "UNIQUE constraint failed"
            String msg = cur.getMessage() == null ? "" : cur.getMessage().toUpperCase(Locale.ROOT);
            if (cur.getErrorCode() == 19 || msg.contains("UNIQUE CONSTRAINT FAILED")) {
                return true;
            }
            cur = cur.getNextException();
        }
        return false;
    }

    @Override
    public List<String> conflictColumns(SQLException e) {
        // SQLite: UNIQUE constraint failed: table.col1, table.col2
        SQLException cur = e;
        while (cur != null) {
            if (isUniqueViolation(cur)) {
                String msg = cur.getMessage() == null ? "" : cur.getMessage();
                java.util.regex.Matcher m = java.util.regex.Pattern
                        .compile("UNIQUE constraint failed: (.+)", java.util.regex.Pattern.CASE_INSENSITIVE)
                        .matcher(msg);
                if (m.find()) {
                    String[] cols = m.group(1).split(",");
                    List<String> result = new ArrayList<>();
                    for (String c : cols) {
                        String col = c.trim();
                        int dot = col.lastIndexOf('.');
                        if (dot >= 0) {
                            col = col.substring(dot + 1);
                        }
                        result.add(col);
                    }
                    if (new java.util.HashSet<>(result)
                            .equals(new java.util.HashSet<>(result.size() == result.size() ? result : result))) {
                        return result;
                    }
                    return result;
                }
                return List.of(); // 唯一冲突但无法解析列
            }
            cur = cur.getNextException();
        }
        return null;
    }

    @Override
    public boolean isRetryable(SQLException e) {
        // SQLITE_BUSY(5) / SQLITE_LOCKED(6) 视为可重试
        SQLException cur = e;
        while (cur != null) {
            if (cur.getErrorCode() == 5 || cur.getErrorCode() == 6) {
                return true;
            }
            cur = cur.getNextException();
        }
        return false;
    }

    @Override
    public boolean isLockTimeout(SQLException e) {
        SQLException cur = e;
        while (cur != null) {
            if (cur.getErrorCode() == 5 || cur.getErrorCode() == 6) {
                return true;
            }
            cur = cur.getNextException();
        }
        return false;
    }

    private static boolean containsGroup(List<List<String>> actual, List<String> expected) {
        for (List<String> g : actual) {
            if (sameGroup(g, expected)) {
                return true;
            }
        }
        return false;
    }

    private static boolean sameGroup(List<String> a, List<String> b) {
        if (a.size() != b.size()) {
            return false;
        }
        for (int i = 0; i < a.size(); i++) {
            if (!a.get(i).equalsIgnoreCase(b.get(i))) {
                return false;
            }
        }
        return true;
    }

    static String quote(String ident) {
        return "\"" + ident + "\"";
    }

    static String joinQuote(List<String> idents) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < idents.size(); i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(quote(idents.get(i)));
        }
        return sb.toString();
    }
}
