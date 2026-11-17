package com.example.datasink.db;

import com.example.datasink.config.ColumnConfig;
import com.example.datasink.config.ConnectionConfig;
import com.example.datasink.config.TableConfig;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * MySQL 方言。
 * upsert 采用"先查后写"策略（ON DUPLICATE KEY UPDATE 会响应任意唯一索引，
 * 无法限定冲突键，见方案 8.4/9.3）。
 */
public class MySqlDialect implements Dialect {

    @Override
    public String dbType() {
        return ConnectionConfig.TYPE_MYSQL;
    }

    @Override
    public String buildJdbcUrl(ConnectionConfig config) {
        return "jdbc:mysql://" + config.getHost() + ":" + config.getPort() + "/" + config.getDatabase()
                + "?useSSL=false&serverTimezone=UTC&rewriteBatchedStatements=true"
                + "&allowPublicKeyRetrieval=true&connectTimeout=10000&socketTimeout=60000";
    }

    @Override
    public String buildCreateTableSql(TableConfig table) {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE IF NOT EXISTS ").append(quote(table.getName())).append(" (\n");
        List<String> pkCols = new ArrayList<>();
        for (ColumnConfig col : table.getColumns()) {
            sb.append("  ").append(quote(col.getColumn())).append(" ")
              .append(TypeMapper.toMySql(col.getType()));
            if (!col.isNullable()) {
                sb.append(" NOT NULL");
            }
            sb.append(",\n");
        }
        // upsert_key 唯一约束
        sb.append("  CONSTRAINT ").append(quote("uk_upsert")).append(" UNIQUE (")
          .append(joinQuote(table.getUpsertKey())).append(")");
        // 辅助唯一索引
        if (table.getUniqueKeys() != null) {
            int i = 1;
            for (List<String> group : table.getUniqueKeys()) {
                sb.append(",\n  CONSTRAINT ").append(quote("uk_aux_" + i++))
                  .append(" UNIQUE (").append(joinQuote(group)).append(")");
            }
        }
        sb.append("\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4");
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
        // 列存在性 + NOT NULL 校验
        for (ColumnConfig col : table.getColumns()) {
            boolean found = false;
            for (String desc : actualCols) {
                String[] parts = desc.split("\\|", -1);
                if (parts[0].equalsIgnoreCase(col.getColumn())) {
                    found = true;
                    boolean actualNullable = Boolean.parseBoolean(parts[2].substring("nullable=".length()));
                    if (!col.isNullable() && actualNullable) {
                        diffs.add("表 " + table.getName() + " 列 " + col.getColumn() + " 期望 NOT NULL，实际允许 NULL");
                    }
                    break;
                }
            }
            if (!found) {
                diffs.add("表 " + table.getName() + " 缺少必需列: " + col.getColumn());
            }
        }
        // 唯一索引校验：upsert_key 与 unique_keys 必须有对应唯一索引
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

    @Override
    public boolean isUniqueViolation(SQLException e) {
        // MySQL error 1062: Duplicate entry '%s' for key '%s'
        SQLException cur = e;
        while (cur != null) {
            if (cur.getErrorCode() == 1062) {
                return true;
            }
            cur = cur.getNextException();
        }
        return false;
    }

    @Override
    public List<String> conflictColumns(SQLException e) {
        // 1062 消息形如：Duplicate entry 'xxx' for key 'uk_upsert' / 'uk_aux_1'
        SQLException cur = e;
        while (cur != null) {
            if (cur.getErrorCode() == 1062) {
                String msg = cur.getMessage() == null ? "" : cur.getMessage().toLowerCase(Locale.ROOT);
                java.util.regex.Matcher m = java.util.regex.Pattern
                        .compile("for key ['\"]?([\\w.]+)['\"]?", java.util.regex.Pattern.CASE_INSENSITIVE)
                        .matcher(msg);
                if (m.find()) {
                    String idxName = m.group(1);
                    // 形如 db.uk_upsert 时取最后一段
                    int dot = idxName.lastIndexOf('.');
                    if (dot >= 0) {
                        idxName = idxName.substring(dot + 1);
                    }
                    if (idxName.equalsIgnoreCase("uk_upsert")) {
                        return null; // upsert_key 冲突（按约定返回 null 表示 upsert 键冲突）
                    }
                    // 辅助键冲突：返回该索引名（由调用方比对 unique_keys 生成名）
                    return List.of(idxName);
                }
                return null; // 是 1062 但无法解析索引名，保守视为 upsert_key 冲突
            }
            cur = cur.getNextException();
        }
        return null;
    }

    @Override
    public boolean isRetryable(SQLException e) {
        SQLException cur = e;
        while (cur != null) {
            // 08xxx 连接异常类；40001 死锁；1213 死锁；1205 锁等待超时
            String state = cur.getSQLState() == null ? "" : cur.getSQLState();
            if (state.startsWith("08") || state.equals("40001")
                    || cur.getErrorCode() == 1213 || cur.getErrorCode() == 1205) {
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
            if (cur.getErrorCode() == 1205 || cur.getErrorCode() == 1213) {
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

    private static String quote(String ident) {
        return "`" + ident + "`";
    }

    private static String joinQuote(List<String> idents) {
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
