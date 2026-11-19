package com.example.datasink.config;

import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * 启动校验（P1-⑩）。配置错误必须在启动阶段拦截，阻止服务启动，日志输出具体位置。
 */
public final class ConfigValidator {

    private ConfigValidator() {
    }

    /** 合法通用类型（含参数化形式单独校验） */
    private static final Set<String> SIMPLE_TYPES = Set.of(
            "TEXT", "LONG_TEXT", "INT", "BIGINT", "DATE", "TIMESTAMP", "BOOLEAN");
    private static final Pattern VARCHAR_PATTERN = Pattern.compile("VARCHAR\\((\\d+)\\)");
    private static final Pattern DECIMAL_PATTERN = Pattern.compile("DECIMAL\\((\\d+)\\s*,\\s*(\\d+)\\)");
    /** 表名/列名规范：字母数字下划线，字母开头 */
    private static final Pattern IDENTIFIER = Pattern.compile("[A-Za-z][A-Za-z0-9_]*");

    /** 校验失败异常，携带全部问题 */
    public static class ValidationException extends RuntimeException {
        public ValidationException(String message) {
            super(message);
        }
    }

    public static void validate(AppConfig config) {
        Set<String> tableNames = new HashSet<>();
        Set<String> connectionNames = new HashSet<>();

        if (config.getConnections() == null || config.getConnections().isEmpty()) {
            throw new ValidationException("配置错误：connections 不能为空，至少定义一个数据库连接");
        }
        config.getConnections().forEach((name, conn) -> validateConnection(name, conn, connectionNames));

        if (config.getTables() == null || config.getTables().getAllowlist() == null
                || config.getTables().getAllowlist().isEmpty()) {
            throw new ValidationException("配置错误：tables.allowlist 不能为空，至少声明一张白名单表");
        }

        StringBuilder errors = new StringBuilder();
        for (TableConfig table : config.getTables().getAllowlist()) {
            validateTable(table, errors);
            if (!tableNames.add(lower(table.getName()))) {
                errors.append("tables.allowlist 中表名重复: ").append(table.getName()).append("; ");
            }
        }

        if (config.getTables().getAllowlist().stream().anyMatch(t ->
                t.getUpsertKey() == null || t.getUpsertKey().isEmpty())) {
            errors.append("每张表必须明确指定单组 upsert_key; ");
        }

        if (errors.length() > 0) {
            throw new ValidationException("配置校验失败: " + errors);
        }
    }

    private static void validateConnection(String name, ConnectionConfig conn, Set<String> names) {
        if (name == null || name.isBlank()) {
            throw new ValidationException("配置错误：connections 存在空连接名");
        }
        if (!names.add(name)) {
            throw new ValidationException("配置错误：连接名重复: " + name);
        }
        String type = lower(conn.getType());
        if (!ConnectionConfig.TYPE_MYSQL.equals(type) && !ConnectionConfig.TYPE_SQLITE.equals(type)) {
            throw new ValidationException("配置错误：连接 " + name + " 的 type 仅支持 mysql/sqlite，当前: " + conn.getType());
        }
        if (ConnectionConfig.TYPE_MYSQL.equals(type)) {
            if (isBlank(conn.getHost())) {
                throw new ValidationException("配置错误：MySQL 连接 " + name + " 缺少 host");
            }
            if (conn.getPort() == null) {
                throw new ValidationException("配置错误：MySQL 连接 " + name + " 缺少 port");
            }
            if (isBlank(conn.getDatabase())) {
                throw new ValidationException("配置错误：MySQL 连接 " + name + " 缺少 database");
            }
            if (isBlank(conn.getUsername())) {
                throw new ValidationException("配置错误：MySQL 连接 " + name + " 缺少 username");
            }
            if (conn.isPasswordEnvMissing()) {
                throw new ValidationException("配置错误：MySQL 连接 " + name
                        + " 的 password_env 环境变量未设置: " + conn.getPasswordEnv()
                        + "（请设置系统环境变量，或在配置中改用 password 字段写明文）");
            }
            if (isBlank(conn.resolvePassword())) {
                throw new ValidationException("配置错误：MySQL 连接 " + name + " 缺少密码（password_env 未设置且 password 为空）");
            }
        } else {
            if (isBlank(conn.getPath())) {
                throw new ValidationException("配置错误：SQLite 连接 " + name + " 缺少 path");
            }
        }
    }

    private static void validateTable(TableConfig table, StringBuilder errors) {
        String tn = table.getName() == null ? "<null>" : table.getName();
        if (!IDENTIFIER.matcher(tn).matches()) {
            errors.append("表名不合规范（须字母开头，仅字母数字下划线）: ").append(tn).append("; ");
        }
        String mode = lower(table.getMode());
        if (!TableConfig.MODE_OVERWRITE.equals(mode) && !TableConfig.MODE_HISTORY.equals(mode)) {
            errors.append("表 ").append(tn).append(" 的 mode 仅支持 overwrite/history，当前: ").append(table.getMode()).append("; ");
        }
        if (table.getColumns() == null || table.getColumns().isEmpty()) {
            errors.append("表 ").append(tn).append(" 缺少 columns 定义; ");
            return;
        }

        Set<String> columnNames = new HashSet<>();
        for (ColumnConfig col : table.getColumns()) {
            String cn = col.getColumn() == null ? "<null>" : col.getColumn();
            if (!IDENTIFIER.matcher(cn).matches()) {
                errors.append("表 ").append(tn).append(" 列名不合规范: ").append(cn).append("; ");
            }
            if (!columnNames.add(lower(cn))) {
                errors.append("表 ").append(tn).append(" 列名重复: ").append(cn).append("; ");
            }
            validateType(tn, cn, col.getType(), errors);
            String src = col.getSource();
            if (isBlank(src)) {
                errors.append("表 ").append(tn).append(" 列 ").append(cn).append(" 缺少 source; ");
            } else if (!col.isSystemSource() && !col.isTopLevelSource() && !col.isMetaSource()) {
                errors.append("表 ").append(tn).append(" 列 ").append(cn)
                      .append(" source 非法: ").append(src)
                      .append("（允许 content/format/task_name/run_id/business_date/meta.xxx/system.uuid/system.now）; ");
            }
        }

        // 强制系统列不得由调用方声明为调用方来源
        if ("document_id".equalsIgnoreCase(cn(table, "document_id"))) {
            ColumnConfig docId = table.findColumn("document_id");
            if (docId != null && !docId.isSystemSource()) {
                errors.append("表 ").append(tn)
                      .append(" document_id 为系统强制列，source 必须为 system.uuid（不接受调用方传入）; ");
            }
        }
        if ("created_at".equalsIgnoreCase(cn(table, "created_at"))) {
            ColumnConfig createdAt = table.findColumn("created_at");
            if (createdAt != null && !createdAt.isSystemSource()) {
                errors.append("表 ").append(tn)
                      .append(" created_at 为系统强制列，source 必须为 system.now; ");
            }
        }

        // upsert_key 校验（P0-①：单组、唯一、NOT NULL）
        List<String> upsertKey = table.getUpsertKey();
        if (upsertKey == null || upsertKey.isEmpty()) {
            errors.append("表 ").append(tn).append(" 必须指定单组 upsert_key; ");
        } else {
            for (String key : upsertKey) {
                ColumnConfig col = table.findColumn(key);
                if (col == null) {
                    errors.append("表 ").append(tn).append(" upsert_key 引用的列不存在于 columns: ").append(key).append("; ");
                } else if (col.isNullable()) {
                    errors.append("表 ").append(tn).append(" upsert_key 列必须 nullable: false: ").append(key).append("; ");
                }
            }
        }

        // unique_keys 辅助唯一索引校验：引用列必须存在且 NOT NULL
        if (table.getUniqueKeys() != null) {
            for (List<String> group : table.getUniqueKeys()) {
                if (group == null || group.isEmpty()) {
                    errors.append("表 ").append(tn).append(" unique_keys 存在空组; ");
                    continue;
                }
                for (String key : group) {
                    ColumnConfig col = table.findColumn(key);
                    if (col == null) {
                        errors.append("表 ").append(tn).append(" unique_keys 引用的列不存在于 columns: ").append(key).append("; ");
                    } else if (col.isNullable()) {
                        errors.append("表 ").append(tn)
                              .append(" 参与唯一索引的列不允许为 NULL，需 nullable: false: ").append(key).append("; ");
                    }
                }
            }
        }
    }

    private static void validateType(String table, String column, String type, StringBuilder errors) {
        if (isBlank(type)) {
            errors.append("表 ").append(table).append(" 列 ").append(column).append(" 缺少 type; ");
            return;
        }
        String t = type.trim().toUpperCase(Locale.ROOT);
        if (SIMPLE_TYPES.contains(t)) {
            return;
        }
        if (VARCHAR_PATTERN.matcher(t).matches() || DECIMAL_PATTERN.matcher(t).matches()) {
            return;
        }
        errors.append("表 ").append(table).append(" 列 ").append(column)
              .append(" type 非法: ").append(type)
              .append("（允许 TEXT/LONG_TEXT/VARCHAR(n)/INT/BIGINT/DECIMAL(p,s)/DATE/TIMESTAMP/BOOLEAN）; ");
    }

    private static String cn(TableConfig table, String name) {
        return table.findColumn(name) == null ? "" : name;
    }

    private static String lower(String s) {
        return s == null ? "" : s.toLowerCase(Locale.ROOT);
    }

    private static boolean isBlank(String s) {
        return s == null || s.isBlank();
    }
}
