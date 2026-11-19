package com.example.datasink;

import com.example.datasink.config.ColumnConfig;
import com.example.datasink.config.ConnectionConfig;
import com.example.datasink.config.TableConfig;
import com.example.datasink.db.MySqlDialect;
import com.example.datasink.db.SqliteDialect;
import com.example.datasink.db.TypeMapper;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 方言层无服务依赖单元测试（MySQL 方言逻辑独立验证；
 * 真实 MySQL 集成测试见 MySqlIntegrationTest，需 SINK_MYSQL_TEST=1）。
 */
class DialectTest {

    private TableConfig sampleTable() {
        TableConfig t = new TableConfig();
        t.setName("daily_report");
        t.setMode(TableConfig.MODE_OVERWRITE);
        col(t, "task_name", "VARCHAR(128)", "task_name", false);
        col(t, "business_date", "DATE", "business_date", false);
        col(t, "format", "VARCHAR(8)", "format", false);
        col(t, "content", "LONG_TEXT", "content", false);
        t.setUpsertKey(List.of("task_name", "business_date", "format"));
        return t;
    }

    private void col(TableConfig t, String name, String type, String source, boolean nullable) {
        ColumnConfig c = new ColumnConfig();
        c.setColumn(name);
        c.setType(type);
        c.setSource(source);
        c.setNullable(nullable);
        t.getColumns().add(c);
    }

    // ---------- TypeMapper（方案 9.2 类型映射） ----------

    @Test
    void typeMapperMySql() {
        assertEquals("LONGTEXT", TypeMapper.toMySql("LONG_TEXT"));
        assertEquals("TEXT", TypeMapper.toMySql("TEXT"));
        assertEquals("DATETIME", TypeMapper.toMySql("TIMESTAMP"));
        assertEquals("TINYINT(1)", TypeMapper.toMySql("BOOLEAN"));
        assertEquals("VARCHAR(128)", TypeMapper.toMySql("VARCHAR(128)"));
        assertEquals("DECIMAL(10,2)", TypeMapper.toMySql("DECIMAL(10,2)"));
        assertEquals("INT", TypeMapper.toMySql("INT"));
    }

    @Test
    void typeMapperSqlite() {
        assertEquals("TEXT", TypeMapper.toSqlite("LONG_TEXT"));
        assertEquals("INTEGER", TypeMapper.toSqlite("INT"));
        assertEquals("INTEGER", TypeMapper.toSqlite("BIGINT"));
        assertEquals("INTEGER", TypeMapper.toSqlite("BOOLEAN"));
        assertEquals("DATETIME", TypeMapper.toSqlite("TIMESTAMP"));
        assertEquals("NUMERIC(10,2)", TypeMapper.toSqlite("DECIMAL(10,2)"));
    }

    @Test
    void typeMapperJdbcTypes() {
        ColumnConfig c = new ColumnConfig();
        c.setType("LONG_TEXT");
        assertEquals(java.sql.Types.LONGVARCHAR, TypeMapper.toJdbcType(c));
        c.setType("DATE");
        assertEquals(java.sql.Types.DATE, TypeMapper.toJdbcType(c));
    }

    // ---------- MySqlDialect DDL 生成（方案 8.3） ----------

    @Test
    void mysqlCreateTableSql() {
        TableConfig t = sampleTable();
        t.setUniqueKeys(List.of(List.of("document_id")));
        String ddl = new MySqlDialect().buildCreateTableSql(t);
        assertTrue(ddl.startsWith("CREATE TABLE IF NOT EXISTS `daily_report`"), ddl);
        assertTrue(ddl.contains("`content` LONGTEXT NOT NULL"), "LONG_TEXT → LONGTEXT: " + ddl);
        assertTrue(ddl.contains("`business_date` DATE NOT NULL"), ddl);
        assertTrue(ddl.contains("CONSTRAINT `uk_upsert` UNIQUE (`task_name`, `business_date`, `format`)"), ddl);
        assertTrue(ddl.contains("CONSTRAINT `uk_aux_1` UNIQUE (`document_id`)"), ddl);
        assertTrue(ddl.contains("ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"), ddl);
        // 未声明注释时不应出现 COMMENT 关键字
        assertFalse(ddl.contains("COMMENT"), "无注释配置不应生成 COMMENT: " + ddl);
    }

    // ---------- 表/列注释（仅 MySQL DDL 生效） ----------

    @Test
    void mysqlCreateTableSqlWithComments() {
        TableConfig t = sampleTable();
        t.setComment("日报落库（覆盖模式，同键更新最新记录）");
        // 给前两列补注释（复用已有列对象）
        t.getColumns().get(0).setComment("任务名称");
        t.getColumns().get(1).setComment("业务日期");
        String ddl = new MySqlDialect().buildCreateTableSql(t);
        assertTrue(ddl.contains("`task_name` VARCHAR(128) NOT NULL COMMENT '任务名称'"), ddl);
        assertTrue(ddl.contains("`business_date` DATE NOT NULL COMMENT '业务日期'"), ddl);
        // 未设置注释的列不拼接 COMMENT
        assertTrue(ddl.contains("`content` LONGTEXT NOT NULL,\n"), ddl);
        assertTrue(ddl.endsWith("ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='日报落库（覆盖模式，同键更新最新记录）'"), ddl);
    }

    @Test
    void mysqlCreateTableSqlCommentEscaping() {
        TableConfig t = sampleTable();
        // 单引号需翻倍，换行需压平，避免破坏 DDL
        t.getColumns().get(0).setComment("任务'名称'\n第二行");
        t.setComment("表'注释'");
        String ddl = new MySqlDialect().buildCreateTableSql(t);
        assertTrue(ddl.contains("COMMENT '任务''名称'' 第二行'"), ddl);
        assertTrue(ddl.contains("COMMENT='表''注释'"), ddl);
    }

    @Test
    void systemColumnsHaveDefaultComments() {
        TableConfig t = sampleTable();
        t.ensureSystemColumns();
        assertEquals("文档唯一ID（服务端生成 UUID）",
                t.findColumn("document_id").getComment());
        assertEquals("记录创建时间（服务端生成）",
                t.findColumn("created_at").getComment());
    }

    @Test
    void mysqlJdbcUrl() {
        ConnectionConfig c = new ConnectionConfig();
        c.setHost("127.0.0.1");
        c.setPort(3306);
        c.setDatabase("sink_data");
        String url = new MySqlDialect().buildJdbcUrl(c);
        assertEquals("jdbc:mysql://127.0.0.1:3306/sink_data"
                + "?useSSL=false&serverTimezone=UTC&rewriteBatchedStatements=true"
                + "&allowPublicKeyRetrieval=true&connectTimeout=10000&socketTimeout=60000", url);
    }

    @Test
    void sqliteJdbcUrl() {
        ConnectionConfig c = new ConnectionConfig();
        c.setPath("./data/local.db");
        assertEquals("jdbc:sqlite:./data/local.db", new SqliteDialect().buildJdbcUrl(c));
    }

    @Test
    void sqliteCreateTableSql() {
        TableConfig t = sampleTable();
        t.setComment("日报表注释");
        t.getColumns().get(0).setComment("任务名称注释");
        String ddl = new SqliteDialect().buildCreateTableSql(t);
        assertTrue(ddl.startsWith("CREATE TABLE IF NOT EXISTS \"daily_report\""), ddl);
        assertTrue(ddl.contains("\"content\" TEXT NOT NULL"), "LONG_TEXT → TEXT: " + ddl);
        assertTrue(ddl.contains("CONSTRAINT \"uk_upsert\" UNIQUE (\"task_name\", \"business_date\", \"format\")"), ddl);
        // SQLite 无注释语法，配置的 comment 不参与 DDL
        assertFalse(ddl.contains("COMMENT"), "SQLite DDL 不应包含 COMMENT: " + ddl);
    }

    // ---------- MySQL 唯一冲突解析（方案 8.4 / 9.3：区分 uk_upsert 与 uk_aux_N） ----------

    @Test
    void mysqlUniqueViolationDetection() {
        MySqlDialect d = new MySqlDialect();
        SQLException e1062 = new SQLException("Duplicate entry 'a-2026-11-17-html' for key 'daily_report.uk_upsert'", "23000", 1062);
        assertTrue(d.isUniqueViolation(e1062));

        SQLException eOther = new SQLException("Some other error", "HY000", 1045);
        assertFalse(d.isUniqueViolation(eOther));
    }

    @Test
    void mysqlConflictColumnsUpsertKey() {
        MySqlDialect d = new MySqlDialect();
        SQLException e = new SQLException("Duplicate entry 'x' for key 'uk_upsert'", "23000", 1062);
        // 方言契约：uk_upsert 冲突返回 null（由 UpsertWriter 保守视为 upsert_key 冲突）；
        // 辅助键冲突返回索引名（uk_aux_N）
        assertNull(d.conflictColumns(e));
        SQLException aux = new SQLException("Duplicate entry 'doc1' for key 'uk_aux_1'", "23000", 1062);
        assertEquals(List.of("uk_aux_1"), d.conflictColumns(aux));
    }

    // ---------- MySQL 可重试异常判定（方案 8.5-2/3：有限重试仅针对可重试异常） ----------

    @Test
    void mysqlRetryableClassification() {
        MySqlDialect d = new MySqlDialect();
        // 08xxx 连接类异常 → 可重试
        assertTrue(d.isRetryable(new SQLException("Communications link failure", "08S01", 0)));
        // 死锁 1213 → 可重试
        assertTrue(d.isRetryable(new SQLException("Deadlock found", "40001", 1213)));
        // 锁等待超时 1205 → 可重试 + 锁超时
        SQLException lockWait = new SQLException("Lock wait timeout", "HY000", 1205);
        assertTrue(d.isRetryable(lockWait));
        assertTrue(d.isLockTimeout(lockWait));
        // 语法错误 → 不可重试
        assertFalse(d.isRetryable(new SQLException("You have an error in your SQL syntax", "42000", 1064)));
        assertFalse(d.isLockTimeout(new SQLException("syntax error", "42000", 1064)));
    }

    @Test
    void sqliteRetryableClassification() {
        SqliteDialect d = new SqliteDialect();
        // SQLITE_BUSY(5) / SQLITE_LOCKED(6) → 可重试 + 锁超时
        SQLException busy = new SQLException("[SQLITE_BUSY] The database file is locked", null, 5);
        assertTrue(d.isRetryable(busy));
        assertTrue(d.isLockTimeout(busy));
        assertTrue(d.isUniqueViolation(new SQLException(
                "[SQLITE_CONSTRAINT] UNIQUE constraint failed: daily_report.task_name", null, 19)));
        // 语法类 → 不可重试
        assertFalse(d.isRetryable(new SQLException("[SQLITE_ERROR] SQL error", null, 1)));
    }

    @Test
    void sqliteConflictColumnsFromMessage() {
        SqliteDialect d = new SqliteDialect();
        SQLException e = new SQLException(
                "[SQLITE_CONSTRAINT] UNIQUE constraint failed: daily_report.task_name, daily_report.business_date", null, 19);
        List<String> cols = d.conflictColumns(e);
        assertEquals(List.of("task_name", "business_date"), cols);
    }

    @Test
    void dialectDbType() {
        assertEquals("mysql", new MySqlDialect().dbType());
        assertEquals("sqlite", new SqliteDialect().dbType());
    }
}
