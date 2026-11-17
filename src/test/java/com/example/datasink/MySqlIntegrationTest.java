package com.example.datasink;

import com.example.datasink.config.AppConfig;
import com.example.datasink.config.ConfigValidator;
import com.example.datasink.config.ConnectionConfig;
import com.example.datasink.config.TableConfig;
import com.example.datasink.db.ConnectionManager;
import com.example.datasink.engine.DataSinkException;
import com.example.datasink.engine.DocumentQueryService;
import com.example.datasink.engine.ErrorCode;
import com.example.datasink.engine.ExecutionRecorder;
import com.example.datasink.engine.SchemaManager;
import com.example.datasink.engine.UpsertWriter;
import com.example.datasink.engine.ValueResolver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * MySQL 独立集成测试（T3/T4/T23 等 MySQL 专属用例）。
 *
 * 运行条件：设置环境变量 SINK_MYSQL_TEST=1 并提供连接信息：
 *   SINK_MYSQL_HOST（默认 127.0.0.1）、SINK_MYSQL_PORT（默认 3306）、
 *   SINK_MYSQL_DB（默认 sink_test）、SINK_MYSQL_USER、SINK_MYSQL_PASSWORD
 * 未设置时自动跳过（不影响 SQLite 用例与构建）。
 */
@EnabledIfEnvironmentVariable(named = "SINK_MYSQL_TEST", matches = "1")
class MySqlIntegrationTest {

    private AppConfig config;
    private ConnectionManager manager;
    private SchemaManager schemaManager;
    private ExecutionRecorder recorder;
    private UpsertWriter writer;
    private DocumentQueryService queryService;
    private ValueResolver resolver;

    @BeforeEach
    void setUp() {
        config = buildConfig();
        ConfigValidator.validate(config);
        manager = new ConnectionManager(config);
        schemaManager = new SchemaManager(manager);
        recorder = new ExecutionRecorder(manager);
        writer = new UpsertWriter(manager, recorder, config.getDb().getMaxRetry(),
                config.getDb().getRetryBackoffMs());
        queryService = new DocumentQueryService(manager, config);
        resolver = new ValueResolver();
        schemaManager.init();
    }

    @AfterEach
    void tearDown() {
        if (manager != null) {
            manager.close();
        }
    }

    private AppConfig buildConfig() {
        AppConfig cfg = new AppConfig();
        ConnectionConfig mysql = new ConnectionConfig();
        mysql.setName("mysql");
        mysql.setType(ConnectionConfig.TYPE_MYSQL);
        mysql.setHost(env("SINK_MYSQL_HOST", "127.0.0.1"));
        mysql.setPort(Integer.parseInt(env("SINK_MYSQL_PORT", "3306")));
        mysql.setDatabase(env("SINK_MYSQL_DB", "sink_test"));
        mysql.setUsername(env("SINK_MYSQL_USER", "root"));
        mysql.setPasswordEnv("SINK_MYSQL_PASSWORD");
        cfg.getConnections().put("mysql", mysql);
        cfg.getTables().setAutoCreate(true);

        // ai_news：history 模式，含辅助唯一键（T23）
        TableConfig aiNews = table("ai_news", "history");
        col(aiNews, "task_name", "VARCHAR(128)", "task_name", false);
        col(aiNews, "run_id", "VARCHAR(128)", "run_id", false);
        col(aiNews, "business_date", "DATE", "business_date", true);
        col(aiNews, "format", "VARCHAR(8)", "format", false);
        col(aiNews, "content", "LONG_TEXT", "content", false);
        col(aiNews, "article_id", "VARCHAR(64)", "meta.article_id", false);
        aiNews.setUpsertKey(List.of("task_name", "run_id", "format"));
        aiNews.setUniqueKeys(List.of(List.of("article_id")));

        // daily_report：overwrite 模式
        TableConfig daily = table("daily_report", "overwrite");
        col(daily, "task_name", "VARCHAR(128)", "task_name", false);
        col(daily, "business_date", "DATE", "business_date", false);
        col(daily, "format", "VARCHAR(8)", "format", false);
        col(daily, "content", "LONG_TEXT", "content", false);
        daily.setUpsertKey(List.of("task_name", "business_date", "format"));

        cfg.getTables().getAllowlist().add(aiNews);
        cfg.getTables().getAllowlist().add(daily);
        return cfg;
    }

    private String env(String key, String def) {
        String v = System.getenv(key);
        return (v == null || v.isBlank()) ? def : v;
    }

    private TableConfig table(String name, String mode) {
        TableConfig t = new TableConfig();
        t.setName(name);
        t.setMode(mode);
        return t;
    }

    private void col(TableConfig t, String name, String type, String source, boolean nullable) {
        com.example.datasink.config.ColumnConfig c = new com.example.datasink.config.ColumnConfig();
        c.setColumn(name);
        c.setType(type);
        c.setSource(source);
        c.setNullable(nullable);
        t.getColumns().add(c);
    }

    private Map<String, Object> saveArgs(String table, String task, String runId,
                                         String bizDate, String format, String content,
                                         Map<String, Object> meta) {
        Map<String, Object> args = new LinkedHashMap<>();
        args.put("connection", "mysql");
        args.put("table", table);
        args.put("task_name", task);
        args.put("run_id", runId);
        args.put("business_date", bizDate);
        args.put("format", format);
        args.put("content", content);
        if (meta != null) {
            args.put("meta", meta);
        }
        return args;
    }

    private Map<String, Object> saveDocument(TableConfig table, Map<String, Object> args) {
        String connection = (String) args.get("connection");
        String taskName = (String) args.get("task_name");
        String runId = (String) args.get("run_id");
        try {
            recorder.getExecution(connection, taskName, runId);
        } catch (DataSinkException e) {
            if (e.getCode() == ErrorCode.EXECUTION_NOT_FOUND) {
                recorder.startExecution(connection, taskName, runId, null);
            } else {
                throw e;
            }
        }
        table.ensureSystemColumns();
        List<String> diffs = schemaManager.validateBusinessTable(connection, table);
        if (diffs.size() == 1 && diffs.get(0).contains("不存在")) {
            schemaManager.ensureBusinessTable(connection, table);
        } else if (!diffs.isEmpty()) {
            throw new DataSinkException(ErrorCode.SCHEMA_VALIDATION_FAILED, String.join("; ", diffs));
        }
        Map<String, Object> row = resolver.resolve(table, args, manager.dialect(connection));
        UpsertWriter.SaveResult result = writer.save(connection, table, row, taskName, runId);
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("document_id", result.documentId());
        out.put("operation", result.updated() ? "update" : "insert");
        return out;
    }

    /** T3/T4：MySQL 保存 HTML / Markdown */
    @Test
    void t3_t4_saveHtmlAndMarkdown() {
        TableConfig daily = config.getTables().findTable("daily_report");
        var r1 = saveDocument(daily, saveArgs("daily_report", "t3", "r1", "2026-11-17",
                "html", "<html><body><h1>ok</h1></body></html>", null));
        assertNotNull(r1.get("document_id"));

        TableConfig aiNews = config.getTables().findTable("ai_news");
        var r2 = saveDocument(aiNews, saveArgs("ai_news", "t4", "r1", "2026-11-17",
                "md", "# md ok", Map.of("article_id", "t4-a")));
        assertNotNull(r2.get("document_id"));
    }

    /** T23：MySQL 辅助唯一键冲突 → UNIQUE_KEY_CONFLICT，不更新其他记录 */
    @Test
    void t23_auxUniqueKeyConflict() {
        TableConfig aiNews = config.getTables().findTable("ai_news");
        saveDocument(aiNews, saveArgs("ai_news", "t23", "r1", "2026-11-17", "md", "v1",
                Map.of("article_id", "dup-23")));
        // 不同 run_id（upsert_key 不同）但 article_id 相同 → 辅助键冲突（MySQL 1062）
        DataSinkException e = assertThrows(DataSinkException.class, () ->
                saveDocument(aiNews, saveArgs("ai_news", "t23", "r2", "2026-11-17", "md", "v2",
                        Map.of("article_id", "dup-23"))));
        assertEquals(ErrorCode.UNIQUE_KEY_CONFLICT, e.getCode());
    }

    /** T5/T6/T24/T27/T29：overwrite 先查后写覆盖 + 执行记录保留 + document_id 稳定 */
    @Test
    void t24_t27_t29_overwriteSemantics() {
        TableConfig daily = config.getTables().findTable("daily_report");
        var r1 = saveDocument(daily, saveArgs("daily_report", "t24", "run_a", "2026-11-17",
                "html", "<p>v1</p>", null));
        var r2 = saveDocument(daily, saveArgs("daily_report", "t24", "run_b", "2026-11-17",
                "html", "<p>v2</p>", null));
        assertEquals("update", r2.get("operation"));
        assertEquals(r1.get("document_id"), r2.get("document_id"));
        assertEquals(ExecutionRecorder.STATUS_SUCCESS,
                recorder.getExecution("mysql", "t24", "run_a").status());
        assertEquals(ExecutionRecorder.STATUS_SUCCESS,
                recorder.getExecution("mysql", "t24", "run_b").status());
    }
}
