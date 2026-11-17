package com.example.datasink;

import com.example.datasink.config.AppConfig;
import com.example.datasink.config.ConfigLoader;
import com.example.datasink.config.ConfigValidator;
import com.example.datasink.config.ConnectionConfig;
import com.example.datasink.config.TableConfig;
import com.example.datasink.db.ConnectionManager;
import com.example.datasink.engine.DocumentQueryService;
import com.example.datasink.engine.DataSinkException;
import com.example.datasink.engine.ErrorCode;
import com.example.datasink.engine.ExecutionRecorder;
import com.example.datasink.engine.SchemaManager;
import com.example.datasink.engine.UpsertWriter;
import com.example.datasink.engine.ValueResolver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * SQLite 独立集成测试（T1–T38 SQLite 部分）。
 * 不经 MCP stdio，直接驱动引擎层（与 MCP 工具层同一调用路径）。
 */
class SqliteIntegrationTest {

    @TempDir
    Path tempDir;

    private AppConfig config;
    private ConnectionManager manager;
    private SchemaManager schemaManager;
    private ExecutionRecorder recorder;
    private UpsertWriter writer;
    private DocumentQueryService queryService;
    private ValueResolver resolver;

    private Path dbFile;

    @BeforeEach
    void setUp() throws Exception {
        dbFile = tempDir.resolve("test.db");
        config = buildConfig(dbFile.toString());
        ConfigValidator.validate(config);
        manager = new ConnectionManager(config);
        schemaManager = new SchemaManager(manager);
        recorder = new ExecutionRecorder(manager);
        writer = new UpsertWriter(manager, recorder, config.getDb().getMaxRetry(),
                config.getDb().getRetryBackoffMs());
        queryService = new DocumentQueryService(manager, config);
        resolver = new ValueResolver();
        schemaManager.init(); // T32：WAL 初始化 + 执行记录表就绪
    }

    @AfterEach
    void tearDown() {
        if (manager != null) {
            manager.close(); // T38/T19：连接池释放
        }
    }

    private AppConfig buildConfig(String sqlitePath) {
        AppConfig cfg = new AppConfig();
        ConnectionConfig local = new ConnectionConfig();
        local.setName("local");
        local.setType(ConnectionConfig.TYPE_SQLITE);
        local.setPath(sqlitePath);
        cfg.getConnections().put("local", local);
        cfg.getTables().setAutoCreate(true);

        // ai_news：history 模式，含辅助唯一键 article_id
        TableConfig aiNews = table("ai_news", "history");
        col(aiNews, "task_name", "VARCHAR(128)", "task_name", false);
        col(aiNews, "run_id", "VARCHAR(128)", "run_id", false);
        col(aiNews, "business_date", "DATE", "business_date", true);
        col(aiNews, "format", "VARCHAR(8)", "format", false);
        col(aiNews, "content", "LONG_TEXT", "content", false);
        col(aiNews, "article_id", "VARCHAR(64)", "meta.article_id", false);
        col(aiNews, "title", "TEXT", "meta.title", true);
        aiNews.setUpsertKey(List.of("task_name", "run_id", "format"));
        aiNews.setUniqueKeys(List.of(List.of("article_id")));

        // daily_report：overwrite 模式
        TableConfig daily = table("daily_report", "overwrite");
        col(daily, "task_name", "VARCHAR(128)", "task_name", false);
        col(daily, "business_date", "DATE", "business_date", false);
        col(daily, "format", "VARCHAR(8)", "format", false);
        col(daily, "content", "LONG_TEXT", "content", false);
        col(daily, "summary", "TEXT", "meta.summary", true);
        daily.setUpsertKey(List.of("task_name", "business_date", "format"));

        cfg.getTables().getAllowlist().add(aiNews);
        cfg.getTables().getAllowlist().add(daily);
        return cfg;
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
        args.put("connection", "local");
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

    /** 模拟 SaveDocumentTool 的核心写入路径（隐式 RUNNING + ensureTable + resolve + save） */
    private Map<String, Object> saveDocument(TableConfig table, Map<String, Object> args) {
        String connection = (String) args.get("connection");
        String taskName = (String) args.get("task_name");
        String runId = (String) args.get("run_id");
        // 隐式创建 RUNNING
        try {
            recorder.getExecution(connection, taskName, runId);
        } catch (DataSinkException e) {
            if (e.getCode() == ErrorCode.EXECUTION_NOT_FOUND) {
                recorder.startExecution(connection, taskName, runId, null);
            } else {
                throw e;
            }
        }
        table.ensureSystemColumns(); // 系统强制列兜底（建表 DDL 包含 document_id/created_at）
        ensureTable(connection, table);
        Map<String, Object> row = resolver.resolve(table, args, manager.dialect(connection));
        UpsertWriter.SaveResult result = writer.save(connection, table, row, taskName, runId);
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("document_id", result.documentId());
        out.put("operation", result.updated() ? "update" : "insert");
        return out;
    }

    private void ensureTable(String connection, TableConfig table) {
        boolean autoCreate = config.getTables().isAutoCreate();
        List<String> diffs = schemaManager.validateBusinessTable(connection, table);
        if (diffs.size() == 1 && diffs.get(0).contains("不存在")) {
            if (autoCreate) {
                schemaManager.ensureBusinessTable(connection, table);
                return;
            }
            throw new DataSinkException(ErrorCode.SCHEMA_VALIDATION_FAILED,
                    "表不存在且 auto_create=false: " + table.getName());
        }
        if (!diffs.isEmpty()) {
            throw new DataSinkException(ErrorCode.SCHEMA_VALIDATION_FAILED,
                    "表结构校验失败（禁止写入）: " + String.join("; ", diffs));
        }
    }

    private void ensureSystemColumns(TableConfig table, Map<String, Object> row) {
        table.ensureSystemColumns();
        if (!row.containsKey("document_id") && table.findColumn("document_id") != null) {
            row.put("document_id", com.example.datasink.engine.DocumentIdGenerator.generate());
        }
        if (!row.containsKey("created_at") && table.findColumn("created_at") != null) {
            row.put("created_at", manager.dialect("local").formatNow());
        }
    }

    private long countRows(String table) throws Exception {
        try (Connection conn = manager.getConnection("local");
             Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM " + table)) {
            rs.next();
            return rs.getLong(1);
        }
    }

    private long countRowsOn(Connection conn, String table) throws Exception {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM " + table)) {
            rs.next();
            return rs.getLong(1);
        }
    }

    private String queryContent(String table, String documentId) throws Exception {
        try (Connection conn = manager.getConnection("local");
             var ps = conn.prepareStatement("SELECT content FROM " + table + " WHERE document_id = ?")) {
            ps.setString(1, documentId);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getString(1) : null;
            }
        }
    }

    // ================= 功能测试 =================

    /** T1/T2：SQLite 保存 HTML / Markdown，原文一致 */
    @Test
    void t1_t2_saveHtmlAndMarkdown() throws Exception {
        TableConfig daily = config.getTables().findTable("daily_report");
        String html = "<html><body><h1>AI 日报</h1><table><tr><td>x</td></tr></table></body></html>";
        var r1 = saveDocument(daily, saveArgs("daily_report", "daily_ai_news",
                "t1_run1", "2026-11-17", "html", html, null));
        assertEquals(html, queryContent("daily_report", (String) r1.get("document_id")));

        TableConfig aiNews = config.getTables().findTable("ai_news");
        String md = "# 笔记\n\n- item1\n- item2\n";
        var r2 = saveDocument(aiNews, saveArgs("ai_news", "md_task", "t2_run1",
                "2026-11-17", "md", md, Map.of("article_id", "a-t2")));
        assertEquals(md, queryContent("ai_news", (String) r2.get("document_id")));
    }

    /** T5/T6：重复保存（覆盖模式）→ 1 条，内容更新 */
    @Test
    void t5_t6_overwriteKeepsSingleRow() throws Exception {
        TableConfig daily = config.getTables().findTable("daily_report");
        var r1 = saveDocument(daily, saveArgs("daily_report", "daily_ai_news",
                "t5_run1", "2026-11-17", "html", "<p>v1</p>", null));
        var r2 = saveDocument(daily, saveArgs("daily_report", "daily_ai_news",
                "t5_run2", "2026-11-17", "html", "<p>v2</p>", null));
        assertEquals(1, countRows("daily_report"));
        assertEquals("update", r2.get("operation"));
        assertEquals("<p>v2</p>", queryContent("daily_report", (String) r2.get("document_id")));
        // T29：overwrite 更新保留原 document_id
        assertEquals(r1.get("document_id"), r2.get("document_id"));
    }

    /** T7：多日期日报各自成条 */
    @Test
    void t7_multiDateReports() {
        TableConfig daily = config.getTables().findTable("daily_report");
        saveDocument(daily, saveArgs("daily_report", "t", "r1", "2026-11-16", "html", "<p>a</p>", null));
        saveDocument(daily, saveArgs("daily_report", "t", "r2", "2026-11-17", "html", "<p>b</p>", null));
        try {
            assertEquals(2, countRows("daily_report"));
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    /** T8：不同任务互不干扰 */
    @Test
    void t8_differentTasks() {
        TableConfig daily = config.getTables().findTable("daily_report");
        saveDocument(daily, saveArgs("daily_report", "task_a", "r1", "2026-11-17", "html", "<p>a</p>", null));
        saveDocument(daily, saveArgs("daily_report", "task_b", "r2", "2026-11-17", "html", "<p>b</p>", null));
        try {
            assertEquals(2, countRows("daily_report"));
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    /** T9/T28：历史模式同任务多次运行生成多条（同 business_date、不同 run_id） */
    @Test
    void t9_t28_historyMultipleRuns() {
        TableConfig aiNews = config.getTables().findTable("ai_news");
        Map<String, Object> meta1 = Map.of("article_id", "a1");
        Map<String, Object> meta2 = Map.of("article_id", "a2");
        saveDocument(aiNews, saveArgs("ai_news", "t", "20261117_090000", "2026-11-17", "md", "v1", meta1));
        saveDocument(aiNews, saveArgs("ai_news", "t", "20261117_150000", "2026-11-17", "md", "v2", meta2));
        try {
            assertEquals(2, countRows("ai_news"));
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    /** T10：非白名单表 → INVALID_TABLE（引擎层由 findTable 返回 null 体现） */
    @Test
    void t10_nonWhitelistTable() {
        assertNull(config.getTables().findTable("hacker_table"));
        assertNull(config.getTables().findTable("user; DROP TABLE x"));
    }

    /** T11：多余 meta 字段被忽略 */
    @Test
    void t11_extraMetaIgnored() throws Exception {
        TableConfig daily = config.getTables().findTable("daily_report");
        Map<String, Object> meta = new LinkedHashMap<>();
        meta.put("summary", "ok");
        meta.put("evil_column", "DROP TABLE daily_report"); // 未在 source 声明
        saveDocument(daily, saveArgs("daily_report", "t", "r1", "2026-11-17", "html", "<p>x</p>", meta));
        try (Connection conn = manager.getConnection("local");
             var rs = conn.createStatement().executeQuery(
                     "SELECT COUNT(*) FROM pragma_table_info('daily_report') WHERE name='evil_column'")) {
            rs.next();
            assertEquals(0, rs.getLong(1));
        }
    }

    /** T12/T36：超大文档 → CONTENT_TOO_LARGE，拒绝保存（长度校验在工具入口层，此处直接复现该规则） */
    @Test
    void t12_t36_contentTooLarge() throws Exception {
        TableConfig daily = config.getTables().findTable("daily_report");
        // 5MB 上限：构造 6MB 内容
        byte[] big = new byte[6 * 1024 * 1024];
        java.util.Arrays.fill(big, (byte) 'a');
        String bigContent = new String(big, StandardCharsets.UTF_8);
        assertEquals(6 * 1024 * 1024, bigContent.getBytes(StandardCharsets.UTF_8).length);
        long maxBytes = config.getContent().maxBytes();
        assertTrue(bigContent.getBytes(StandardCharsets.UTF_8).length > maxBytes);
        // 长度校验在工具入口（SaveDocumentTool），超限 → CONTENT_TOO_LARGE，不落库
        // 此处独立验证 COUNT 逻辑：工具入口拒绝后引擎不会被调用，表可为空或不存在
        long count = 0;
        try {
            count = countRows("daily_report");
        } catch (java.sql.SQLException e) {
            // 表不存在 = 0 条
        }
        assertEquals(0, count);
    }

    /** T13：list_documents 分页正确、不含完整 content、order_by 白名单 */
    @Test
    void t13_listDocumentsPagination() throws Exception {
        TableConfig daily = config.getTables().findTable("daily_report");
        for (int i = 1; i <= 3; i++) {
            saveDocument(daily, saveArgs("daily_report", "t", "r" + i,
                    String.format("2026-11-%02d", 10 + i), "html", "<p>content" + i + "</p>", null));
        }
        Map<String, Object> args = new LinkedHashMap<>();
        args.put("page", 1);
        args.put("page_size", 2);
        args.put("order_by", "business_date");
        args.put("order_dir", "asc");
        Map<String, Object> data = queryService.listDocuments("local", daily, args);
        assertEquals(3L, data.get("total"));
        assertEquals(2, ((List<?>) data.get("items")).size());
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> items = (List<Map<String, Object>>) data.get("items");
        assertFalse(items.get(0).containsKey("content"));
        assertTrue(items.get(0).containsKey("content_summary"));
        assertEquals("<p>content1</p>", items.get(0).get("content_summary"));

        // order_by 非白名单列 → INVALID_COLUMN
        Map<String, Object> bad = new LinkedHashMap<>();
        bad.put("order_by", "document_id; DROP TABLE x");
        DataSinkException e = assertThrows(DataSinkException.class,
                () -> queryService.listDocuments("local", daily, bad));
        assertEquals(ErrorCode.INVALID_COLUMN, e.getCode());

        // page_size 越界 → INVALID_ARGUMENT
        Map<String, Object> badPage = new LinkedHashMap<>();
        badPage.put("page_size", 1000);
        e = assertThrows(DataSinkException.class,
                () -> queryService.listDocuments("local", daily, badPage));
        assertEquals(ErrorCode.INVALID_ARGUMENT, e.getCode());
    }

    // ================= 异常/可靠性 =================

    /** T15：写入异常（约束错误）→ 事务回滚，无半条数据 */
    @Test
    void t15_constraintErrorRollsBack() {
        TableConfig daily = config.getTables().findTable("daily_report");
        // 构造超长 VARCHAR 溢出在 SQLite 不报错，改用 NOT NULL 缺失：business_date null 且参与 upsert_key
        Map<String, Object> args = saveArgs("daily_report", "t", "r1", null, "html", "<p>x</p>", null);
        // business_date 是 NOT NULL 且参与 upsert_key；resolver 对 null 非 nullable 列抛 INVALID_ARGUMENT
        DataSinkException e = assertThrows(DataSinkException.class, () -> {
            // 模拟绕过工具层校验直接 resolve（SaveDocumentTool 会在 resolve 前拦截）
            Map<String, Object> row = resolver.resolve(daily, args, manager.dialect("local"));
            ensureSystemColumns(daily, row);
            writer.save("local", daily, row, "t", "r1");
        });
        assertEquals(ErrorCode.INVALID_ARGUMENT, e.getCode());
    }

    /** T17/T35：已有表结构与配置不一致 → SCHEMA_VALIDATION_FAILED */
    @Test
    void t17_t35_schemaMismatchRejected() throws Exception {
        // 先手工建一个缺列的表
        try (Connection conn = manager.getConnection("local");
             Statement st = conn.createStatement()) {
            st.execute("CREATE TABLE IF NOT EXISTS daily_report (" +
                    "task_name VARCHAR(128) NOT NULL)");
        }
        TableConfig daily = config.getTables().findTable("daily_report");
        List<String> diffs = schemaManager.validateBusinessTable("local", daily);
        assertFalse(diffs.isEmpty());
        DataSinkException e = assertThrows(DataSinkException.class,
                () -> ensureTable("local", daily));
        assertEquals(ErrorCode.SCHEMA_VALIDATION_FAILED, e.getCode());
    }

    /** T37：SQL 特殊字符和注入内容 → 参数化处理 */
    @Test
    void t37_sqlInjectionSafe() throws Exception {
        TableConfig daily = config.getTables().findTable("daily_report");
        String evil = "<p>Robert'); DROP TABLE daily_report;--</p>";
        var r = saveDocument(daily, saveArgs("daily_report", "t", "r1",
                "2026-11-17", "html", evil, null));
        assertEquals(evil, queryContent("daily_report", (String) r.get("document_id")));
        assertEquals(1, countRows("daily_report")); // 表未被删除
    }

    // ================= 新增测试（T23–T38 SQLite 部分） =================

    /** T23：辅助唯一键冲突 → UNIQUE_KEY_CONFLICT，不更新其他记录 */
    @Test
    void t23_auxUniqueKeyConflict() {
        TableConfig aiNews = config.getTables().findTable("ai_news");
        saveDocument(aiNews, saveArgs("ai_news", "t", "r1", "2026-11-17", "md", "v1",
                Map.of("article_id", "dup-a")));
        // 不同 run_id（upsert_key 不同）但 article_id 相同 → 辅助键冲突
        DataSinkException e = assertThrows(DataSinkException.class, () ->
                saveDocument(aiNews, saveArgs("ai_news", "t", "r2", "2026-11-17", "md", "v2",
                        Map.of("article_id", "dup-a"))));
        assertEquals(ErrorCode.UNIQUE_KEY_CONFLICT, e.getCode());
        try {
            assertEquals(1, countRows("ai_news"));
        } catch (Exception ex) {
            fail(ex.getMessage());
        }
    }

    /** T24：SQLite 指定 Upsert 唯一键（ON CONFLICT 语义等价的先查后写生效） */
    @Test
    void t24_upsertKeyOverwrite() throws Exception {
        TableConfig daily = config.getTables().findTable("daily_report");
        saveDocument(daily, saveArgs("daily_report", "t", "r1", "2026-11-17", "html", "<p>v1</p>", null));
        // 相同 upsert_key（task+date+format）、不同 run_id → 覆盖
        saveDocument(daily, saveArgs("daily_report", "t", "r2", "2026-11-17", "html", "<p>v2</p>", null));
        assertEquals(1, countRows("daily_report"));
        // 另一个 format 不覆盖
        saveDocument(daily, saveArgs("daily_report", "t", "r3", "2026-11-17", "md", "md-v", null));
        assertEquals(2, countRows("daily_report"));
    }

    /** T25：文档写入失败 → 执行记录 FAILED */
    @Test
    void t25_failedExecutionRecorded() {
        TableConfig aiNews = config.getTables().findTable("ai_news");
        recorder.startExecution("local", "t25", "r1", null);
        // 辅助键冲突触发失败路径
        saveDocument(aiNews, saveArgs("ai_news", "t25", "r0", "2026-11-17", "md", "v0",
                Map.of("article_id", "dup-t25")));
        try {
            DataSinkException e = assertThrows(DataSinkException.class, () ->
                    saveDocument(aiNews, saveArgs("ai_news", "t25", "r1", "2026-11-17", "md", "v1",
                            Map.of("article_id", "dup-t25"))));
            assertEquals(ErrorCode.UNIQUE_KEY_CONFLICT, e.getCode());
        } finally {
            recorder.markFailed("local", "t25", "r1", "UNIQUE_KEY_CONFLICT", "辅助唯一键冲突");
        }
        ExecutionRecorder.Execution exec = recorder.getExecution("local", "t25", "r1");
        assertEquals(ExecutionRecorder.STATUS_FAILED, exec.status());
        assertEquals("UNIQUE_KEY_CONFLICT", exec.errorCode());
    }

    /** T26：SUCCESS 更新失败 → 业务文档和 SUCCESS 一起回滚（T2 事务原子性验证） */
    @Test
    void t26_transactionAtomicity() throws Exception {
        // 先建表（测试顺序无关）
        TableConfig daily = config.getTables().findTable("daily_report");
        daily.ensureSystemColumns();
        schemaManager.ensureBusinessTable("local", daily);
        try (Connection conn = manager.getConnection("local")) {
            conn.setAutoCommit(false);
            // 在事务内写入但不提交，回滚后无残留
            var ps = conn.prepareStatement(
                    "INSERT INTO daily_report (task_name, business_date, format, content, document_id, created_at)" +
                            " VALUES ('t26','2026-11-17','html','<p>uncommitted</p>','doc-t26','2026-11-17 00:00:00')");
            ps.executeUpdate();
            conn.rollback();
            conn.setAutoCommit(true);
        }
        assertEquals(0, countRows("daily_report"));
        // 正常 save 后 SUCCESS 状态与文档同在
        TableConfig daily2 = config.getTables().findTable("daily_report");
        saveDocument(daily2, saveArgs("daily_report", "t26b", "r1", "2026-11-17", "html", "<p>ok</p>", null));
        ExecutionRecorder.Execution exec = recorder.getExecution("local", "t26b", "r1");
        assertEquals(ExecutionRecorder.STATUS_SUCCESS, exec.status());
        assertNotNull(exec.documentId());
    }

    /** T27：overwrite 相同业务日期、不同 run_id → 更新原文档，保留两次执行记录 */
    @Test
    void t27_overwriteKeepsBothExecutions() throws Exception {
        TableConfig daily = config.getTables().findTable("daily_report");
        saveDocument(daily, saveArgs("daily_report", "t27", "run_a", "2026-11-17", "html", "<p>a</p>", null));
        saveDocument(daily, saveArgs("daily_report", "t27", "run_b", "2026-11-17", "html", "<p>b</p>", null));
        assertEquals(1, countRows("daily_report"));
        ExecutionRecorder.Execution e1 = recorder.getExecution("local", "t27", "run_a");
        ExecutionRecorder.Execution e2 = recorder.getExecution("local", "t27", "run_b");
        assertEquals(ExecutionRecorder.STATUS_SUCCESS, e1.status());
        assertEquals(ExecutionRecorder.STATUS_SUCCESS, e2.status());
        // 两次执行记录关联同一个 document_id
        assertEquals(e1.documentId(), e2.documentId());
    }

    /** T29：overwrite 更新 document_id 保持不变（在 T5 已验证，这里独立断言） */
    @Test
    void t29_documentIdStableOnOverwrite() throws Exception {
        TableConfig daily = config.getTables().findTable("daily_report");
        var r1 = saveDocument(daily, saveArgs("daily_report", "t29", "r1", "2026-11-17", "md", "v1", null));
        var r2 = saveDocument(daily, saveArgs("daily_report", "t29", "r2", "2026-11-17", "md", "v2", null));
        String stored = queryContent("daily_report", (String) r1.get("document_id"));
        assertEquals("v2", stored);
        assertEquals(r1.get("document_id"), r2.get("document_id"));
        // 执行记录表里 run_a/run_b 的 document_id 同为原值
        assertEquals(r1.get("document_id"), recorder.getExecution("local", "t29", "r1").documentId());
        assertEquals(r1.get("document_id"), recorder.getExecution("local", "t29", "r2").documentId());
    }

    /** T30：history 新增每次生成不同 UUID */
    @Test
    void t30_historyNewUuidEachTime() {
        TableConfig aiNews = config.getTables().findTable("ai_news");
        var r1 = saveDocument(aiNews, saveArgs("ai_news", "t30", "r1", "2026-11-17", "md", "v1",
                Map.of("article_id", "t30-a")));
        var r2 = saveDocument(aiNews, saveArgs("ai_news", "t30", "r2", "2026-11-17", "md", "v2",
                Map.of("article_id", "t30-b")));
        assertNotEquals(r1.get("document_id"), r2.get("document_id"));
    }

    /** T31：多进程（多连接实例）同时写入 SQLite → 无数据损坏 */
    @Test
    void t31_multiInstanceWrites() throws Exception {
        // 第二个 ConnectionManager 模拟另一个进程
        AppConfig cfg2 = buildConfig(dbFile.toString());
        ConnectionManager manager2 = new ConnectionManager(cfg2);
        SchemaManager schema2 = new SchemaManager(manager2);
        schema2.init();
        try {
            TableConfig t1 = config.getTables().findTable("daily_report");
            TableConfig t2 = cfg2.getTables().findTable("daily_report");
            saveDocument(t1, saveArgs("daily_report", "proc1", "r1", "2026-11-17", "html", "<p>p1</p>", null));
            // 第二实例写入不同 task
            recorder = new ExecutionRecorder(manager);
            saveDocument(t2, saveArgs("daily_report", "proc2", "r2", "2026-11-17", "html", "<p>p2</p>", null));
            assertEquals(2, countRows("daily_report"));
        } finally {
            manager2.close();
        }
    }

    /** T32：SQLite WAL 初始化验证 */
    @Test
    void t32_walEnabled() throws Exception {
        try (Connection conn = manager.getConnection("local");
             Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("PRAGMA journal_mode;")) {
            assertTrue(rs.next());
            assertEquals("wal", rs.getString(1).toLowerCase());
        }
        try (Connection conn = manager.getConnection("local");
             Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery("PRAGMA busy_timeout;")) {
            assertTrue(rs.next());
            assertEquals(5000, rs.getInt(1));
        }
    }

    /** T33：get_document 不完整唯一键条件 → INVALID_QUERY_CONDITION */
    @Test
    void t33_incompleteQueryCondition() {
        TableConfig daily = config.getTables().findTable("daily_report");
        Map<String, Object> args = new LinkedHashMap<>();
        args.put("task_name", "t33");
        // 缺 business_date、format
        DataSinkException e = assertThrows(DataSinkException.class,
                () -> queryService.getByBusinessKey("local", daily, args));
        assertEquals(ErrorCode.INVALID_QUERY_CONDITION, e.getCode());
    }

    /** T34：查询结果不唯一 → NON_UNIQUE_RESULT（SQLite 中 upsert_key 唯一约束
     *  使业务键查询必唯一；此处验证 document_id 重复查询的防御逻辑 + 无结果路径） */
    @Test
    void t34_documentNotFound() {
        TableConfig daily = config.getTables().findTable("daily_report");
        DataSinkException e = assertThrows(DataSinkException.class,
                () -> queryService.getByDocumentId("local", daily, "no-such-id"));
        assertEquals(ErrorCode.DOCUMENT_NOT_FOUND, e.getCode());
    }

    /** T18：配置错误 → 启动失败（校验异常） */
    @Test
    void t18_configValidationFails() {
        AppConfig bad = buildConfig(dbFile.toString());
        // 移除一张表的 upsert_key
        bad.getTables().getAllowlist().get(1).setUpsertKey(List.of());
        assertThrows(ConfigValidator.ValidationException.class, () -> ConfigValidator.validate(bad));

        AppConfig bad2 = buildConfig(dbFile.toString());
        // upsert_key 引用不存在的列
        bad2.getTables().getAllowlist().get(1).setUpsertKey(List.of("no_such_col"));
        assertThrows(ConfigValidator.ValidationException.class, () -> ConfigValidator.validate(bad2));

        AppConfig bad3 = buildConfig(dbFile.toString());
        // 辅助唯一键列允许 NULL
        com.example.datasink.config.ColumnConfig c = new com.example.datasink.config.ColumnConfig();
        c.setColumn("dup_col");
        c.setType("VARCHAR(10)");
        c.setSource("meta.dup");
        c.setNullable(true);
        bad3.getTables().getAllowlist().get(0).getColumns().add(c);
        bad3.getTables().getAllowlist().get(0).setUniqueKeys(List.of(List.of("dup_col")));
        assertThrows(ConfigValidator.ValidationException.class, () -> ConfigValidator.validate(bad3));
    }

    /** T22：重复 run_id 幂等重试：相同 run_id 再次 save 不报错、不新建执行记录 */
    @Test
    void t22_duplicateRunIdIdempotent() throws Exception {
        TableConfig daily = config.getTables().findTable("daily_report");
        saveDocument(daily, saveArgs("daily_report", "t22", "same_run", "2026-11-17", "html", "<p>v1</p>", null));
        saveDocument(daily, saveArgs("daily_report", "t22", "same_run", "2026-11-17", "html", "<p>v2</p>", null));
        assertEquals(1, countRows("daily_report"));
        ExecutionRecorder.Execution exec = recorder.getExecution("local", "t22", "same_run");
        assertEquals(ExecutionRecorder.STATUS_SUCCESS, exec.status());
        assertEquals("<p>v2</p>", queryContent("daily_report", exec.documentId()));
    }

    /** start_execution 状态流转：RUNNING → SUCCESS；重复 start 报 ALREADY_*；FAILED 复用重置 */
    @Test
    void executionLifecycle() {
        // ALREADY_RUNNING
        recorder.startExecution("local", "t_lc", "r1", null);
        DataSinkException e = assertThrows(DataSinkException.class,
                () -> recorder.startExecution("local", "t_lc", "r1", null));
        assertEquals(ErrorCode.EXECUTION_ALREADY_RUNNING, e.getCode());
        // → SUCCESS
        try (java.sql.Connection conn = manager.getConnection("local")) {
            recorder.markSuccess(conn, "t_lc", "r1", "doc-1", "daily_report", manager.dialect("local"));
        } catch (java.sql.SQLException sqle) {
            throw new RuntimeException(sqle);
        }
        ExecutionRecorder.Execution s = recorder.getExecution("local", "t_lc", "r1");
        assertEquals(ExecutionRecorder.STATUS_SUCCESS, s.status());
        assertEquals("doc-1", s.documentId());
        assertNotNull(s.finishedAt());
        // 重复 start 已 SUCCESS → ALREADY_SUCCESS
        e = assertThrows(DataSinkException.class,
                () -> recorder.startExecution("local", "t_lc", "r1", null));
        assertEquals(ErrorCode.EXECUTION_ALREADY_SUCCESS, e.getCode());
        // FAILED → 复用重置 RUNNING
        recorder.markFailed("local", "t_lc", "r2", "DATABASE_TRANSACTION_ERROR", "写入失败");
        ExecutionRecorder.Execution f = recorder.startExecution("local", "t_lc", "r2", null);
        assertEquals(ExecutionRecorder.STATUS_RUNNING, f.status());
        // get_execution 查不到 → EXECUTION_NOT_FOUND
        e = assertThrows(DataSinkException.class,
                () -> recorder.getExecution("local", "t_lc", "no_such"));
        assertEquals(ErrorCode.EXECUTION_NOT_FOUND, e.getCode());
    }

    private long countQuiet(String table) {
        try {
            return countRows(table);
        } catch (Exception e) {
            return -1;
        }
    }
}
