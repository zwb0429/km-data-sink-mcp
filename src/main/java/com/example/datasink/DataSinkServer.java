package com.example.datasink;

import com.example.datasink.config.AppConfig;
import com.example.datasink.config.ConfigLoader;
import com.example.datasink.config.ConfigValidator;
import com.example.datasink.db.ConnectionManager;
import com.example.datasink.engine.DocumentQueryService;
import com.example.datasink.engine.ExecutionRecorder;
import com.example.datasink.engine.SchemaManager;
import com.example.datasink.engine.UpsertWriter;
import com.example.datasink.tools.GetDocumentTool;
import com.example.datasink.tools.GetExecutionTool;
import com.example.datasink.tools.ListConnectionsTool;
import com.example.datasink.tools.ListDocumentsTool;
import com.example.datasink.tools.SaveDocumentTool;
import com.example.datasink.tools.StartExecutionTool;
import com.example.datasink.tools.TestConnectionTool;
import io.modelcontextprotocol.json.McpJsonDefaults;
import io.modelcontextprotocol.server.McpServer;
import io.modelcontextprotocol.server.McpServerFeatures.SyncToolSpecification;
import io.modelcontextprotocol.server.transport.StdioServerTransportProvider;
import io.modelcontextprotocol.spec.McpSchema.ServerCapabilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * 入口：组装 server + 工具 + shutdown hook。
 * stdio 纪律：日志只写 System.err / 文件（logback 配置），绝不 System.out。
 */
public final class DataSinkServer {

    private static final Logger log = LoggerFactory.getLogger(DataSinkServer.class);

    public static void main(String[] args) {
        String configPath = parseArg(args, "--config", "config.yaml");
        AppConfig config;
        try {
            config = new ConfigLoader().load(Path.of(configPath));
        } catch (Exception e) {
            log.error("配置加载失败: {} - {}", configPath, e.getMessage());
            System.exit(1);
            return;
        }
        try {
            ConfigValidator.validate(config); // 启动校验，失败即退出
        } catch (ConfigValidator.ValidationException e) {
            log.error("{}", e.getMessage());
            System.exit(1);
            return;
        }

        ConnectionManager manager = new ConnectionManager(config);
        SchemaManager schemaManager = new SchemaManager(manager);
        ExecutionRecorder recorder = new ExecutionRecorder(manager,
                config.getDb().getMaxRetry(), config.getDb().getRetryBackoffMs());
        UpsertWriter writer = new UpsertWriter(manager, recorder,
                config.getDb().getMaxRetry(), config.getDb().getRetryBackoffMs());
        DocumentQueryService queryService = new DocumentQueryService(manager, config);

        List<SyncToolSpecification> tools = new ArrayList<>();
        tools.add(new TestConnectionTool(manager).specification());
        tools.add(new ListConnectionsTool(manager).specification());
        tools.add(new StartExecutionTool(manager, recorder).specification());
        tools.add(new SaveDocumentTool(manager, schemaManager, writer, recorder, config).specification());
        tools.add(new GetDocumentTool(manager, queryService, config).specification());
        tools.add(new ListDocumentsTool(manager, queryService, config).specification());
        tools.add(new GetExecutionTool(manager, recorder).specification());

        // 先构建 MCP server：stdio 立即就绪，initialize 握手不被 DB 初始化阻塞。
        // （MySQL 连接池冷启动可达 15s+，若先初始化 DB 再起 server，客户端会因握手超时报 Connection closed）
        var server = McpServer.sync(
                        new StdioServerTransportProvider(McpJsonDefaults.getMapper()))
                .serverInfo("km-data-sink-mcp", "1.0.2")
                .capabilities(ServerCapabilities.builder().tools(true).build())
                .tools(tools.toArray(new SyncToolSpecification[0]))
                .build();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("服务关闭：释放连接池资源");
            manager.close();
        }));

        // server 就绪后再初始化连接池与执行记录表；失败不退出，保留 server 供 test_connection 排查
        try {
            schemaManager.init(); // 建执行记录表 + 执行记录表结构校验（每连接）
        } catch (Exception e) {
            log.error("连接初始化失败（server 保持运行，可通过 test_connection 排查）: {}", e.getMessage());
            // 输出根因链（Access denied / Unknown database 等真实原因在 cause 中）
            Throwable cur = e.getCause();
            while (cur != null) {
                log.error("  根因: {}", cur.getMessage());
                cur = cur.getCause();
            }
        }
        // SDK 2.x：build() 后 transport 已开始处理 stdio 请求，主线程阻塞等待即可
        try {
            Thread.currentThread().join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            manager.close();
        }
    }

    private static String parseArg(String[] args, String flag, String def) {
        for (int i = 0; i < args.length - 1; i++) {
            if (flag.equals(args[i])) {
                return args[i + 1];
            }
        }
        return def;
    }

    private DataSinkServer() {
    }
}
