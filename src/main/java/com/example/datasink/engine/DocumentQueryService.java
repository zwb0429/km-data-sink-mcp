package com.example.datasink.engine;

import com.example.datasink.config.AppConfig;
import com.example.datasink.config.ColumnConfig;
import com.example.datasink.config.TableConfig;
import com.example.datasink.db.ConnectionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * 文档查询服务：get_document（按 document_id 或完整 upsert_key）+ list_documents（分页，不含全文）。
 * 查询规则（P1-⑥ / P1-⑪）：
 * 1. 优先 document_id；2. 业务键查询必须提供完整 upsert_key 组合；
 * 3. 不完整 → INVALID_QUERY_CONDITION；4. 不唯一 → NON_UNIQUE_RESULT；5. 无结果 → DOCUMENT_NOT_FOUND。
 */
public class DocumentQueryService {

    private static final Logger log = LoggerFactory.getLogger(DocumentQueryService.class);

    private final ConnectionManager manager;
    private final AppConfig config;

    public DocumentQueryService(ConnectionManager manager, AppConfig config) {
        this.manager = manager;
        this.config = config;
    }

    /** 按 document_id 查询完整文档 */
    public Map<String, Object> getByDocumentId(String connection, TableConfig table, String documentId) {
        String sql = "SELECT * FROM " + table.getName() + " WHERE document_id = ?";
        try (Connection conn = manager.getConnection(connection);
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, documentId);
            try (ResultSet rs = ps.executeQuery()) {
                List<Map<String, Object>> rows = mapRows(table, rs, true);
                if (rows.isEmpty()) {
                    throw new DataSinkException(ErrorCode.DOCUMENT_NOT_FOUND,
                            "文档不存在: document_id=" + documentId);
                }
                if (rows.size() > 1) {
                    throw new DataSinkException(ErrorCode.NON_UNIQUE_RESULT,
                            "查询结果不唯一: document_id=" + documentId + " 命中 " + rows.size() + " 条");
                }
                return rows.get(0);
            }
        } catch (SQLException e) {
            // 表不存在视为文档不存在（get_document 场景）
            if (isTableMissing(e, connection)) {
                throw new DataSinkException(ErrorCode.DOCUMENT_NOT_FOUND,
                        "文档不存在: document_id=" + documentId + "（表 " + table.getName() + " 不存在）");
            }
            throw translate(e, connection);
        }
    }

    /**
     * 按业务唯一键查询完整文档：必须提供完整 upsert_key 字段组合。
     */
    public Map<String, Object> getByBusinessKey(String connection, TableConfig table,
                                                Map<String, Object> args) {
        List<String> key = table.getUpsertKey();
        // 条件完整性校验：upsert_key 全部字段必须提供
        List<String> missing = key.stream()
                .filter(c -> args.get(c) == null || String.valueOf(args.get(c)).isBlank())
                .collect(Collectors.toList());
        if (!missing.isEmpty()) {
            throw new DataSinkException(ErrorCode.INVALID_QUERY_CONDITION,
                    "查询条件不完整：缺少 upsert_key 字段 " + missing
                            + "（需完整提供: " + key + "），或改用 document_id 查询");
        }
        String where = key.stream().map(c -> c + " = ?").collect(Collectors.joining(" AND "));
        String sql = "SELECT * FROM " + table.getName() + " WHERE " + where;
        try (Connection conn = manager.getConnection(connection);
             PreparedStatement ps = conn.prepareStatement(sql)) {
            int idx = 1;
            for (String c : key) {
                ps.setObject(idx++, args.get(c));
            }
            try (ResultSet rs = ps.executeQuery()) {
                List<Map<String, Object>> rows = mapRows(table, rs, true);
                if (rows.isEmpty()) {
                    throw new DataSinkException(ErrorCode.DOCUMENT_NOT_FOUND,
                            "文档不存在（按业务键 " + key + " 查询）");
                }
                if (rows.size() > 1) {
                    throw new DataSinkException(ErrorCode.NON_UNIQUE_RESULT,
                            "查询结果不唯一: 命中 " + rows.size() + " 条，请改用 document_id 查询");
                }
                return rows.get(0);
            }
        } catch (SQLException e) {
            throw translate(e, connection);
        }
    }

    /**
     * 分页查询文档列表（不含 content 全文，返回摘要 + 元数据含 document_id）。
     */
    public Map<String, Object> listDocuments(String connection, TableConfig table,
                                             Map<String, Object> args) {
        int page = intArg(args, "page", config.getQuery().getDefaultPageSize() > 0 ? 1 : 1);
        if (args.get("page") == null) {
            page = 1;
        }
        int pageSize = args.get("page_size") == null
                ? config.getQuery().getDefaultPageSize()
                : intArg(args, "page_size", config.getQuery().getDefaultPageSize());
        if (page < 1) {
            throw new DataSinkException(ErrorCode.INVALID_ARGUMENT, "page 必须 >= 1");
        }
        if (pageSize < 1 || pageSize > config.getQuery().getMaxPageSize()) {
            throw new DataSinkException(ErrorCode.INVALID_ARGUMENT,
                    "page_size 必须 1-" + config.getQuery().getMaxPageSize());
        }
        String orderBy = args.get("order_by") == null || String.valueOf(args.get("order_by")).isBlank()
                ? "created_at" : String.valueOf(args.get("order_by"));
        // 排序字段白名单校验（仅允许表 columns 声明的列）
        if (table.findColumn(orderBy) == null) {
            throw new DataSinkException(ErrorCode.INVALID_COLUMN,
                    "order_by 非法（不在表 " + table.getName() + " columns 白名单中）: " + orderBy);
        }
        String orderDir = args.get("order_dir") == null ? "desc" : String.valueOf(args.get("order_dir")).toLowerCase();
        if (!orderDir.equals("asc") && !orderDir.equals("desc")) {
            throw new DataSinkException(ErrorCode.INVALID_ARGUMENT, "order_dir 仅允许 asc/desc");
        }

        // WHERE 条件（全部走参数化 + 白名单列）
        StringBuilder where = new StringBuilder(" WHERE 1=1");
        List<Object> params = new ArrayList<>();
        if (args.get("task_name") != null && !String.valueOf(args.get("task_name")).isBlank()) {
            where.append(" AND task_name = ?");
            params.add(args.get("task_name"));
        }
        if (args.get("format") != null && !String.valueOf(args.get("format")).isBlank()) {
            where.append(" AND format = ?");
            params.add(args.get("format"));
        }
        if (table.findColumn("business_date") != null) {
            if (args.get("business_date_from") != null && !String.valueOf(args.get("business_date_from")).isBlank()) {
                where.append(" AND business_date >= ?");
                params.add(args.get("business_date_from"));
            }
            if (args.get("business_date_to") != null && !String.valueOf(args.get("business_date_to")).isBlank()) {
                where.append(" AND business_date <= ?");
                params.add(args.get("business_date_to"));
            }
        }

        // 列表列：除 content 外全部列 + content 摘要（前 200 字符）
        List<String> listCols = table.getColumns().stream()
                .map(ColumnConfig::getColumn)
                .filter(c -> !"content".equalsIgnoreCase(c))
                .collect(Collectors.toList());

        String sql = "SELECT " + String.join(", ", listCols)
                + ", SUBSTR(content, 1, 200) AS content_summary, LENGTH(content) AS content_length"
                + " FROM " + table.getName() + where
                + " ORDER BY " + orderBy + " " + orderDir
                + " LIMIT ? OFFSET ?";
        try (Connection conn = manager.getConnection(connection);
             PreparedStatement ps = conn.prepareStatement(sql)) {
            int idx = 1;
            for (Object p : params) {
                ps.setObject(idx++, p);
            }
            ps.setInt(idx++, pageSize);
            ps.setInt(idx, (page - 1) * pageSize);
            try (ResultSet rs = ps.executeQuery()) {
                List<Map<String, Object>> items = new ArrayList<>();
                while (rs.next()) {
                    Map<String, Object> item = new LinkedHashMap<>();
                    for (String c : listCols) {
                        item.put(c, rs.getObject(c));
                    }
                    String summary = rs.getString("content_summary");
                    item.put("content_summary", summary);
                    item.put("content_length", rs.getLong("content_length"));
                    items.add(item);
                }

                // 总数（同条件）
                String countSql = "SELECT COUNT(*) FROM " + table.getName() + where;
                long total;
                try (PreparedStatement cps = conn.prepareStatement(countSql)) {
                    int cidx = 1;
                    for (Object p : params) {
                        cps.setObject(cidx++, p);
                    }
                    try (ResultSet crs = cps.executeQuery()) {
                        total = crs.next() ? crs.getLong(1) : 0;
                    }
                }
                Map<String, Object> data = new LinkedHashMap<>();
                data.put("items", items);
                data.put("page", page);
                data.put("page_size", pageSize);
                data.put("total", total);
                data.put("total_pages", (total + pageSize - 1) / pageSize);
                return data;
            }
        } catch (SQLException e) {
            throw translate(e, connection);
        }
    }

    private List<Map<String, Object>> mapRows(TableConfig table, ResultSet rs, boolean withContent)
            throws SQLException {
        List<Map<String, Object>> rows = new ArrayList<>();
        Set<String> configured = table.getColumns().stream()
                .map(ColumnConfig::getColumn)
                .map(String::toLowerCase)
                .collect(Collectors.toSet());
        while (rs.next()) {
            Map<String, Object> row = new LinkedHashMap<>();
            var md = rs.getMetaData();
            for (int i = 1; i <= md.getColumnCount(); i++) {
                String col = md.getColumnLabel(i);
                // 只返回配置中声明的列（防意外列泄露）
                if (!configured.contains(col.toLowerCase())) {
                    continue;
                }
                row.put(col, rs.getObject(i));
            }
            rows.add(row);
        }
        return rows;
    }

    private int intArg(Map<String, Object> args, String key, int def) {
        Object v = args.get(key);
        if (v == null) {
            return def;
        }
        if (v instanceof Number n) {
            return n.intValue();
        }
        try {
            return Integer.parseInt(String.valueOf(v).trim());
        } catch (NumberFormatException e) {
            throw new DataSinkException(ErrorCode.INVALID_ARGUMENT, key + " 必须为整数");
        }
    }

    private DataSinkException translate(SQLException e, String connection) {
        DialectHolder h = new DialectHolder(manager.dialect(connection));
        if (h.d.isLockTimeout(e)) {
            return new DataSinkException(ErrorCode.DATABASE_LOCK_TIMEOUT, "查询失败：数据库锁/超时", e);
        }
        return new DataSinkException(ErrorCode.DATABASE_TRANSACTION_ERROR, "查询失败：数据库错误", e);
    }

    /** 判断异常是否为表不存在（SQLite "no such table" / MySQL 1146） */
    private boolean isTableMissing(SQLException e, String connection) {
        SQLException cur = e;
        while (cur != null) {
            String msg = cur.getMessage() == null ? "" : cur.getMessage().toLowerCase();
            if (msg.contains("no such table") || cur.getErrorCode() == 1146) {
                return true;
            }
            cur = cur.getNextException();
        }
        return false;
    }

    private record DialectHolder(com.example.datasink.db.Dialect d) {
    }
}
