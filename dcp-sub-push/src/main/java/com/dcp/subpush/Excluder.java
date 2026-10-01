package com.dcp.subpush;

import com.fasterxml.jackson.databind.JsonNode;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * tag 契约 exclude 剔除器: 事件实体是否持有 exclude 标签(查元库 tag_fact_view 当前
 * 标签态; 与切流前 DataSubService.tagChanges 的 exclude 语义一致 —— 剔除"当前持有
 * 该标签"的实体)。
 *
 * <p>事件流只能看到当前事件的标签, 实体是否持有 exclude 标签必须回查标签投影;
 * 带 60s TTL 小缓存(单线程 process 串行访问, 无需并发结构), 事件量大时避免逐条点查。
 */
public class Excluder {

    /** 无 exclude 规格: 恒不剔除。 */
    public static Excluder none() {
        return new Excluder(null, null, null, null);
    }

    /** exclude 规格清单(与 DataSubService appendTagCond 同构: id 优先, 兼容旧行按路径)。 */
    private final java.util.List<String[]> specs;   // {tagDictId, path}
    private Connection conn;
    /** entity → 是否剔除(60s TTL; 条目超限整体清空防膨胀)。 */
    private final Map<String, Object[]> cache = new LinkedHashMap<>();
    private static final long TTL_MS = 60_000;
    private static final int MAX_CACHE = 100_000;

    /**
     * @param view tag 视图快照(读 exclude[]: 每项 {tagDictId?, path?})
     * @param jdbcUrl 元库连接(有 exclude 规格时必填)
     */
    public Excluder(JsonNode view, String jdbcUrl, String user, String password) {
        java.util.List<String[]> s = new java.util.ArrayList<>();
        if (view != null) {
            for (JsonNode ex : view.path("exclude")) {
                String tid = ex.hasNonNull("tagDictId") ? ex.path("tagDictId").asText() : null;
                String path = ex.isTextual() ? ex.asText() : ex.path("path").asText("");
                if ((tid == null || tid.isBlank()) && path.isBlank()) {
                    continue;
                }
                s.add(new String[]{tid, path});
            }
        }
        this.specs = s.isEmpty() ? null : s;
        if (this.specs != null && (jdbcUrl == null || jdbcUrl.isBlank())) {
            throw new IllegalStateException("[sub-push] tag 契约含 exclude 规格但 params 缺元库连接(jdbcUrl)");
        }
        if (this.specs != null) {
            try {
                Class.forName("com.mysql.cj.jdbc.Driver");
                this.conn = DriverManager.getConnection(jdbcUrl, user, password);
            } catch (Exception e) {
                throw new IllegalStateException("[sub-push] exclude 元库连接失败: " + e.getMessage(), e);
            }
        }
    }

    /** 实体当前是否持有 exclude 标签 → 剔除。 */
    public boolean excluded(String entity) {
        if (specs == null || entity == null) {
            return false;
        }
        long now = System.currentTimeMillis();
        Object[] hit = cache.get(entity);
        if (hit != null && (long) hit[1] > now) {
            return (boolean) hit[0];
        }
        if (cache.size() > MAX_CACHE) {
            cache.clear();
        }
        boolean ex = query(entity);
        cache.put(entity, new Object[]{ex, now + TTL_MS});
        return ex;
    }

    private boolean query(String entity) {
        try {
            StringBuilder sql = new StringBuilder("SELECT 1 FROM tag_fact_view WHERE entity=? AND (");
            java.util.List<String> conds = new java.util.ArrayList<>();
            for (String[] spec : specs) {
                if (spec[0] != null && !spec[0].isBlank()) {
                    conds.add("(tag_dict_id=? OR (tag_dict_id IS NULL AND tag=?))");
                } else {
                    conds.add("tag=?");
                }
            }
            sql.append(String.join(" OR ", conds)).append(") LIMIT 1");
            try (PreparedStatement ps = conn.prepareStatement(sql.toString())) {
                int idx = 1;
                ps.setString(idx++, entity);
                for (String[] spec : specs) {
                    if (spec[0] != null && !spec[0].isBlank()) {
                        ps.setString(idx++, spec[0]);
                    }
                    ps.setString(idx++, spec[1]);
                }
                try (ResultSet rs = ps.executeQuery()) {
                    return rs.next();
                }
            }
        } catch (Exception e) {
            throw new IllegalStateException("[sub-push] exclude 查询失败 entity=" + entity + ": " + e.getMessage(), e);
        }
    }

    public void close() {
        if (conn != null) {
            try {
                conn.close();
            } catch (Exception ignore) {
            }
            conn = null;
        }
    }
}
