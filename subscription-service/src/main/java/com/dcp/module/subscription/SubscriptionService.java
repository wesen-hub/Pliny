package com.dcp.module.subscription;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Service;

import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.*;

/**
 * 订阅中心: 订阅 = 标签条件(include tags + op + exclude + 密级上限)。
 * 命中资产 = TagService.subset 结果(与资产目录子集同一套标签体系)。
 */
@Slf4j
@Service
public class SubscriptionService {

    private final JdbcTemplate metaJdbc;
    private final ObjectMapper om = new ObjectMapper();

    public SubscriptionService(JdbcTemplate jdbcTemplate) {
        this.metaJdbc = jdbcTemplate;
    }

    public List<Map<String, Object>> list() {
        List<Map<String, Object>> out = new ArrayList<>();
        metaJdbc.query("SELECT id, name, expr, mode, target, status, create_by, create_time, update_time "
                + "FROM asset_subscription ORDER BY id DESC", rs -> {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("id", rs.getLong("id"));
            m.put("name", rs.getString("name"));
            m.put("expr", parseExpr(rs.getString("expr")));
            m.put("mode", rs.getString("mode"));
            m.put("target", rs.getString("target"));
            m.put("status", rs.getString("status"));
            m.put("create_by", rs.getString("create_by"));
            m.put("create_time", rs.getTimestamp("create_time"));
            m.put("update_time", rs.getTimestamp("update_time"));
            out.add(m);
        });
        return out;
    }

    /** 订阅详情: 基本信息 + 命中资产(实时子集) + 命中数 */
    public Map<String, Object> detail(long id) {
        Map<String, Object> sub = null;
        try {
            Map<String, Object> row = metaJdbc.queryForMap(
                    "SELECT id, name, expr, mode, target, status, create_by, create_time, update_time "
                            + "FROM asset_subscription WHERE id = ?", id);
            sub = new LinkedHashMap<>();
            sub.put("id", row.get("id"));
            sub.put("name", row.get("name"));
            sub.put("expr", parseExpr(String.valueOf(row.get("expr"))));
            sub.put("mode", row.get("mode"));
            sub.put("target", row.get("target"));
            sub.put("status", row.get("status"));
        } catch (Exception e) {
            return null;
        }
        if (sub == null) {
            return null;
        }
        sub.put("assets", matchedAssets(sub));
        return sub;
    }

    /** 命中资产 = 标签子集查询(密级上限过滤) */
    public List<Map<String, Object>> matchedAssets(Map<String, Object> sub) {
        Map<String, Object> expr = (Map<String, Object>) sub.get("expr");
        if (expr == null) {
            return List.of();
        }
        List<String> tags = strList(expr.get("tags"));
        String op = String.valueOf(expr.getOrDefault("op", "AND"));
        List<String> exclude = strList(expr.get("exclude"));
        Number maxSecret = expr.get("maxSecret") instanceof Number n ? n : null;
        List<Map<String, Object>> assets = subset(tags, op, exclude);
        if (maxSecret == null) {
            return assets;
        }
        return assets.stream().filter(a -> (a.get("secretLevel") instanceof Number s ? s.intValue() : 2)
                <= maxSecret.intValue()).toList();
    }

    public long create(String name, Map<String, Object> expr, String mode, String target, String createBy) {
        KeyHolder kh = new GeneratedKeyHolder();
        metaJdbc.update(con -> {
            PreparedStatement ps = con.prepareStatement(
                    "INSERT INTO asset_subscription (name, expr, mode, target, status, create_by) VALUES (?, ?, ?, ?, '运行中', ?)",
                    Statement.RETURN_GENERATED_KEYS);
            ps.setString(1, name);
            ps.setString(2, toJson(expr));
            ps.setString(3, mode == null ? "pull" : mode);
            ps.setString(4, target);
            ps.setString(5, createBy);
            return ps;
        }, kh);
        return kh.getKey().longValue();
    }

    public void update(long id, String name, Map<String, Object> expr, String mode, String target) {
        StringBuilder sql = new StringBuilder("UPDATE asset_subscription SET ");
        List<Object> args = new ArrayList<>();
        if (name != null && !name.isBlank()) {
            sql.append("name = ?, ");
            args.add(name);
        }
        if (expr != null) {
            sql.append("expr = ?, ");
            args.add(toJson(expr));
        }
        if (mode != null) {
            sql.append("mode = ?, ");
            args.add(mode);
        }
        if (target != null) {
            sql.append("target = ?, ");
            args.add(target);
        }
        if (args.isEmpty()) {
            return;
        }
        sql.setLength(sql.length() - 2);
        sql.append(" WHERE id = ?");
        args.add(id);
        metaJdbc.update(sql.toString(), args.toArray());
    }

    public void setStatus(long id, String status) {
        metaJdbc.update("UPDATE asset_subscription SET status = ? WHERE id = ?", status, id);
    }

    public void delete(long id) {
        metaJdbc.update("DELETE FROM asset_subscription WHERE id = ?", id);
    }

    private Map<String, Object> parseExpr(String expr) {
        try {
            JsonNode node = om.readTree(expr);
            Map<String, Object> m = new LinkedHashMap<>();
            List<String> tags = new ArrayList<>();
            node.path("tags").forEach(t -> tags.add(t.asText()));
            m.put("tags", tags);
            m.put("op", node.path("op").asText("AND"));
            List<String> exclude = new ArrayList<>();
            node.path("exclude").forEach(t -> exclude.add(t.asText()));
            m.put("exclude", exclude);
            if (node.has("maxSecret")) {
                m.put("maxSecret", node.get("maxSecret").asInt());
            }
            return m;
        } catch (Exception e) {
            return new LinkedHashMap<>();
        }
    }

    private List<String> strList(Object o) {
        if (o instanceof List<?> list) {
            return list.stream().map(String::valueOf).toList();
        }
        return List.of();
    }

    private String toJson(Object o) {
        try {
            return om.writeValueAsString(o);
        } catch (Exception e) {
            return "{}";
        }
    }

    // ============ 标签子集内嵌实现(与 asset-service TagService.subset 同源逻辑;
    // 拆分后订阅命中改为本服务自治查询 logical_table/asset_meta/tag_dict, 避免跨服务鉴权调用) ============

    /** 逻辑表全量 + 可维护元数据增强(展示名/域/分层/密级/标签) */
    private List<Map<String, Object>> assetsWithMeta() {
        List<Map<String, Object>> out = new ArrayList<>();
        List<Map<String, Object>> tables = metaJdbc.queryForList(
                "SELECT table_name, topic, bucket_count, table_count, table_prefix, partition_count, updated_at " +
                        "FROM logical_table ORDER BY table_name");
        Map<String, Map<String, Object>> metas = new HashMap<>();
        try {
            for (Map<String, Object> r : metaJdbc.queryForList("SELECT * FROM asset_meta")) {
                Object k = r.get("table_name");
                if (k != null) metas.put(String.valueOf(k), r);
            }
        } catch (Exception e) {
            log.warn("asset_meta 批量读取失败(降级为空): {}", e.getMessage());
        }
        for (Map<String, Object> t : tables) {
            String table = String.valueOf(t.get("table_name"));
            Map<String, Object> a = new LinkedHashMap<>();
            a.put("table", table);
            a.put("name", table);
            a.put("topic", t.get("topic"));
            a.put("sources", List.of());
            Map<String, Object> meta = metas.getOrDefault(table, Map.of());
            a.put("displayName", meta.getOrDefault("display_name", ""));
            a.put("domain", meta.getOrDefault("domain", ""));
            a.put("layer", meta.getOrDefault("layer", ""));
            a.put("secretLevel", meta.getOrDefault("secret_level", 2));
            Object tags = meta.getOrDefault("tags", null);
            if (tags instanceof String s && !s.isBlank()) {
                try {
                    a.put("tags", om.readValue(s, List.class));
                } catch (Exception e) {
                    a.put("tags", null);
                }
            } else {
                a.put("tags", null);
            }
            out.add(a);
        }
        return out;
    }

    private List<Map<String, Object>> loadTags() {
        return metaJdbc.queryForList("SELECT id, parent_id, name FROM tag_dict ORDER BY sort, id");
    }

    /** 命中判定: path 等于某标签 或 是其子孙 */
    private boolean matches(Set<String> paths, String tagPath) {
        return paths.stream().anyMatch(p -> p.equals(tagPath) || p.startsWith(tagPath + "/"));
    }

    /** 标签子集: include(AND/OR)命中 + exclude 剔除 */
    public List<Map<String, Object>> subset(List<String> include, String op, List<String> exclude) {
        List<Map<String, Object>> all = assetsWithMeta();
        Map<String, Set<String>> assetPaths = assetTagPaths(all, loadTags());
        List<String> excludes = exclude == null ? List.of() : exclude;
        boolean and = !"OR".equalsIgnoreCase(op);
        return all.stream().filter(a -> {
            Set<String> paths = assetPaths.getOrDefault(String.valueOf(a.get("table")), Set.of());
            boolean hitInclude = include == null || include.isEmpty() || (and
                    ? include.stream().allMatch(t -> matches(paths, t))
                    : include.stream().anyMatch(t -> matches(paths, t)));
            if (!hitInclude) {
                return false;
            }
            return excludes.stream().noneMatch(t -> matches(paths, t));
        }).toList();
    }

    /** 每资产命中的标签 path 集合(系统标签规则 + 手动标签) */
    private Map<String, Set<String>> assetTagPaths(List<Map<String, Object>> assets,
                                                   List<Map<String, Object>> tagsIn) {
        Map<String, Set<String>> map = new HashMap<>();
        Map<Long, Map<String, Object>> tagMap = new LinkedHashMap<>();
        for (Map<String, Object> t : tagsIn) {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("id", t.get("id"));
            m.put("parent_id", t.get("parent_id"));
            m.put("tag", t.get("name"));
            tagMap.put(((Number) t.get("id")).longValue(), m);
        }
        List<String> allPaths = new ArrayList<>();
        for (Map<String, Object> t : tagsIn) {
            allPaths.add(buildPath(tagMap, ((Number) t.get("id")).longValue()));
        }
        for (Map<String, Object> a : assets) {
            String table = String.valueOf(a.get("table"));
            Set<String> paths = new HashSet<>();
            for (String p : allPaths) {
                if (assetMatchesTag(a, p)) paths.add(p);
            }
            Object manual = a.get("tags");
            if (manual instanceof List<?> list) {
                for (Object t : list) paths.add(String.valueOf(t));
            }
            map.put(table, paths);
        }
        return map;
    }

    /** 资产是否命中某标签 path(系统标签规则 + 手动标签) */
    private boolean assetMatchesTag(Map<String, Object> asset, String path) {
        String[] secNames = {"", "公开", "内部", "机密", "绝密"};
        if (path.startsWith("分层/")) {
            return path.substring("分层/".length()).equals(asset.get("layer"));
        }
        if (path.startsWith("密级/")) {
            String name = path.substring("密级/".length());
            int sec = asset.get("secretLevel") instanceof Number n ? n.intValue() : 2;
            return sec >= 1 && sec <= 4 && name.equals(secNames[sec]);
        }
        Object tags = asset.get("tags");
        if (tags instanceof List<?> list) {
            return list.stream().anyMatch(t -> String.valueOf(t).equals(path));
        }
        return false;
    }

    private String buildPath(Map<Long, Map<String, Object>> nodeMap, long id) {
        return buildPath(nodeMap, id, new HashSet<>());
    }

    /** 父链求路径; guard 防父链成环时无限递归(脏数据兜底) */
    private String buildPath(Map<Long, Map<String, Object>> nodeMap, long id, Set<Long> guard) {
        Map<String, Object> nd = nodeMap.get(id);
        if (nd == null || !guard.add(id)) return "";
        long pid = ((Number) nd.get("parent_id")).longValue();
        String name = String.valueOf(nd.get("tag"));
        if (pid == 0) return name;
        String parent = buildPath(nodeMap, pid, guard);
        return parent.isEmpty() ? name : parent + "/" + name;
    }
}
