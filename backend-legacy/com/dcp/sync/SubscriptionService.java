package com.dcp.sync;

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
    private final TagService tagService;
    private final ObjectMapper om = new ObjectMapper();

    public SubscriptionService(JdbcTemplate jdbcTemplate, TagService tagService) {
        this.metaJdbc = jdbcTemplate;
        this.tagService = tagService;
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
        List<Map<String, Object>> assets = tagService.subset(tags, op, exclude);
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
}
