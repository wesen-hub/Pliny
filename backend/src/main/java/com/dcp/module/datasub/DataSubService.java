package com.dcp.module.datasub;

import com.dcp.common.BizException;
import com.dcp.common.ErrorCode;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.stereotype.Service;

import java.security.SecureRandom;
import java.util.*;

/**
 * 数据订阅(2026-09-09 方案): 对外数据契约 —— 视图定义解析 + 目标库/标签视图查询 + 审批 + 交付(pull API/push)。
 * 视图:
 *   logical: tables[](+cols 同构)/merge/conds 组构造器; 增量列=表 _seq% 探测
 *   tag: tags[]{path,value}/op(AND|OR)/exclude/shape(entity|matrix|wide)+joinTable/joinKey/joinCols
 * 交付: pull=数据服务 API(schema/snapshot/changes/export); push=deliver(webhook HMAC 验签目标/Kafka topic)。
 * 鉴权: pull 用 X-Dcp-Token(生效签发); push webhook 订阅方侧 secret 验签(生成下发)。
 */
@Service
public class DataSubService {

    private static final ObjectMapper OM = new ObjectMapper();
    private final JdbcTemplate metaJdbc;
    private final String targetUrl;
    private final String targetUser;
    private final String targetPassword;
    private volatile JdbcTemplate targetJdbc;
    private final SecureRandom rnd = new SecureRandom();

    public DataSubService(JdbcTemplate metaJdbc,
                          @Value("${dcp.sync.target.url}") String targetUrl,
                          @Value("${dcp.sync.target.username:root}") String username,
                          @Value("${dcp.sync.target.password:root123}") String password) {
        this.metaJdbc = metaJdbc;
        this.targetUrl = targetUrl;
        this.targetUser = username;
        this.targetPassword = password;
    }

    private JdbcTemplate target() {
        if (targetJdbc == null) {
            synchronized (this) {
                if (targetJdbc == null) {
                    DriverManagerDataSource ds = new DriverManagerDataSource();
                    ds.setDriverClassName("com.mysql.cj.jdbc.Driver");
                    ds.setUrl(targetUrl);
                    ds.setUsername(targetUser);
                    ds.setPassword(targetPassword);
                    targetJdbc = new JdbcTemplate(ds);
                }
            }
        }
        return targetJdbc;
    }

    /* ---------- 工具 ---------- */

    private static String s(Object o) {
        return o == null ? "" : String.valueOf(o);
    }

    private String tok(int len) {
        byte[] b = new byte[len];
        rnd.nextBytes(b);
        return java.util.HexFormat.of().formatHex(b);
    }

    private static String ident(String x) {
        return x == null || !x.matches("[A-Za-z0-9_]+") ? null : x;
    }

    private static String lit(String x) {
        return x == null ? "''" : "'" + x.replace("'", "''") + "'";
    }

    public JsonNode viewOf(Map<String, Object> c) {
        try {
            return OM.readTree((String) c.get("view_json"));
        } catch (Exception e) {
            throw new BizException(ErrorCode.PARAM_ERROR, "视图定义损坏");
        }
    }

    public Map<String, Object> getContract(long id) {
        List<Map<String, Object>> rows = metaJdbc.queryForList(
                "SELECT id,name,subscriber,view_type,view_json,push,freq,target_type,target_url,topic,max_secret,"
                        + "ttl,status,token,webhook_secret,approved_by,approved_at,reject_reason,create_by,create_time,update_time"
                        + " FROM data_contract WHERE id=?", id);
        if (rows.isEmpty()) throw new BizException(ErrorCode.NOT_FOUND, "契约不存在 id=" + id);
        Map<String, Object> m = new LinkedHashMap<>(rows.get(0));
        m.put("view", viewOf(m));
        m.remove("view_json");
        return m;
    }

    /* ---------- 契约管理(申请/审批/状态) ---------- */

    public Map<String, Object> create(Map<String, Object> body, String who) {
        String name = s(body.get("name")).trim();
        String subscriber = s(body.get("subscriber")).trim();
        String vt = s(body.get("viewType"));
        if (name.isEmpty() || subscriber.isEmpty() || !List.of("logical", "tag").contains(vt)) {
            throw new BizException(ErrorCode.PARAM_ERROR, "name/subscriber/viewType 必填");
        }
        JsonNode view = body.get("view") instanceof JsonNode jn ? jn : OM.valueToTree(body.get("view"));
        if (view == null || !view.isObject()) throw new BizException(ErrorCode.PARAM_ERROR, "view 定义必填");
        // 简单结构校验
        if ("logical".equals(vt) && (!view.path("tables").isArray() || view.path("tables").isEmpty())) {
            throw new BizException(ErrorCode.PARAM_ERROR, "logical 视图需 tables[]");
        }
        if ("tag".equals(vt) && (!view.path("tags").isArray() || view.path("tags").isEmpty())) {
            throw new BizException(ErrorCode.PARAM_ERROR, "tag 视图需 tags[]");
        }
        String push = "push".equals(s(body.get("push"))) ? "push" : "pull";
        String freq = s(body.getOrDefault("freq", "manual"));
        String tt = s(body.get("targetType"));
        String url = s(body.get("targetUrl"));
        int maxSecret = body.get("maxSecret") instanceof Number n ? n.intValue() : 2;
        java.sql.Date ttl = null;
        if (body.get("ttl") != null && !s(body.get("ttl")).isBlank()) {
            try {
                ttl = java.sql.Date.valueOf(s(body.get("ttl")).substring(0, 10));
            } catch (Exception ignore) {
            }
        }
        metaJdbc.update("INSERT INTO data_contract(name,subscriber,view_type,view_json,push,freq,target_type,target_url,max_secret,ttl,status,create_by)"
                        + " VALUES(?,?,?,?,?,?,?,?,?,?,'pending',?)",
                name, subscriber, vt, view.toString(), push, freq,
                "push".equals(push) ? tt : null,
                "push".equals(push) && "webhook".equals(tt) ? url : null,
                maxSecret, ttl, who);
        Number id = metaJdbc.queryForObject("SELECT LAST_INSERT_ID()", Number.class);
        audit("APPLY", id.longValue(), "订阅申请: " + name + " 订阅方=" + subscriber);
        return Map.of("id", id.longValue(), "status", "pending");
    }

    /** 审批(可调列范围/密级): 生效签发 token / webhook secret */
    public Map<String, Object> approve(long id, Map<String, Object> body, String who) {
        Map<String, Object> c = getContract(id);
        if (!"pending".equals(s(c.get("status")))) throw new BizException(ErrorCode.PARAM_ERROR, "仅待审批可批准");
        JsonNode view = (JsonNode) c.get("view");
        // 范围调整: 收窄列(仅删除不新增)
        JsonNode cols = body.get("cols") == null ? null : OM.valueToTree(body.get("cols"));
        if (cols != null && cols.isArray() && "logical".equals(s(c.get("view_type")))) {
            ArrayNode newCols = OM.createArrayNode();
            Set<String> base = new HashSet<>();
            view.path("cols").forEach(x -> base.add(x.asText()));
            for (JsonNode cc : cols) {
                if (base.contains(cc.asText())) newCols.add(cc.asText());
            }
            ((ObjectNode) view).set("cols", newCols);
        }
        int maxSecret = body.get("maxSecret") instanceof Number n ? n.intValue() : ((Number) c.get("max_secret")).intValue();
        String token = tok(32);
        String secret = "webhook".equals(s(c.get("target_type"))) ? tok(32) : null;
        metaJdbc.update("UPDATE data_contract SET status='active', view_json=?, max_secret=?, token=?, webhook_secret=?,"
                        + " approved_by=?, approved_at=NOW() WHERE id=?",
                view.toString(), maxSecret, token, secret, who, id);
        audit("APPROVE", id, "批准契约 " + s(c.get("name")) + " maxSecret=" + maxSecret);
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("id", id);
        out.put("status", "active");
        out.put("token", token);   // pull 拉取鉴权(下发订阅方)
        if (secret != null) out.put("webhookSecret", secret);
        return out;
    }

    public void reject(long id, String reason, String who) {
        Map<String, Object> c = getContract(id);
        if (!"pending".equals(s(c.get("status")))) throw new BizException(ErrorCode.PARAM_ERROR, "仅待审批可拒绝");
        metaJdbc.update("UPDATE data_contract SET status='rejected', reject_reason=?, approved_by=? WHERE id=?",
                reason == null ? "未说明" : reason, who, id);
        audit("REJECT", id, "拒绝契约 " + s(c.get("name")));
    }

    public void stop(long id) {
        metaJdbc.update("UPDATE data_contract SET status='stopped' WHERE id=?", id);
    }

    private Map<String, Object> requireActiveForPull(long id, String token) {
        Map<String, Object> c = getContract(id);
        if (!"active".equals(s(c.get("status")))) throw new BizException(ErrorCode.FORBIDDEN, "契约未生效(状态 " + c.get("status") + ")");
        if (token == null || !token.equals(s(c.get("token")))) throw new BizException(ErrorCode.FORBIDDEN, "X-Dcp-Token 无效");
        java.sql.Date ttl = (java.sql.Date) c.get("ttl");
        if (ttl != null && ttl.toLocalDate().isBefore(java.time.LocalDate.now())) {
            metaJdbc.update("UPDATE data_contract SET status='expired' WHERE id=?", id);
            throw new BizException(ErrorCode.FORBIDDEN, "契约已到期");
        }
        return c;
    }

    public List<Map<String, Object>> list() {
        List<Map<String, Object>> rows = metaJdbc.queryForList(
                "SELECT id,name,subscriber,view_type,view_json,push,freq,target_type,target_url,topic,max_secret,ttl,status,"
                        + "approved_by,approved_at,reject_reason,create_by,create_time,update_time FROM data_contract ORDER BY id DESC");
        List<Map<String, Object>> out = new ArrayList<>();
        for (Map<String, Object> r : rows) {
            try {
                JsonNode view = OM.readTree((String) r.get("view_json"));
                r.put("view", view);
            } catch (Exception ignore) {
            }
            r.remove("view_json");
            out.add(r);
        }
        return out;
    }

    /** 数据视图目录(向导用): 目标库逻辑表(列/行数/增量键) + 标签目录(圈选 path + 实体数) */
    public Map<String, Object> catalog() {
        List<Map<String, Object>> tables = new ArrayList<>();
        List<Map<String, Object>> trows = target().queryForList(
                "SELECT TABLE_NAME name FROM information_schema.TABLES WHERE TABLE_SCHEMA=DATABASE()"
                        + " AND TABLE_NAME NOT LIKE '%authority%' AND TABLE_NAME NOT LIKE 'wqe' ORDER BY TABLE_NAME");
        for (Map<String, Object> t : trows) {
            String name = s(t.get("name"));
            List<String> cols = new ArrayList<>();
            List<Map<String, Object>> crows = target().queryForList(
                    "SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=?"
                            + " AND COLUMN_NAME NOT IN ('_is_deleted') ORDER BY ORDINAL_POSITION", name);
            crows.forEach(c -> cols.add(s(c.get("COLUMN_NAME"))));
            Number cnt = target().queryForObject("SELECT COUNT(*) FROM `" + name + "`", Number.class);
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("name", name);
            m.put("cols", cols);
            m.put("rows", cnt == null ? 0 : cnt.longValue());
            m.put("incrementKey", probeSeqCol(name));
            tables.add(m);
        }
        List<Map<String, Object>> tagRows = metaJdbc.queryForList(
                "SELECT tag, COUNT(DISTINCT entity) cnt FROM tag_fact_view GROUP BY tag ORDER BY tag");
        List<Map<String, Object>> tags = new ArrayList<>();
        for (Map<String, Object> r : tagRows) {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("path", s(r.get("tag")));
            m.put("entities", r.get("cnt"));
            tags.add(m);
        }
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("tables", tables);
        out.put("tags", tags);
        return out;
    }

    public List<Map<String, Object>> deliveries(long id) {
        return metaJdbc.queryForList("SELECT id,mode,channel,range_text,rows_count,status,retries,error_msg,ts"
                + " FROM data_sub_delivery WHERE contract_id=? ORDER BY id DESC LIMIT 50", id);
    }

    /** PULL schema: 视图结构(列/类型/主键/增量键); 需 active+token */
    public Map<String, Object> schema(long id, String token) {
        Map<String, Object> c = requireActiveForPull(id, token);
        JsonNode view = (JsonNode) c.get("view");
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("contract", s(c.get("name")));
        out.put("viewType", c.get("view_type"));
        out.put("maxSecret", c.get("max_secret"));
        if ("logical".equals(s(c.get("view_type")))) {
            List<Map<String, Object>> colsMeta = new ArrayList<>();
            List<String> cols = new ArrayList<>();
            view.path("cols").forEach(x -> cols.add(x.asText()));
            String firstTable = view.path("tables").get(0).asText();
            try {
                List<Map<String, Object>> metas = target().queryForList(
                        "SELECT COLUMN_NAME name, DATA_TYPE type FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE()"
                                + " AND TABLE_NAME=? AND COLUMN_NAME IN ("
                                + cols.stream().map(DataSubService::lit).collect(java.util.stream.Collectors.joining(","))
                                + ") ORDER BY ORDINAL_POSITION", firstTable);
                colsMeta = metas;
            } catch (Exception ignore) {
            }
            out.put("table", firstTable);
            out.put("tables", view.path("tables"));
            out.put("merge", view.path("merge").asText(""));
            out.put("cols", cols);
            out.put("colTypes", colsMeta);
            out.put("incrementKey", probeSeqCol(firstTable));
        } else {
            List<Map<String, Object>> tags = new ArrayList<>();
            view.path("tags").forEach(t -> tags.add(Map.of("path", t.path("path").asText(), "value", t.path("value").asText(""))));
            out.put("tags", tags);
            out.put("op", view.path("op").asText("OR"));
            out.put("shape", view.path("shape").asText("entity"));
            if ("wide".equals(view.path("shape").asText())) {
                out.put("joinTable", view.path("joinTable").asText());
                List<String> jc = new ArrayList<>();
                view.path("joinCols").forEach(x -> jc.add(x.asText()));
                out.put("joinCols", jc);
            }
            out.put("rows", "实体(键=" + s(view.path("joinKey").asText("entity_key")) + ")");
        }
        return out;
    }

    /* ---------- 视图查询 ---------- */

    /** 逻辑表视图行(conds 组: 组内 AND; 组间 AND——多组语义按页面"组间关系"存储, 这里组间 AND 简化; OR 场景前端用单组 in) */
    public List<Map<String, Object>> queryLogical(JsonNode view, Long afterSeq, int limit) {
        List<String> tables = new ArrayList<>();
        for (JsonNode t : view.path("tables")) {
            String n = ident(t.asText());
            if (n == null) throw new BizException(ErrorCode.PARAM_ERROR, "非法表名");
            tables.add(n);
        }
        if (tables.isEmpty()) throw new BizException(ErrorCode.PARAM_ERROR, "tables 为空");
        // 列白名单
        List<String> cols = new ArrayList<>();
        for (JsonNode c : view.path("cols")) {
            String n = ident(c.asText());
            if (n != null) cols.add(n);
        }
        if (cols.isEmpty()) throw new BizException(ErrorCode.PARAM_ERROR, "视图未选交付列");
        boolean merged = tables.size() > 1 && "merge".equals(view.path("merge").asText("merge"));
        // 增量列探测(取表 _seq 列; 多表取第一张)
        String seqCol = probeSeqCol(tables.get(0));
        if (afterSeq != null && seqCol == null) throw new BizException(ErrorCode.PARAM_ERROR, "表无 _seq 增量列, 不支持增量拉取");
        String where = condSql(view);
        List<Map<String, Object>> out = new ArrayList<>();
        for (String t : tables) {
            StringBuilder sql = new StringBuilder("SELECT ");
            if (merged) {
                sql.append(lit(t)).append(" AS source_table,");
            }
            List<String> sel = new ArrayList<>(cols);
            if (afterSeq != null && seqCol != null && !sel.contains(seqCol)) sel.add(seqCol);
            sql.append(String.join(",", sel)).append(" FROM `").append(t).append("`");
            List<String> ws = new ArrayList<>();
            if (where != null) ws.add(where);
            if (afterSeq != null) ws.add("`" + seqCol + "` > " + afterSeq);
            if (!ws.isEmpty()) sql.append(" WHERE ").append(String.join(" AND ", ws));
            if (afterSeq != null) sql.append(" ORDER BY `").append(seqCol).append("` LIMIT ").append(Math.min(Math.max(limit, 1), 1000));
            try {
                out.addAll(target().queryForList(sql.toString()));
            } catch (Exception e) {
                throw new BizException(ErrorCode.PARAM_ERROR, "表查询失败 " + t + ": " + e.getMessage());
            }
        }
        return out;
    }

    private String probeSeqCol(String table) {
        try {
            List<Map<String, Object>> rows = target().queryForList(
                    "SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=? AND COLUMN_NAME LIKE '_seq%' ORDER BY ORDINAL_POSITION LIMIT 1",
                    table);
            return rows.isEmpty() ? null : s(rows.get(0).get("COLUMN_NAME"));
        } catch (Exception e) {
            return null;
        }
    }

    private String condSql(JsonNode view) {
        JsonNode groups = view.path("groups");
        if (!groups.isArray() || groups.isEmpty()) return null;
        List<String> groupExpr = new ArrayList<>();
        for (JsonNode g : groups) {
            List<String> conds = new ArrayList<>();
            for (JsonNode c : g.path("conds")) {
                String f = ident(c.path("field").asText());
                String op = c.path("op").asText("");
                String v = c.path("val").asText("");
                if (f == null || op.isBlank()) continue;
                String e = null;
                switch (op) {
                    case "=":
                        e = f + "=" + lit(v);
                        break;
                    case "!=":
                        e = f + "<>" + lit(v);
                        break;
                    case ">":
                    case ">=":
                    case "<":
                    case "<=":
                        e = f + op + (v.isBlank() ? "0" : v);
                        break;
                    case "in": {
                        List<String> vs = new ArrayList<>();
                        for (String p : v.split("[,，]")) if (!p.isBlank()) vs.add(lit(p.trim()));
                        if (!vs.isEmpty()) e = f + " IN (" + String.join(",", vs) + ")";
                        break;
                    }
                    case "like":
                        e = f + " LIKE " + lit("%" + v + "%");
                        break;
                    case "null":
                        e = f + " IS NULL";
                        break;
                    default:
                        break;
                }
                if (e == null) continue;
                conds.add(e);
            }
            if (!conds.isEmpty()) groupExpr.add("(" + String.join(" AND ", conds) + ")");
        }
        return groupExpr.isEmpty() ? null : String.join(" AND ", groupExpr);
    }

    /** 标签视图: 圈选实体集合(AND 交/OR 并 + exclude) */
    public Set<String> tagEntities(JsonNode view) {
        String op = view.path("op").asText("OR");
        Set<String> acc = op.equals("AND") ? null : new LinkedHashSet<>();
        for (JsonNode t : view.path("tags")) {
            String path = t.path("path").asText("");
            String value = t.path("value").asText("");
            if (path.isBlank()) continue;
            List<Map<String, Object>> rows;
            if (value.isBlank()) {
                rows = metaJdbc.queryForList("SELECT DISTINCT entity FROM tag_fact_view WHERE tag=?", path);
            } else {
                rows = metaJdbc.queryForList("SELECT DISTINCT entity FROM tag_fact_view WHERE tag=? AND value=?", path, value);
            }
            Set<String> cur = new LinkedHashSet<>();
            rows.forEach(r -> cur.add(s(r.get("entity"))));
            if (op.equals("AND")) {
                if (acc == null) acc = new LinkedHashSet<>(cur);
                else acc.retainAll(cur);
            } else {
                acc.addAll(cur);
            }
        }
        if (acc == null) return new LinkedHashSet<>();
        // exclude
        for (JsonNode ex : view.path("exclude")) {
            String path = ex.isTextual() ? ex.asText() : ex.path("path").asText("");
            if (path.isBlank()) continue;
            List<Map<String, Object>> rows = metaJdbc.queryForList("SELECT DISTINCT entity FROM tag_fact_view WHERE tag=?", path);
            Set<String> exSet = new HashSet<>();
            rows.forEach(r -> exSet.add(s(r.get("entity"))));
            acc.removeAll(exSet);
        }
        return acc;
    }

    /** 标签视图按形态渲染: entity 键 / matrix(每标签值列) / wide(回表字段) */
    public List<Map<String, Object>> renderTagView(JsonNode view, Set<String> entities) {
        List<Map<String, Object>> out = new ArrayList<>();
        String shape = view.path("shape").asText("entity");
        List<JsonNode> tags = new ArrayList<>();
        view.path("tags").forEach(tags::add);
        for (String e : entities) {
            Map<String, Object> m = new LinkedHashMap<>();
            if ("wide".equals(shape)) {
                m.put("entity_key", e);
                String joinTable = ident(view.path("joinTable").asText(""));
                String joinKey = ident(view.path("joinKey").asText("id"));
                List<String> joinCols = new ArrayList<>();
                view.path("joinCols").forEach(c -> {
                    String n = ident(c.asText());
                    if (n != null) joinCols.add(n);
                });
                if (joinTable != null && !joinCols.isEmpty()) {
                    try {
                        List<Map<String, Object>> rows = target().queryForList(
                                "SELECT " + String.join(",", joinCols) + " FROM `" + joinTable
                                        + "` WHERE `" + joinKey + "`=? LIMIT 1", e);
                        if (!rows.isEmpty()) m.putAll(rows.get(0));
                    } catch (Exception ex) {
                        m.put("__joinError", ex.getMessage());
                    }
                }
            } else {
                m.put("entity_key", e);
                if ("matrix".equals(shape)) {
                    for (JsonNode t : tags) {
                        String path = t.path("path").asText("");
                        List<Map<String, Object>> rows = metaJdbc.queryForList(
                                "SELECT value FROM tag_fact_view WHERE entity=? AND tag=? LIMIT 1", e, path);
                        m.put(path, rows.isEmpty() ? "∅" : rows.get(0).get("value") == null ? "✓" : s(rows.get(0).get("value")));
                    }
                }
            }
            out.add(m);
        }
        return out;
    }

    /* ---------- 交付(PULL 由订阅方调; PUSH 平台 deliver 触发) ---------- */

    /** PULL snapshot/changes 统一取数入口 */
    public Map<String, Object> pull(long id, String token, Long after, int limit, boolean asChanges) {
        Map<String, Object> c = requireActiveForPull(id, token);
        JsonNode view = (JsonNode) c.get("view");
        List<Map<String, Object>> rows;
        String nextCursor = null;
        if ("logical".equals(s(c.get("view_type")))) {
            rows = queryLogical(view, asChanges ? after : null, limit);
            if (asChanges && !rows.isEmpty()) {
                String seqCol = probeSeqCol(view.path("tables").get(0).asText());
                for (Map<String, Object> r : rows) {
                    Object sc = r.get(seqCol);
                    if (sc != null) nextCursor = String.valueOf(sc);
                }
            }
        } else {
            Set<String> ents = tagEntities(view);
            List<String> sorted = new ArrayList<>(ents);
            if (after != null && asChanges) {
                // 标签增量: after 为上次实体数(简化: 分页语义)——取实体序号 > after
                List<String> sub = sorted.stream().skip(after).limit(limit).toList();
                rows = renderTagView(view, new LinkedHashSet<>(sub));
                if (!sub.isEmpty()) nextCursor = String.valueOf(after + sub.size());
            } else {
                rows = renderTagView(view, new LinkedHashSet<>(sorted));
            }
        }
        // 交付审计 + 记录
        metaJdbc.update("INSERT INTO data_sub_delivery(contract_id,mode,channel,range_text,rows_count,status) VALUES(?,?,?,?,?,'ok')",
                id, "pull", "api", asChanges ? "after=" + (after == null ? "0" : after) : "snapshot", rows.size());
        audit("PULL", id, (asChanges ? "增量拉取 after=" + after + " " : "快照拉取 ") + rows.size() + " 行");
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("contract", s(c.get("name")));
        out.put("rows", rows);
        out.put("count", rows.size());
        out.put("nextCursor", nextCursor);
        return out;
    }

    /** PUSH 投递(平台触发: 手动/后续调度); webhook 验签密钥已随生效下发; kafka 发 topic */
    public Map<String, Object> deliver(long id, String who) {
        Map<String, Object> c = getContract(id);
        if (!"active".equals(s(c.get("status"))) && !"push".equals(s(c.get("push")))) {
            throw new BizException(ErrorCode.PARAM_ERROR, "仅生效 push 契约可投递");
        }
        JsonNode view = (JsonNode) c.get("view");
        List<Map<String, Object>> rows = "logical".equals(s(c.get("view_type")))
                ? queryLogical(view, null, 1000)
                : renderTagView(view, tagEntities(view));
        String channel = "webhook".equals(s(c.get("target_type"))) ? "webhook" : "kafka";
        String status = "ok";
        int retries = 0;
        String err = null;
        try {
            if ("webhook".equals(channel)) {
                String url = s(c.get("target_url"));
                String secret = s(c.get("webhook_secret"));
                if (url.isBlank()) throw new IllegalStateException("未配置目标 URL");
                String body = OM.writeValueAsString(Map.of("contract", s(c.get("name")), "rows", rows, "count", rows.size()));
                long ts = System.currentTimeMillis();
                String sign = hmac(secret, ts, body);
                var client = java.net.http.HttpClient.newHttpClient();
                var req = java.net.http.HttpRequest.newBuilder()
                        .uri(java.net.URI.create(url))
                        .header("Content-Type", "application/json")
                        .header("X-Dcp-Contract", s(c.get("id")))
                        .header("X-Dcp-Timestamp", String.valueOf(ts))
                        .header("X-Dcp-Sign", sign)
                        .POST(java.net.http.HttpRequest.BodyPublishers.ofString(body))
                        .timeout(java.time.Duration.ofSeconds(5))
                        .build();
                var resp = client.send(req, java.net.http.HttpResponse.BodyHandlers.ofString());
                if (resp.statusCode() >= 400) throw new IllegalStateException("webhook HTTP " + resp.statusCode());
            } else {
                kafkaSend("dcp.sub." + s(c.get("subscriber")) + "." + id,
                        OM.writeValueAsString(Map.of("contract", s(c.get("name")), "rows", rows, "count", rows.size())));
            }
        } catch (Exception e) {
            status = "fail";
            err = e.getMessage();
            retries = 1;
        }
        metaJdbc.update("INSERT INTO data_sub_delivery(contract_id,mode,channel,range_text,rows_count,status,retries,error_msg)"
                        + " VALUES(?,?,?,?,?,?,?,?)", id, "push", channel, "deliver", rows.size(), status, retries, err);
        audit("DELIVER", id, (status.equals("ok") ? "投递成功 " : "投递失败 ") + channel + " " + rows.size() + " 行" + (err == null ? "" : ": " + err));
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("id", id);
        out.put("channel", channel);
        out.put("rows", rows.size());
        out.put("status", status);
        out.put("error", err);
        return out;
    }

    private static String hmac(String secret, long ts, String body) {
        try {
            javax.crypto.Mac mac = javax.crypto.Mac.getInstance("HmacSHA256");
            mac.init(new javax.crypto.spec.SecretKeySpec(secret.getBytes(java.nio.charset.StandardCharsets.UTF_8), "HmacSHA256"));
            return java.util.HexFormat.of().formatHex(mac.doFinal((ts + body).getBytes(java.nio.charset.StandardCharsets.UTF_8)));
        } catch (Exception e) {
            return "";
        }
    }

    private volatile org.apache.kafka.clients.producer.KafkaProducer<String, String> kafkaProducer;

    private void kafkaSend(String topic, String payload) {
        if (kafkaProducer == null) {
            synchronized (this) {
                if (kafkaProducer == null) {
                    java.util.Properties p = new java.util.Properties();
                    p.put("bootstrap.servers", envOr("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"));
                    p.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
                    p.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
                    kafkaProducer = new org.apache.kafka.clients.producer.KafkaProducer<>(p);
                }
            }
        }
        try {
            kafkaProducer.send(new org.apache.kafka.clients.producer.ProducerRecord<>(topic, payload)).get(5, java.util.concurrent.TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new IllegalStateException("Kafka 投递失败: " + e.getMessage());
        }
    }

    private static String envOr(String k, String def) {
        String v = System.getenv(k);
        return v == null || v.isBlank() ? def : v;
    }

    /** CSV 导出(全量视图, 直接返回文本) */
    public String exportCsv(long id, String token) {
        Map<String, Object> c = requireActiveForPull(id, token);
        JsonNode view = (JsonNode) c.get("view");
        List<Map<String, Object>> rows = "logical".equals(s(c.get("view_type")))
                ? queryLogical(view, null, 10000)
                : renderTagView(view, tagEntities(view));
        StringBuilder sb = new StringBuilder();
        if (!rows.isEmpty()) {
            sb.append(String.join(",", rows.get(0).keySet())).append("\n");
            for (Map<String, Object> r : rows) {
                List<String> vals = new ArrayList<>();
                r.values().forEach(v -> vals.add(String.valueOf(v == null ? "" : v).replace(",", " ")));
                sb.append(String.join(",", vals)).append("\n");
            }
        }
        metaJdbc.update("INSERT INTO data_sub_delivery(contract_id,mode,channel,range_text,rows_count,status) VALUES(?,?,'export',?,?,'ok')",
                id, "pull", "csv", rows.size());
        audit("EXPORT", id, "CSV 导出 " + rows.size() + " 行");
        return sb.toString();
    }

    @org.springframework.beans.factory.annotation.Autowired
    private com.dcp.module.system.service.AuditService auditService;

    private void audit(String op, long id, String detail) {
        try {
            if (auditService != null) auditService.record("data-sub", op, "contract", id, detail);
        } catch (Exception ignore) {
        }
    }
}
