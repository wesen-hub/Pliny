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
                        + "push_interval_min,last_cursor,last_deliver_at,ttl,status,token,webhook_secret,approved_by,approved_at,reject_reason,create_by,create_time,update_time"
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
        Integer pushIntervalMin = "push".equals(push) && "定时".equals(freq) && body.get("pushIntervalMin") instanceof Number n2 && n2.intValue() > 0
                ? Math.min(n2.intValue(), 24 * 60) : null;
        int maxSecret = body.get("maxSecret") instanceof Number n ? n.intValue() : 2;
        java.sql.Date ttl = null;
        if (body.get("ttl") != null && !s(body.get("ttl")).isBlank()) {
            try {
                ttl = java.sql.Date.valueOf(s(body.get("ttl")).substring(0, 10));
            } catch (Exception ignore) {
            }
        }
        metaJdbc.update("INSERT INTO data_contract(name,subscriber,view_type,view_json,push,freq,target_type,target_url,push_interval_min,max_secret,ttl,status,create_by)"
                        + " VALUES(?,?,?,?,?,?,?,?,?,?,?,'pending',?)",
                name, subscriber, vt, view.toString(), push, freq,
                "push".equals(push) ? tt : null,
                "push".equals(push) && "webhook".equals(tt) ? url : null,
                pushIntervalMin, maxSecret, ttl, who);
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
                "SELECT id,name,subscriber,view_type,view_json,push,freq,target_type,target_url,topic,max_secret,push_interval_min,last_cursor,last_deliver_at,ttl,status,"
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

    /** 标签变更事件流(occur_ts 时间游标): 圈选标签集内打标/改值事件(exclude 实体剔除) */
    public List<Map<String, Object>> tagChanges(JsonNode view, String afterTs, int limit) {
        List<String> paths = new ArrayList<>();
        view.path("tags").forEach(t -> {
            String x = t.path("path").asText("");
            if (!x.isBlank()) paths.add(lit(x));
        });
        if (paths.isEmpty()) return new ArrayList<>();
        Set<String> exEntities = new HashSet<>();
        for (JsonNode ex : view.path("exclude")) {
            String path = ex.isTextual() ? ex.asText() : ex.path("path").asText("");
            if (path.isBlank()) continue;
            List<Map<String, Object>> exr = metaJdbc.queryForList("SELECT DISTINCT entity FROM tag_fact_view WHERE tag=?", path);
            exr.forEach(r -> exEntities.add(s(r.get("entity"))));
        }
        StringBuilder sql = new StringBuilder("SELECT entity, tag_path, tag_value, occur_ts FROM tag_notify WHERE tag_path IN (")
                .append(String.join(",", paths)).append(")");
        if (afterTs != null && !afterTs.isBlank()) sql.append(" AND occur_ts > ").append(lit(afterTs));
        sql.append(" ORDER BY occur_ts, entity LIMIT ").append(Math.min(Math.max(limit, 1), 5000));
        List<Map<String, Object>> out = new ArrayList<>();
        for (Map<String, Object> r : metaJdbc.queryForList(sql.toString())) {
            if (exEntities.contains(s(r.get("entity")))) continue;
            Map<String, Object> ev = new LinkedHashMap<>();
            ev.put("entity", s(r.get("entity")));
            ev.put("tag", s(r.get("tag_path")));
            ev.put("value", r.get("tag_value") == null ? "" : s(r.get("tag_value")));
            ev.put("ts", s(r.get("occur_ts")).replace('T', ' '));
            out.add(ev);
        }
        return out;
    }

    /** 行集增量键最大值(行须含 seqCol 键; seq 为数值, 数值比较防 "100"<"8" 字符串坑) */
    private static String maxSeqOf(List<Map<String, Object>> rows, String seqCol) {
        if (rows == null || seqCol == null) return null;
        String max = null;
        for (Map<String, Object> r : rows) {
            Object sc = r.get(seqCol);
            if (sc == null) continue;
            String v = s(sc);
            if (max == null) {
                max = v;
                continue;
            }
            try {
                if (new java.math.BigDecimal(v).compareTo(new java.math.BigDecimal(max)) > 0) max = v;
            } catch (NumberFormatException e) {
                if (v.compareTo(max) > 0) max = v;
            }
        }
        return max;
    }

    /** 当前视图全量行(logical 快照 / tag 实体渲染) */
    private List<Map<String, Object>> snapshotRows(Map<String, Object> c) {
        JsonNode view = (JsonNode) c.get("view");
        return "logical".equals(s(c.get("view_type")))
                ? queryLogical(view, null, 10000)
                : renderTagView(view, tagEntities(view));
    }

    /** PULL snapshot/changes 统一取数入口; after 游标: 逻辑表=数字 seq, 标签视图=occur_ts 时间串 */
    public Map<String, Object> pull(long id, String token, String after, int limit, boolean asChanges) {
        Map<String, Object> c = requireActiveForPull(id, token);
        JsonNode view = (JsonNode) c.get("view");
        List<Map<String, Object>> rows;
        String nextCursor = null;
        if ("logical".equals(s(c.get("view_type")))) {
            Long afterSeq = null;
            if (asChanges && after != null && !after.isBlank()) {
                try {
                    afterSeq = Long.valueOf(after);
                } catch (NumberFormatException e) {
                    throw new BizException(ErrorCode.PARAM_ERROR, "逻辑表增量游标须为数字 seq");
                }
            }
            rows = queryLogical(view, asChanges ? afterSeq : null, limit);
            if (asChanges && !rows.isEmpty()) {
                nextCursor = maxSeqOf(rows, probeSeqCol(view.path("tables").get(0).asText()));
            }
        } else if (asChanges) {
            rows = tagChanges(view, after, limit);
            if (!rows.isEmpty()) nextCursor = s(rows.get(rows.size() - 1).get("ts"));
        } else {
            rows = renderTagView(view, tagEntities(view));
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

    /** PUSH 投递; incremental=true 且契约 last_cursor 已存在 → 只推游标后增量(成功才推进游标, at-least-once 可重放); 无游标首轮=全量并初始化游标 */
    public Map<String, Object> deliver(long id, String who, boolean incremental) {
        Map<String, Object> c = getContract(id);
        if (!"active".equals(s(c.get("status"))) || !"push".equals(s(c.get("push")))) {
            throw new BizException(ErrorCode.PARAM_ERROR, "仅生效 push 契约可投递");
        }
        JsonNode view = (JsonNode) c.get("view");
        boolean isLogical = "logical".equals(s(c.get("view_type")));
        String channel = "webhook".equals(s(c.get("target_type"))) ? "webhook" : "kafka";
        String cursor = s(c.get("last_cursor"));
        boolean useIncr = incremental && cursor != null && !cursor.isBlank();
        List<Map<String, Object>> rows;
        String nextCursor = null;
        if (isLogical) {
            String firstTable = ident(view.path("tables").get(0).asText());
            if (firstTable == null) throw new BizException(ErrorCode.PARAM_ERROR, "非法表名");
            String seqCol = probeSeqCol(firstTable);
            if (useIncr) {
                try {
                    long from = Long.parseLong(cursor.trim());
                    rows = queryLogical(view, from, 5000);
                    nextCursor = maxSeqOf(rows, seqCol);
                } catch (NumberFormatException e) {
                    rows = new ArrayList<>();
                }
            } else {
                rows = queryLogical(view, null, 10000);
                if (seqCol != null && !rows.isEmpty()) {
                    Object mx = target().queryForObject("SELECT MAX(`" + seqCol + "`) FROM `" + firstTable + "`", Object.class);
                    if (mx != null) nextCursor = s(mx);
                }
            }
        } else {
            if (useIncr) {
                rows = tagChanges(view, cursor, 5000);
                if (!rows.isEmpty()) nextCursor = s(rows.get(rows.size() - 1).get("ts"));
            } else {
                rows = renderTagView(view, tagEntities(view));
                List<String> paths = new ArrayList<>();
                view.path("tags").forEach(t -> {
                    String x = t.path("path").asText("");
                    if (!x.isBlank()) paths.add(lit(x));
                });
                if (!paths.isEmpty()) {
                    try {
                        Object mx = metaJdbc.queryForObject("SELECT MAX(occur_ts) FROM tag_notify WHERE tag_path IN ("
                                + String.join(",", paths) + ")", Object.class);
                        if (mx != null) nextCursor = s(mx).replace('T', ' ');
                    } catch (Exception ignore) {
                    }
                }
            }
        }
        String status = "ok";
        String err = null;
        try {
            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("contract", s(c.get("name")));
            payload.put("mode", useIncr ? "incremental" : "full");
            if (useIncr) payload.put("from", cursor);
            payload.put("next", nextCursor);
            payload.put("count", rows.size());
            payload.put(useIncr && !isLogical ? "events" : "rows", rows);
            String body = OM.writeValueAsString(payload);
            if ("webhook".equals(channel)) {
                String url = s(c.get("target_url"));
                String secret = s(c.get("webhook_secret"));
                if (url.isBlank()) throw new IllegalStateException("未配置目标 URL");
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
                kafkaSend("dcp.sub." + s(c.get("subscriber")) + "." + id, body);
            }
        } catch (Exception e) {
            status = "fail";
            err = e.getMessage();
        }
        // 失败累计(连续失败次数, 供退避重试上限判断); 成功归零
        int retries = 0;
        if ("fail".equals(status)) {
            try {
                Integer mx = metaJdbc.queryForObject("SELECT COALESCE(MAX(retries),0) FROM data_sub_delivery"
                        + " WHERE contract_id=? AND mode='push' AND status='fail'", Integer.class, id);
                retries = (mx == null ? 0 : mx) + 1;
            } catch (Exception ignore) {
                retries = 1;
            }
        }
        String range = (useIncr ? "incr from=" + cursor : "full") + (nextCursor != null ? " to=" + nextCursor : "");
        metaJdbc.update("INSERT INTO data_sub_delivery(contract_id,mode,channel,range_text,rows_count,status,retries,error_msg)"
                        + " VALUES(?,?,?,?,?,?,?,?)", id, "push", channel, range, rows.size(), status, retries, err);
        // 成功才推进数据游标(失败保留, 下次重放同范围); 节拍时间无条件推进
        if ("ok".equals(status) && nextCursor != null) {
            metaJdbc.update("UPDATE data_contract SET last_cursor=?, last_deliver_at=NOW() WHERE id=?", nextCursor, id);
        } else {
            metaJdbc.update("UPDATE data_contract SET last_deliver_at=NOW() WHERE id=?", id);
        }
        audit("DELIVER", id, (status.equals("ok") ? "投递成功 " : "投递失败 ") + channel + " " + range + " " + rows.size() + " 行" + (err == null ? "" : ": " + err));
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("id", id);
        out.put("channel", channel);
        out.put("mode", useIncr ? "incremental" : "full");
        out.put("rows", rows.size());
        out.put("cursor", nextCursor);
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

    private final java.util.concurrent.ExecutorService exportExec =
            java.util.concurrent.Executors.newFixedThreadPool(2, r -> {
                Thread t = new Thread(r, "dcp-export");
                t.setDaemon(true);
                return t;
            });
    private static final java.nio.file.Path EXPORT_DIR = java.nio.file.Paths.get("/tmp/dcp-export");

    /** 异步导出(方案 3.1): 创建任务(admin 或契约 token) → 后台生成 csv/json → 轮询状态 → TTL 内下载 */
    public Map<String, Object> createExport(long id, String token, boolean admin, String format) {
        String fmt = "json".equals(format) ? "json" : "csv";
        Map<String, Object> c;
        if (admin) {
            c = getContract(id);
            if (!"active".equals(s(c.get("status")))) throw new BizException(ErrorCode.FORBIDDEN, "契约未生效");
        } else {
            c = requireActiveForPull(id, token);
        }
        metaJdbc.update("INSERT INTO data_sub_export(contract_id,format,status,expire_at) VALUES(?,?,'pending',DATE_ADD(NOW(), INTERVAL 30 MINUTE))",
                id, fmt);
        Number taskId = metaJdbc.queryForObject("SELECT LAST_INSERT_ID()", Number.class);
        long tid = taskId.longValue();
        audit("EXPORT", id, "创建导出任务 " + tid + " format=" + fmt);
        exportExec.submit(() -> runExport(tid));
        return Map.of("taskId", tid, "status", "pending");
    }

    private void runExport(long taskId) {
        try {
            List<Map<String, Object>> rows = metaJdbc.queryForList("SELECT contract_id, format FROM data_sub_export WHERE id=?", taskId);
            if (rows.isEmpty()) return;
            Map<String, Object> task = rows.get(0);
            Map<String, Object> c = getContract(((Number) task.get("contract_id")).longValue());
            List<Map<String, Object>> data = snapshotRows(c);
            String fmt = s(task.get("format"));
            String content = "csv".equals(fmt) ? toCsv(data) : OM.writeValueAsString(data);
            java.nio.file.Files.createDirectories(EXPORT_DIR);
            String fn = taskId + "." + fmt;
            java.nio.file.Files.write(EXPORT_DIR.resolve(fn), content.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            metaJdbc.update("UPDATE data_sub_export SET status='done', rows_count=?, file_name=? WHERE id=?", data.size(), fn, taskId);
            metaJdbc.update("INSERT INTO data_sub_delivery(contract_id,mode,channel,range_text,rows_count,status) VALUES(?,?,'export',?,?,'ok')",
                    task.get("contract_id"), "pull", "task#" + taskId, data.size());
        } catch (Exception e) {
            try {
                String m = e.getMessage();
                metaJdbc.update("UPDATE data_sub_export SET status='fail', error_msg=? WHERE id=?",
                        m == null ? "未知错误" : m.substring(0, Math.min(m.length(), 400)), taskId);
            } catch (Exception ignore) {
            }
        }
    }

    private static String toCsv(List<Map<String, Object>> rows) {
        StringBuilder sb = new StringBuilder();
        if (!rows.isEmpty()) {
            sb.append(String.join(",", rows.get(0).keySet())).append("\n");
            for (Map<String, Object> r : rows) {
                List<String> vals = new ArrayList<>();
                r.values().forEach(v -> vals.add(String.valueOf(v == null ? "" : v).replace(",", " ")));
                sb.append(String.join(",", vals)).append("\n");
            }
        }
        return sb.toString();
    }

    private Map<String, Object> exportTaskRow(long taskId) {
        List<Map<String, Object>> rows = metaJdbc.queryForList(
                "SELECT t.*, c.subscriber FROM data_sub_export t JOIN data_contract c ON c.id=t.contract_id WHERE t.id=?", taskId);
        if (rows.isEmpty()) throw new BizException(ErrorCode.NOT_FOUND, "导出任务不存在");
        return rows.get(0);
    }

    private void requireExportAccess(long taskId, String token, boolean admin) {
        if (admin) return;
        Map<String, Object> t = exportTaskRow(taskId);
        Map<String, Object> c = getContract(((Number) t.get("contract_id")).longValue());
        if (token == null || !token.equals(s(c.get("token")))) throw new BizException(ErrorCode.FORBIDDEN, "X-Dcp-Token 无效");
    }

    /** 导出任务状态(订阅方轮询 / 管理查看) */
    public Map<String, Object> exportStatus(long taskId, String token, boolean admin) {
        requireExportAccess(taskId, token, admin);
        Map<String, Object> t = exportTaskRow(taskId);
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("taskId", taskId);
        out.put("format", s(t.get("format")));
        out.put("status", s(t.get("status")));
        out.put("rows", t.get("rows_count"));
        out.put("error", t.get("error_msg"));
        out.put("expireAt", s(t.get("expire_at")));
        out.put("downloadUrl", "done".equals(s(t.get("status"))) ? "/api/data-sub/export-task/" + taskId + "/download" : null);
        return out;
    }

    /** 导出文件下载(TTL 内; 权限=任务归属契约 token 或 admin) */
    public byte[] exportDownload(long taskId, String token, boolean admin) {
        requireExportAccess(taskId, token, admin);
        Map<String, Object> t = exportTaskRow(taskId);
        if (!"done".equals(s(t.get("status")))) throw new BizException(ErrorCode.PARAM_ERROR, "任务未完成(status=" + t.get("status") + ")");
        String expire = s(t.get("expire_at"));
        if (!expire.isBlank()) {
            String now = java.time.LocalDateTime.now().format(java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
            if (now.compareTo(expire) > 0) throw new BizException(ErrorCode.NOT_FOUND, "导出文件已过期(TTL 30min)");
        }
        try {
            return java.nio.file.Files.readAllBytes(EXPORT_DIR.resolve(s(t.get("file_name"))));
        } catch (Exception e) {
            throw new BizException(ErrorCode.NOT_FOUND, "导出文件缺失");
        }
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
