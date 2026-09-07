package com.dcp.module.tagengine;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Component;

import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.*;

/**
 * 打标引擎(V2)后端 —— P0 第一刀: 规则资产(规则管理 kind=rule / 规则流管理 kind=flow) + 打标任务 CRUD。
 *
 * <pre>
 * 模型(设计文档 2026-09-08 §3/§3b/§12):
 *   tag_rule   规则资产: kind=rule(定义 def_json: mode=visual|table|drl) | kind=flow(def_json.steps 引用其它资产)
 *              版本升版制; 删除守卫(被 flow 引用拦截); flow 保存环检测; 字段依赖自动收集(deps_json)
 *   tag_task   打标任务: 绑定规则/规则流(记录引用版本, 发布=固化快照), 执行序 = 规则先跑 → 规则流后跑
 *   P0 仅批(mode=batch); 流/在线 P1(创建即拦截提示)
 * </pre>
 */
@Slf4j
@Component
public class TagEngineService {

    private static final ObjectMapper OM = new ObjectMapper();

    private final JdbcTemplate metaJdbc;

    public TagEngineService(JdbcTemplate metaJdbc) {
        this.metaJdbc = metaJdbc;
    }

    @PostConstruct
    public void ensureSchema() {
        metaJdbc.execute("CREATE TABLE IF NOT EXISTS tag_rule ("
                + "id BIGINT AUTO_INCREMENT PRIMARY KEY,"
                + "name VARCHAR(128) NOT NULL,"
                + "kind VARCHAR(8) NOT NULL COMMENT 'rule(规则管理)|flow(规则流管理)',"
                + "ver INT NOT NULL DEFAULT 1,"
                + "status TINYINT NOT NULL DEFAULT 1 COMMENT '1启用|0停用',"
                + "descr VARCHAR(512),"
                + "def_json MEDIUMTEXT NOT NULL COMMENT 'visual|table|drl 或 flow steps 定义(JSON)',"
                + "deps_json TEXT NULL COMMENT '字段依赖(自动收集, JSON array)',"
                + "create_by VARCHAR(64),"
                + "create_time DATETIME DEFAULT CURRENT_TIMESTAMP,"
                + "update_time DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,"
                + "UNIQUE KEY uk_name_kind (name, kind))");
        metaJdbc.execute("CREATE TABLE IF NOT EXISTS tag_task ("
                + "id BIGINT AUTO_INCREMENT PRIMARY KEY,"
                + "name VARCHAR(128) NOT NULL,"
                + "mode VARCHAR(16) NOT NULL DEFAULT 'batch' COMMENT 'P0 仅 batch; stream/online P1',"
                + "dataset VARCHAR(128) NOT NULL COMMENT '数据集 = 逻辑表名(源=Iceberg 湖表, 湖落地由外部推进)',"
                + "derived_json TEXT NULL COMMENT '派生字段定义 [{name,expr}]',"
                + "bindings_json TEXT NOT NULL COMMENT '绑定规则快照列表 [{kind,id,ver,name}] 执行序',"
                + "schedule VARCHAR(16) NOT NULL DEFAULT 'none' COMMENT 'none|cron',"
                + "cron_expr VARCHAR(64),"
                + "guard_json TEXT NULL,"
                + "status VARCHAR(16) NOT NULL DEFAULT 'draft' COMMENT 'draft|published|stopped',"
                + "ver INT NOT NULL DEFAULT 1 COMMENT '任务版本(每次发布+1)',"
                + "snapshot_json TEXT NULL COMMENT '发布固化的规则版本快照',"
                + "create_by VARCHAR(64),"
                + "create_time DATETIME DEFAULT CURRENT_TIMESTAMP,"
                + "update_time DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,"
                + "UNIQUE KEY uk_name (name))");
        log.info("[tagengine] schema ensured: tag_rule / tag_task");
    }

    /* ═══════════════════ 内部工具 ═══════════════════ */

    private static String jstr(Object v) {
        try {
            return OM.writeValueAsString(v == null ? "" : v);
        } catch (Exception e) {
            throw new IllegalArgumentException("JSON 序列化失败: " + e.getMessage());
        }
    }

    private static JsonNode jnode(String json) {
        try {
            return OM.readTree(json == null || json.isBlank() ? "{}" : json);
        } catch (Exception e) {
            throw new IllegalArgumentException("规则定义不是合法 JSON");
        }
    }

    /** 请求体嵌套值可能是已序列化字符串或 Map/List 对象, 统一转 JsonNode */
    private static JsonNode toNode(Object v) {
        try {
            if (v == null) return OM.createObjectNode();
            if (v instanceof String s) return jnode(s);
            return OM.valueToTree(v);
        } catch (Exception e) {
            throw new IllegalArgumentException("规则定义不是合法 JSON");
        }
    }

    private Map<String, Object> ruleById(long id) {
        List<Map<String, Object>> rows = metaJdbc.queryForList(
                "SELECT id,name,kind,ver,status,descr,def_json,deps_json FROM tag_rule WHERE id=?", id);
        return rows.isEmpty() ? null : rows.get(0);
    }

    private List<Map<String, Object>> allRules() {
        return metaJdbc.queryForList(
                "SELECT id,name,kind,ver,status,descr,def_json,deps_json FROM tag_rule ORDER BY id");
    }

    /** 取一个资产的定义字段依赖(rule: 按 def.mode; flow: 递归引用) —— 入参为库内行 */
    private Set<String> depsOfRow(Map<String, Object> row, Set<Long> seen) {
        Set<String> out = new LinkedHashSet<>();
        long id = ((Number) row.get("id")).longValue();
        if (!seen.add(id)) return out;
        String kind = (String) row.get("kind");
        JsonNode def = jnode((String) row.get("def_json"));
        if ("flow".equals(kind)) {
            JsonNode steps = def.path("steps");
            for (JsonNode st : steps) {
                long refId = st.path("refId").asLong(0);
                if (refId <= 0) continue;
                Map<String, Object> ref = ruleById(refId);
                if (ref != null) out.addAll(depsOfRow(ref, seen));
            }
        } else {
            String mode = def.path("mode").asText("visual");
            if ("table".equals(mode)) {
                String f = def.path("table").path("field").asText("");
                if (!f.isEmpty()) out.add(f);
            } else if ("drl".equals(mode)) {
                /* DRL 手写: 依赖人工保证, 不自动收集 */
            } else {
                JsonNode when = def.path("visual").path("when");
                for (JsonNode g : when) {
                    JsonNode conds = g.path("conds");
                    for (JsonNode c : conds) {
                        String f = c.path("field").asText("");
                        if (!f.isEmpty()) out.add(f);
                    }
                }
            }
        }
        return out;
    }

    private Map<String, Object> rowToMap(Map<String, Object> row) {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("id", row.get("id"));
        out.put("name", row.get("name"));
        out.put("kind", row.get("kind"));
        out.put("ver", row.get("ver"));
        out.put("status", row.get("status"));
        out.put("descr", row.get("descr"));
        out.put("def", jnode((String) row.get("def_json")));
        try {
            out.put("deps", OM.readTree(row.get("deps_json") == null ? "[]" : (String) row.get("deps_json")));
        } catch (Exception e) {
            out.put("deps", new ArrayList<String>());
        }
        return out;
    }

    /** 删除守卫: 被哪些 flow 引用 */
    private List<String> usedByFlows(long id) {
        List<String> out = new ArrayList<>();
        for (Map<String, Object> r : allRules()) {
            if (!"flow".equals(r.get("kind"))) continue;
            JsonNode steps = jnode((String) r.get("def_json")).path("steps");
            for (JsonNode st : steps) {
                if (st.path("refId").asLong(0) == id) {
                    out.add((String) r.get("name"));
                    break;
                }
            }
        }
        return out;
    }

    private void assertStepsOk(long selfId, JsonNode def) {
        JsonNode steps = def.path("steps");
        if (!steps.isArray() || steps.isEmpty()) throw new IllegalArgumentException("规则流至少一个步骤");
        Map<Long, String> exists = new HashMap<>();
        for (Map<String, Object> r : allRules()) exists.put(((Number) r.get("id")).longValue(), (String) r.get("kind"));
        Set<Long> walkSeen = new HashSet<>();
        for (JsonNode st : steps) {
            long refId = st.path("refId").asLong(0);
            if (refId <= 0) throw new IllegalArgumentException("步骤缺少引用规则(refId)");
            String kind = exists.get(refId);
            if (kind == null) throw new IllegalArgumentException("步骤引用不存在的规则 id=" + refId);
            String hit = st.path("hit").asText("next");
            String miss = st.path("miss").asText("next");
            if (!Set.of("next", "stop", "jump").contains(hit)) throw new IllegalArgumentException("命中分支非法: " + hit);
            if (!Set.of("next", "end", "jump").contains(miss)) throw new IllegalArgumentException("未命中分支非法: " + miss);
            if (refId == selfId) throw new IllegalArgumentException("不能直接引用自身(成环)");
        }
        /* 环检测: 从每个新步骤引用出发 DFS, 若能回到 selfId 即成环(库里图无环, 新候选参与判定) */
        for (JsonNode st : steps) {
            long start = st.path("refId").asLong(0);
            walkSeen.clear();
            if (canReachSelf(start, selfId, walkSeen)) {
                throw new IllegalArgumentException("成环! 编排链不能直接/间接引用自身");
            }
        }
    }

    private boolean canReachSelf(long from, long selfId, Set<Long> seen) {
        if (!seen.add(from)) return false;
        if (from == selfId) return true;
        Map<String, Object> row = ruleById(from);
        if (row == null || !"flow".equals(row.get("kind"))) return false;
        JsonNode steps = jnode((String) row.get("def_json")).path("steps");
        for (JsonNode st : steps) {
            if (canReachSelf(st.path("refId").asLong(0), selfId, seen)) return true;
        }
        return false;
    }

    /** 结构校验 + 依赖收集(def 传入前先 json 化; 返回依赖集合) */
    private Set<String> validateDef(String kind, JsonNode def) {
        Set<String> deps = new LinkedHashSet<>();
        if ("flow".equals(kind)) {
            assertStepsOk(0, def);
            /* flow 依赖 = 各步骤引用资产的递归依赖(由库内行收集) */
            Set<Long> seen = new HashSet<>();
            JsonNode steps = def.path("steps");
            for (JsonNode st : steps) {
                long refId = st.path("refId").asLong(0);
                Map<String, Object> ref = ruleById(refId);
                if (ref != null) deps.addAll(depsOfRow(ref, seen));
            }
        } else if ("rule".equals(kind)) {
            String mode = def.path("mode").asText("");
            if (!Set.of("visual", "table", "drl").contains(mode)) {
                throw new IllegalArgumentException("规则定义方式须为 visual|table|drl");
            }
            if ("visual".equals(mode)) {
                JsonNode when = def.path("visual").path("when");
                JsonNode actions = def.path("visual").path("actions");
                boolean hasCond = !when.isArray() || when.isEmpty();
                if (when.isArray()) {
                    for (JsonNode g : when) {
                        JsonNode conds = g.path("conds");
                        for (JsonNode c : conds) {
                            if (!c.path("field").asText("").isEmpty()) { hasCond = true; deps.add(c.path("field").asText()); }
                        }
                    }
                }
                if (!actions.isArray() || actions.isEmpty()) throw new IllegalArgumentException("可视化规则缺 THEN 动作");
                for (JsonNode a : actions) {
                    String type = a.path("type").asText("tag");
                    if ("tag".equals(type) && a.path("tag").asText("").isEmpty()) {
                        throw new IllegalArgumentException("THEN 动作未选标签");
                    }
                }
                if (!hasCond) throw new IllegalArgumentException("可视化规则缺 WHEN 条件(或至少一个条件字段)");
            } else if ("table".equals(mode)) {
                JsonNode t = def.path("table");
                if (t.path("field").asText("").isEmpty() || t.path("tag").asText("").isEmpty()) {
                    throw new IllegalArgumentException("决策表缺分档字段或标签");
                }
                deps.add(t.path("field").asText());
                JsonNode rows = t.path("rows");
                boolean any = rows.isArray() && rows.size() > 0;
                if (!any && t.path("elseVal").asText("").isEmpty()) {
                    throw new IllegalArgumentException("决策表至少一个档位行或兜底值");
                }
            } else { /* drl */
                if (def.path("drl").asText("").isBlank()) throw new IllegalArgumentException("DRL 源码为空");
            }
        } else {
            throw new IllegalArgumentException("kind 须为 rule|flow");
        }
        return deps;
    }

    /* ═══════════════════ 规则/规则流 CRUD ═══════════════════ */

    public List<Map<String, Object>> listRules(String kind) {
        List<Map<String, Object>> out = new ArrayList<>();
        for (Map<String, Object> r : allRules()) {
            if (kind != null && !kind.isBlank() && !kind.equals(r.get("kind"))) continue;
            Map<String, Object> m = rowToMap(r);
            m.put("usedBy", usedByFlows(((Number) r.get("id")).longValue()));
            out.add(m);
        }
        return out;
    }

    public Map<String, Object> getRule(long id) {
        Map<String, Object> row = ruleById(id);
        if (row == null) throw new IllegalArgumentException("规则不存在 id=" + id);
        Map<String, Object> m = rowToMap(row);
        m.put("usedBy", usedByFlows(id));
        return m;
    }

    public Map<String, Object> createRule(Map<String, Object> body) {
        String name = str(body.get("name")).trim();
        String kind = str(body.get("kind"));
        if (name.isEmpty()) throw new IllegalArgumentException("规则名称必填");
        JsonNode def = toNode(body.get("def"));
        Set<String> deps = validateDef(kind, def);
        Integer cnt = metaJdbc.queryForObject("SELECT COUNT(*) FROM tag_rule WHERE name=? AND kind=?",
                Integer.class, name, kind);
        if (cnt != null && cnt > 0) throw new IllegalArgumentException("同名" + ("flow".equals(kind) ? "规则流" : "规则") + "已存在: " + name);
        KeyHolder kh = new GeneratedKeyHolder();
        metaJdbc.update(con -> {
            PreparedStatement ps = con.prepareStatement(
                    "INSERT INTO tag_rule(name,kind,ver,status,descr,def_json,deps_json) VALUES(?,?,1,?,?,?,?)",
                    Statement.RETURN_GENERATED_KEYS);
            ps.setString(1, name);
            ps.setString(2, kind);
            ps.setInt(3, ((Number) body.getOrDefault("status", 1)).intValue());
            ps.setString(4, str(body.get("descr")));
            ps.setString(5, jstr(def));
            ps.setString(6, jstr(deps));
            return ps;
        }, kh);
        Number id = kh.getKey();
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("id", id);
        out.put("name", name);
        out.put("kind", kind);
        out.put("ver", 1);
        out.put("deps", deps);
        log.info("[tagengine] created {} {}#{}", kind, name, out.get("id"));
        return out;
    }

    public Map<String, Object> updateRule(long id, Map<String, Object> body) {
        Map<String, Object> exist = ruleById(id);
        if (exist == null) throw new IllegalArgumentException("规则不存在 id=" + id);
        String name = str(body.getOrDefault("name", exist.get("name"))).trim();
        String kind = (String) exist.get("kind"); /* kind 不可改 */
        JsonNode def = toNode(body.get("def"));
        Set<String> deps = validateDef(kind, def);
        /* 编辑既有 flow 时的环检测(validateDef 传 selfId=0 只做结构; 这里对存在 id 重新做含自引用判定) */
        if ("flow".equals(kind)) assertStepsOk(id, def);
        Integer dup = metaJdbc.queryForObject(
                "SELECT COUNT(*) FROM tag_rule WHERE name=? AND kind=? AND id<>?", Integer.class, name, kind, id);
        if (dup != null && dup > 0) throw new IllegalArgumentException("同名" + ("flow".equals(kind) ? "规则流" : "规则") + "已存在: " + name);
        metaJdbc.update("UPDATE tag_rule SET name=?, status=?, descr=?, def_json=?, deps_json=?, ver=ver+1 WHERE id=?",
                name, ((Number) body.getOrDefault("status", exist.get("status"))).intValue(),
                str(body.getOrDefault("descr", exist.get("descr"))), jstr(def), jstr(deps), id);
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("id", id);
        out.put("name", name);
        out.put("kind", kind);
        out.put("ver", ((Number) exist.get("ver")).intValue() + 1);
        out.put("deps", deps);
        log.info("[tagengine] updated {} {} -> v{}", kind, name, out.get("ver"));
        return out;
    }

    public Map<String, Object> copyRule(long id) {
        Map<String, Object> exist = ruleById(id);
        if (exist == null) throw new IllegalArgumentException("规则不存在 id=" + id);
        String base = (String) exist.get("name");
        String name = base + "(副本)";
        for (int i = 2; ; i++) {
            Integer cnt = metaJdbc.queryForObject("SELECT COUNT(*) FROM tag_rule WHERE name=? AND kind=?",
                    Integer.class, name, exist.get("kind"));
            if (cnt == null || cnt == 0) break;
            name = base + "(副本" + i + ")";
        }
        final String finalName = name;
        KeyHolder kh = new GeneratedKeyHolder();
        metaJdbc.update(con -> {
            PreparedStatement ps = con.prepareStatement(
                    "INSERT INTO tag_rule(name,kind,ver,status,descr,def_json,deps_json) VALUES(?,?,1,1,?,?,?)",
                    Statement.RETURN_GENERATED_KEYS);
            ps.setString(1, finalName);
            ps.setString(2, (String) exist.get("kind"));
            ps.setString(3, (String) exist.get("descr"));
            ps.setString(4, (String) exist.get("def_json"));
            ps.setString(5, (String) exist.get("deps_json"));
            return ps;
        }, kh);
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("id", kh.getKey());
        out.put("name", name);
        out.put("kind", exist.get("kind"));
        out.put("ver", 1);
        return out;
    }

    public void deleteRule(long id) {
        Map<String, Object> exist = ruleById(id);
        if (exist == null) throw new IllegalArgumentException("规则不存在 id=" + id);
        List<String> usedBy = usedByFlows(id);
        if (!usedBy.isEmpty()) throw new IllegalArgumentException("被规则流引用, 请先在引用处移除: " + String.join("、", usedBy));
        metaJdbc.update("DELETE FROM tag_rule WHERE id=?", id);
        log.info("[tagengine] deleted {} {}", exist.get("kind"), exist.get("name"));
    }

    /* ═══════════════════ 打标任务 ═══════════════════ */

    public List<Map<String, Object>> listTasks() {
        List<Map<String, Object>> rows = metaJdbc.queryForList(
                "SELECT id,name,mode,dataset,derived_json,bindings_json,schedule,cron_expr,guard_json,status,ver,snapshot_json,create_time,update_time"
                        + " FROM tag_task ORDER BY id DESC");
        List<Map<String, Object>> out = new ArrayList<>();
        for (Map<String, Object> r : rows) out.add(taskRowToMap(r));
        return out;
    }

    public Map<String, Object> getTask(long id) {
        List<Map<String, Object>> rows = metaJdbc.queryForList(
                "SELECT id,name,mode,dataset,derived_json,bindings_json,schedule,cron_expr,guard_json,status,ver,snapshot_json,create_time,update_time"
                        + " FROM tag_task WHERE id=?", id);
        if (rows.isEmpty()) throw new IllegalArgumentException("任务不存在 id=" + id);
        return taskRowToMap(rows.get(0));
    }

    private Map<String, Object> taskRowToMap(Map<String, Object> r) {
        Map<String, Object> out = new LinkedHashMap<>(r);
        out.put("derived", jnode((String) r.get("derived_json")));
        out.put("bindings", jnode((String) r.get("bindings_json")));
        out.put("guard", jnode((String) r.get("guard_json")));
        out.put("snapshot", r.get("snapshot_json") == null ? null : jnode((String) r.get("snapshot_json")));
        out.remove("derived_json");
        out.remove("bindings_json");
        out.remove("guard_json");
        out.remove("snapshot_json");
        return out;
    }

    private void assertBindingsOk(JsonNode bindings) {
        if (!bindings.isArray() || bindings.isEmpty()) throw new IllegalArgumentException("至少绑定一条规则或规则流");
        int ruleCnt = 0, flowCnt = 0;
        for (JsonNode b : bindings) {
            long refId = b.path("id").asLong(0);
            Map<String, Object> exist = refId > 0 ? ruleById(refId) : null;
            if (exist == null) throw new IllegalArgumentException("绑定的规则不存在 id=" + refId);
            String kind = (String) exist.get("kind");
            if ("rule".equals(kind)) ruleCnt++; else flowCnt++;
        }
        if (ruleCnt == 0 && flowCnt == 0) throw new IllegalArgumentException("至少绑定一条规则或规则流");
    }

    public Map<String, Object> createTask(Map<String, Object> body) {
        String name = str(body.get("name")).trim();
        String mode = str(body.getOrDefault("mode", "batch"));
        String dataset = str(body.get("dataset"));
        if (name.isEmpty()) throw new IllegalArgumentException("任务名称必填");
        if (!"batch".equals(mode)) throw new IllegalArgumentException("P0 仅支持批模式(batch); 流/在线 P1 开放");
        if (dataset.isEmpty()) throw new IllegalArgumentException("数据集(逻辑表)必填");
        JsonNode bindings = toNode(body.get("bindings"));
        assertBindingsOk(bindings);
        Integer cnt = metaJdbc.queryForObject("SELECT COUNT(*) FROM tag_task WHERE name=?", Integer.class, name);
        if (cnt != null && cnt > 0) throw new IllegalArgumentException("任务名已存在: " + name);
        String derived = jstr(body.getOrDefault("derived", new ArrayList<Object>()));
        String guard = jstr(body.getOrDefault("guard", new LinkedHashMap<String, Object>()));
        String sched = str(body.getOrDefault("schedule", "none"));
        String cron = str(body.get("cronExpr"));
        if ("cron".equals(sched) && cron.isEmpty()) throw new IllegalArgumentException("定时任务需 cron 表达式");
        KeyHolder kh = new GeneratedKeyHolder();
        metaJdbc.update(con -> {
            PreparedStatement ps = con.prepareStatement(
                    "INSERT INTO tag_task(name,mode,dataset,derived_json,bindings_json,schedule,cron_expr,guard_json) VALUES(?,?,?,?,?,?,?,?)",
                    Statement.RETURN_GENERATED_KEYS);
            ps.setString(1, name);
            ps.setString(2, mode);
            ps.setString(3, dataset);
            ps.setString(4, derived);
            ps.setString(5, jstr(bindings));
            ps.setString(6, sched);
            ps.setString(7, cron.isEmpty() ? null : cron);
            ps.setString(8, guard);
            return ps;
        }, kh);
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("id", kh.getKey());
        out.put("name", name);
        out.put("mode", mode);
        out.put("status", "draft");
        out.put("ver", 1);
        log.info("[tagengine] task created {}#{}", name, out.get("id"));
        return out;
    }

    public Map<String, Object> updateTask(long id, Map<String, Object> body) {
        Map<String, Object> exist = getTask(id);
        String name = str(body.getOrDefault("name", exist.get("name"))).trim();
        String mode = str(body.getOrDefault("mode", exist.get("mode")));
        String dataset = str(body.getOrDefault("dataset", exist.get("dataset")));
        if (!"batch".equals(mode)) throw new IllegalArgumentException("P0 仅支持批模式(batch); 流/在线 P1 开放");
        JsonNode bindings = toNode(body.get("bindings"));
        assertBindingsOk(bindings);
        Integer dup = metaJdbc.queryForObject("SELECT COUNT(*) FROM tag_task WHERE name=? AND id<>?", Integer.class, name, id);
        if (dup != null && dup > 0) throw new IllegalArgumentException("任务名已存在: " + name);
        String sched = str(body.getOrDefault("schedule", exist.get("schedule")));
        String cron = str(body.getOrDefault("cronExpr", exist.get("cronExpr")));
        if ("cron".equals(sched) && cron.isEmpty()) throw new IllegalArgumentException("定时任务需 cron 表达式");
        String status = str(body.getOrDefault("status", exist.get("status")));
        metaJdbc.update(
                "UPDATE tag_task SET name=?, dataset=?, derived_json=?, bindings_json=?, schedule=?, cron_expr=?, guard_json=?, status=? WHERE id=?",
                name, dataset,
                jstr(body.getOrDefault("derived", exist.get("derived"))),
                jstr(bindings), sched, cron.isEmpty() ? null : cron,
                jstr(body.getOrDefault("guard", exist.get("guard"))),
                status, id);
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("id", id);
        out.put("name", name);
        out.put("status", status);
        return out;
    }

    public void deleteTask(long id) {
        getTask(id); /* 不存在即抛 */
        metaJdbc.update("DELETE FROM tag_task WHERE id=?", id);
        log.info("[tagengine] task deleted {}", id);
    }

    /** 发布: 校验绑定规则当前均启用 → 固化版本快照(ver+1)。DRL/字段依赖编译校验在下一刀接入。 */
    public Map<String, Object> publishTask(long id) {
        Map<String, Object> exist = getTask(id);
        JsonNode bindings = (JsonNode) exist.get("bindings");
        ArrayNode snap = OM.createArrayNode();
        StringBuilder conflict = new StringBuilder();
        for (JsonNode b : bindings) {
            long refId = b.path("id").asLong(0);
            Map<String, Object> rule = ruleById(refId);
            if (rule == null) throw new IllegalArgumentException("绑定规则已被删除 id=" + refId);
            int st = ((Number) rule.get("status")).intValue();
            if (st != 1) throw new IllegalArgumentException("绑定规则已停用: " + rule.get("name"));
            snap.addObject()
                    .put("kind", (String) rule.get("kind"))
                    .put("id", refId)
                    .put("ver", ((Number) rule.get("ver")).intValue())
                    .put("name", (String) rule.get("name"));
        }
        int newVer = ((Number) exist.get("ver")).intValue() + 1;
        metaJdbc.update("UPDATE tag_task SET status='published', ver=?, snapshot_json=? WHERE id=?",
                newVer, jstr(snap), id);
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("id", id);
        out.put("status", "published");
        out.put("ver", newVer);
        out.put("snapshot", snap);
        log.info("[tagengine] task {} published v{} (P0: 快照固化; DRL/字段编译校验后续刀接入)", id, newVer);
        return out;
    }

    private static String str(Object o) {
        return o == null ? "" : String.valueOf(o);
    }
}
