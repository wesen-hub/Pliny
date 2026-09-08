package com.dcp.module.tagengine;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
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

    @Value("${dcp.tagengine.runner-mode:simulate}")
    private String runnerMode;          // simulate(演练: 生成 task-json/命令不下发) | spark(真实 spark-submit)
    @Value("${dcp.tagengine.executor-jar:}")
    private String executorJar;         // 执行器 jar 绝对路径(容器内/spark 节点可见)
    @Value("${dcp.tagengine.spark-submit:spark-submit}")
    private String sparkSubmitBin;
    @Value("${dcp.tagengine.runs-dir:/tmp/dcp-tagengine}")
    private String runsDir;
    @Value("${dcp.tagengine.auto-run:false}")
    private boolean autoRun;            // cron 自动触发开关(真实环境才开, 避免演练噪音)

    private final java.util.concurrent.ExecutorService sparkRuns =
            java.util.concurrent.Executors.newFixedThreadPool(2, r -> {
                Thread t = new Thread(r, "tagengine-spark-run");
                t.setDaemon(true);
                return t;
            });

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
                + "schedule VARCHAR(16) NOT NULL DEFAULT 'none' COMMENT 'none|interval|cron',"
                + "interval_min INT NOT NULL DEFAULT 15 COMMENT 'interval 周期(分钟), schedule=interval 时生效',"
                + "cron_expr VARCHAR(64),"
                + "guard_json TEXT NULL,"
                + "status VARCHAR(16) NOT NULL DEFAULT 'draft' COMMENT 'draft|published|stopped',"
                + "ver INT NOT NULL DEFAULT 1 COMMENT '任务版本(每次发布+1)',"
                + "snapshot_json TEXT NULL COMMENT '发布固化的规则版本快照',"
                + "create_by VARCHAR(64),"
                + "create_time DATETIME DEFAULT CURRENT_TIMESTAMP,"
                + "update_time DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,"
                + "UNIQUE KEY uk_name (name))");
        /* 数据集字段清单(逻辑表字段快照; 来源: 前端/逻辑表元数据 API, 用于发布依赖域校验) */
        try {
            metaJdbc.execute("ALTER TABLE tag_task ADD COLUMN dataset_fields_json TEXT NULL AFTER dataset");
        } catch (DataAccessException ignored) {
            /* 已存在 */
        }
        try {
            metaJdbc.execute("ALTER TABLE tag_task ADD COLUMN entity_key VARCHAR(64) NOT NULL DEFAULT 'id' AFTER dataset");
        } catch (DataAccessException ignored) {
            /* 已存在 */
        }
        try {
            metaJdbc.execute("ALTER TABLE tag_task ADD COLUMN last_auto_run_at DATETIME NULL");
        } catch (DataAccessException ignored) {
            /* 已存在 */
        }
        try {
            metaJdbc.execute("ALTER TABLE tag_task ADD COLUMN cursor_json TEXT NULL COMMENT '增量游标(湖表行级 seq): NULL=全量; {seqColumn:maxSeq}'");
        } catch (DataAccessException ignored) {
            /* 已存在 */
        }
        try {
            metaJdbc.execute("ALTER TABLE tag_task ADD COLUMN interval_min INT NOT NULL DEFAULT 15 COMMENT 'interval 周期(分钟), schedule=interval 时生效'");
        } catch (DataAccessException ignored) {
            /* 已存在 */
        }
        metaJdbc.execute("CREATE TABLE IF NOT EXISTS tag_run ("
                + "id BIGINT AUTO_INCREMENT PRIMARY KEY,"
                + "task_id BIGINT NOT NULL,"
                + "task_name VARCHAR(128),"
                + "trigger_type VARCHAR(16) NOT NULL DEFAULT 'manual' COMMENT 'manual|cron',"
                + "status VARCHAR(16) NOT NULL DEFAULT 'submitted' COMMENT 'submitted|running|succeeded|failed',"
                + "payload_path VARCHAR(512),"
                + "submit_cmd VARCHAR(1024),"
                + "exit_code INT NULL,"
                + "summary_json TEXT NULL COMMENT 'run 摘要(总行数/标签行/每资产命中), 回写待接',"
                + "matrix_json TEXT NULL COMMENT '试跑命中矩阵(执行器 --matrix-out 回写)',"
                + "cursor_after TEXT NULL COMMENT '增量轮 exit0 后游标(cursor-out 内容)',"
                + "error_msg VARCHAR(2048),"
                + "started_at DATETIME DEFAULT CURRENT_TIMESTAMP,"
                + "finished_at DATETIME NULL,"
                + "KEY idx_task (task_id))");
        try {
            metaJdbc.execute("ALTER TABLE tag_run ADD COLUMN matrix_json TEXT NULL");
        } catch (DataAccessException ignored) { /* 已存在 */ }
        try {
            metaJdbc.execute("ALTER TABLE tag_run ADD COLUMN cursor_after TEXT NULL");
        } catch (DataAccessException ignored) { /* 已存在 */ }
        log.info("[tagengine] schema ensured: tag_rule / tag_task / tag_run");
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
                "SELECT id,name,mode,dataset,entity_key,last_auto_run_at,dataset_fields_json,derived_json,bindings_json,schedule,interval_min,cron_expr,guard_json,status,ver,snapshot_json,cursor_json,create_time,update_time"
                        + " FROM tag_task ORDER BY id DESC");
        List<Map<String, Object>> out = new ArrayList<>();
        for (Map<String, Object> r : rows) out.add(taskRowToMap(r));
        return out;
    }

    public Map<String, Object> getTask(long id) {
        List<Map<String, Object>> rows = metaJdbc.queryForList(
                "SELECT id,name,mode,dataset,entity_key,last_auto_run_at,dataset_fields_json,derived_json,bindings_json,schedule,interval_min,cron_expr,guard_json,status,ver,snapshot_json,cursor_json,create_time,update_time"
                        + " FROM tag_task WHERE id=?", id);
        if (rows.isEmpty()) throw new IllegalArgumentException("任务不存在 id=" + id);
        return taskRowToMap(rows.get(0));
    }

    private Map<String, Object> taskRowToMap(Map<String, Object> r) {
        Map<String, Object> out = new LinkedHashMap<>(r);
        out.put("entityKey", out.remove("entity_key"));
        out.put("intervalMin", out.remove("interval_min"));
        out.put("derived", jnode((String) r.get("derived_json")));
        out.put("bindings", jnode((String) r.get("bindings_json")));
        out.put("datasetFields", jnode((String) r.get("dataset_fields_json")));
        out.put("guard", jnode((String) r.get("guard_json")));
        out.put("snapshot", r.get("snapshot_json") == null ? null : jnode((String) r.get("snapshot_json")));
        out.put("cursor", r.get("cursor_json") == null ? null : jnode((String) r.get("cursor_json")));
        out.remove("derived_json");
        out.remove("bindings_json");
        out.remove("dataset_fields_json");
        out.remove("guard_json");
        out.remove("snapshot_json");
        out.remove("cursor_json");
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
        if (!"batch".equals(mode) && !"online".equals(mode)) throw new IllegalArgumentException("模式仅支持 batch|online(P0 批 + P1 在线; 流 stream 待 Flink 执行器)");
        if (dataset.isEmpty()) throw new IllegalArgumentException("数据集(逻辑表)必填");
        String rawKey = str(body.getOrDefault("entityKey", "id")).trim();
        final String entityKey = rawKey.isEmpty() ? "id" : rawKey;
        JsonNode bindings = toNode(body.get("bindings"));
        assertBindingsOk(bindings);
        Integer cnt = metaJdbc.queryForObject("SELECT COUNT(*) FROM tag_task WHERE name=?", Integer.class, name);
        if (cnt != null && cnt > 0) throw new IllegalArgumentException("任务名已存在: " + name);
        String derived = jstr(body.getOrDefault("derived", new ArrayList<Object>()));
        String dsFields = jstr(body.getOrDefault("datasetFields", new ArrayList<Object>()));
        String guard = jstr(body.getOrDefault("guard", new LinkedHashMap<String, Object>()));
        String sched = str(body.getOrDefault("schedule", "none"));
        String cron = str(body.get("cronExpr"));
        int intervalMin = body.get("intervalMin") instanceof Number n ? n.intValue() : 15;
        if ("cron".equals(sched) && cron.isEmpty()) throw new IllegalArgumentException("定时任务需 cron 表达式");
        if ("interval".equals(sched) && intervalMin <= 0) throw new IllegalArgumentException("interval 任务需 intervalMin>0");
        KeyHolder kh = new GeneratedKeyHolder();
        metaJdbc.update(con -> {
            PreparedStatement ps = con.prepareStatement(
                    "INSERT INTO tag_task(name,mode,dataset,entity_key,dataset_fields_json,derived_json,bindings_json,schedule,interval_min,cron_expr,guard_json) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                    Statement.RETURN_GENERATED_KEYS);
            ps.setString(1, name);
            ps.setString(2, mode);
            ps.setString(3, dataset);
            ps.setString(4, entityKey);
            ps.setString(5, dsFields);
            ps.setString(6, derived);
            ps.setString(7, jstr(bindings));
            ps.setString(8, sched);
            ps.setInt(9, intervalMin);
            ps.setString(10, cron.isEmpty() ? null : cron);
            ps.setString(11, guard);
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
        if (!"batch".equals(mode) && !"online".equals(mode)) throw new IllegalArgumentException("模式仅支持 batch|online(P0 批 + P1 在线; 流 stream 待 Flink 执行器)");
        String entityKey = str(body.getOrDefault("entityKey", exist.get("entityKey"))).trim();
        if (entityKey.isEmpty()) entityKey = "id";
        JsonNode bindings = toNode(body.get("bindings"));
        assertBindingsOk(bindings);
        Integer dup = metaJdbc.queryForObject("SELECT COUNT(*) FROM tag_task WHERE name=? AND id<>?", Integer.class, name, id);
        if (dup != null && dup > 0) throw new IllegalArgumentException("任务名已存在: " + name);
        String sched = str(body.getOrDefault("schedule", exist.get("schedule")));
        String cron = str(body.getOrDefault("cronExpr", exist.get("cronExpr")));
        int intervalMin = body.get("intervalMin") instanceof Number n ? n.intValue()
                : (exist.get("intervalMin") instanceof Number m ? m.intValue() : 15);
        if ("cron".equals(sched) && cron.isEmpty()) throw new IllegalArgumentException("定时任务需 cron 表达式");
        if ("interval".equals(sched) && intervalMin <= 0) throw new IllegalArgumentException("interval 任务需 intervalMin>0");
        String status = str(body.getOrDefault("status", exist.get("status")));
        metaJdbc.update(
                "UPDATE tag_task SET name=?, dataset=?, entity_key=?, dataset_fields_json=?, derived_json=?, bindings_json=?, schedule=?, interval_min=?, cron_expr=?, guard_json=?, status=? WHERE id=?",
                name, dataset, entityKey,
                jstr(body.getOrDefault("datasetFields", exist.get("datasetFields"))),
                jstr(body.getOrDefault("derived", exist.get("derived"))),
                jstr(bindings), sched, intervalMin, cron.isEmpty() ? null : cron,
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

    /** 发布: 校验绑定规则均启用 + 字段依赖 ⊆ 任务字段域(数据集字段 ∪ 派生字段) → 固化版本快照(ver+1)。
     *  快照含资产定义(规则库升版后旧版定义靠快照还原执行), 供执行器生成 task-json。
     *  注: DRL 的类型级编译校验落在 Spark 执行器(driver 侧 schema 可知), 发布阶段以字段依赖域校验为主。 */
    public Map<String, Object> publishTask(long id) {
        Map<String, Object> exist = getTask(id);
        JsonNode bindings = (JsonNode) exist.get("bindings");
        // 有序收录: 绑定资产 → 规则流步骤引用的资产(递归闭包, 含嵌套流), 快照自含执行所需全部定义
        List<Map<String, Object>> ordered = new ArrayList<>();
        Map<Long, Integer> seenIdx = new HashMap<>();
        List<Map<String, Object>> binds = new ArrayList<>();
        Set<String> missing = new LinkedHashSet<>();
        Set<Long> seenRules = new HashSet<>();
        for (JsonNode b : bindings) {
            long refId = b.path("id").asLong(0);
            Map<String, Object> rule = ruleById(refId);
            if (rule == null) throw new IllegalArgumentException("绑定规则已被删除 id=" + refId);
            if (((Number) rule.get("status")).intValue() != 1) {
                throw new IllegalArgumentException("绑定规则已停用: " + rule.get("name"));
            }
            binds.add(rule);
            if (!seenIdx.containsKey(refId)) {
                seenIdx.put(refId, ordered.size());
                ordered.add(rule);
            }
        }
        // 闭包展开(顺序稳定: 绑定在前, 引用随后; 防御环 seen 已挡, 双保险深度上限)
        for (int pos = 0; pos < ordered.size() && pos < 512; pos++) {
            Map<String, Object> r = ordered.get(pos);
            if (!"flow".equals(r.get("kind"))) continue;
            JsonNode steps = jnode((String) r.get("def_json")).path("steps");
            for (JsonNode st : steps) {
                long refId = st.path("refId").asLong(0);
                if (refId <= 0 || seenIdx.containsKey(refId)) continue;
                Map<String, Object> ref = ruleById(refId);
                if (ref != null) {
                    seenIdx.put(refId, ordered.size());
                    ordered.add(ref);
                }
            }
        }
        if (binds.isEmpty() && ordered.isEmpty()) throw new IllegalArgumentException("任务未绑定任何规则/规则流");
        ObjectNode snap = OM.createObjectNode();
        ArrayNode bindArr = snap.putArray("bindings");
        ArrayNode assetArr = snap.putArray("assets");
        for (Map<String, Object> rb : binds) {
            long refId = ((Number) rb.get("id")).longValue();
            bindArr.addObject().put("kind", (String) rb.get("kind")).put("id", refId)
                    .put("ver", ((Number) rb.get("ver")).intValue()).put("name", (String) rb.get("name"));
        }
        for (Map<String, Object> r : ordered) {
            ObjectNode ab = assetArr.addObject()
                    .put("kind", (String) r.get("kind"))
                    .put("id", ((Number) r.get("id")).longValue())
                    .put("ver", ((Number) r.get("ver")).intValue())
                    .put("name", (String) r.get("name"));
            ab.set("def", jnode((String) r.get("def_json")));   // 固化资产定义(按当前版本冻结)
            collectDepsOf(r, seenRules, missing);
        }
        /* 字段域 = 数据集字段 ∪ 派生字段名 */
        Set<String> universe = new HashSet<>();
        JsonNode ds = (JsonNode) exist.get("datasetFields");
        if (ds != null && ds.isArray()) ds.forEach(x -> { if (!x.asText("").isEmpty()) universe.add(x.asText()); });
        JsonNode der = (JsonNode) exist.get("derived");
        if (der != null && der.isArray()) der.forEach(x -> { if (!x.path("name").asText("").isEmpty()) universe.add(x.path("name").asText()); });
        if (!missing.isEmpty()) {
            List<String> unknown = missing.stream().filter(f -> !universe.contains(f)).toList();
            if (!unknown.isEmpty()) {
                throw new IllegalArgumentException("字段依赖超出任务字段域: " + String.join("、", unknown)
                        + " —— 请补数据集字段清单(datasetFields)或在派生字段中定义(DRL 规则的引用字段无法自动收集, 请人工核对)");
            }
        }
        int newVer = ((Number) exist.get("ver")).intValue() + 1;
        checkPublishTags(id, ordered, str(exist.get("mode")));
        metaJdbc.update("UPDATE tag_task SET status='published', ver=?, snapshot_json=? WHERE id=?",
                newVer, jstr(snap), id);
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("id", id);
        out.put("status", "published");
        out.put("ver", newVer);
        out.put("snapshot", snap);
        log.info("[tagengine] task {} published v{} (快照固化 + 字段依赖域校验; DRL 类型编译在执行器)", id, newVer);
        return out;
    }

    /** datasetFields: 前端存字符串数组, 执行器契约需 {name} 对象 → 包装 */
    private static JsonNode wrapFields(JsonNode raw) {
        ArrayNode out = OM.createArrayNode();
        if (raw != null && raw.isArray()) {
            for (JsonNode f : raw) {
                if (f.isTextual()) out.addObject().put("name", f.asText());
                else out.add(f);
            }
        }
        return out;
    }

    /* ═══════════ 平台→执行器接线(P0): task-json 生成 + 提交(演练/spark) + tag_run 审计 + cron 自动触发 ═══════════ */

    private static String env(String key, String def) {
        String v = System.getenv(key);
        return v == null || v.isBlank() ? def : v;
    }

    /** 触发一次批运行: 生成 task-json(发布快照自含资产定义) → 落 runs 目录 → 记 tag_run → 按 runner-mode 演练/提交 */
    public Map<String, Object> startRun(long taskId, String trigger) {
        return startRun(taskId, trigger, false, false);
    }

    /** full=true: 全量重算(忽略任务游标, 不带 incremental 段; 跑完由执行器输出全表 maxSeq 重新初始化游标) */
    public Map<String, Object> startRun(long taskId, String trigger, boolean full) {
        return startRun(taskId, trigger, full, false);
    }

    /** matrix=true: 试跑模式(执行器 --matrix --limit 1000: 采样求值出命中矩阵, 不写 tag_fact) */
    public Map<String, Object> startRun(long taskId, String trigger, boolean full, boolean matrix) {
        Map<String, Object> task = getTask(taskId);
        if (!"published".equals(task.get("status"))) throw new IllegalArgumentException("任务未发布, 请先 publish");
        JsonNode snap = (JsonNode) task.get("snapshot");
        if (snap == null || !snap.has("assets") || !snap.get("assets").isArray() || snap.get("assets").isEmpty()) {
            throw new IllegalArgumentException("任务快照为旧版(缺资产定义), 请重新发布");
        }
        ObjectNode payload = OM.createObjectNode();
        payload.put("taskId", taskId);
        payload.put("taskVer", ((Number) task.get("ver")).intValue());
        payload.put("dataset", (String) task.get("dataset"));
        payload.put("entityKey", str(task.get("entityKey")));
        payload.set("iceberg", OM.createObjectNode()
                .put("catalog", env("DCP_TAGENGINE_CATALOG", "demo"))
                .put("namespace", env("DCP_TAGENGINE_NAMESPACE", "dcp"))
                .put("table", (String) task.get("dataset")));
        payload.set("tagFact", OM.createObjectNode()
                .put("catalog", env("DCP_TAGENGINE_CATALOG", "demo"))
                .put("namespace", env("DCP_TAGENGINE_NAMESPACE", "dcp"))
                .put("table", env("DCP_TAGENGINE_TAGFACT_TABLE", "tag_fact")));
        payload.set("datasetFields", wrapFields((JsonNode) task.get("datasetFields")));
        payload.set("derived", (JsonNode) task.get("derived"));
        payload.set("bindings", snap.get("bindings"));
        payload.set("rules", snap.get("assets"));   // 执行器契约键名 rules(资产快照自含 def)
        // 增量配置(§14): 任务游标非空且非 full → 增量轮(仅重算变更实体); full=1 → 全量(跑完重新初始化游标); matrix 试跑不走增量
        JsonNode cursorNode = (JsonNode) task.get("cursor");
        long cursor = (!full && !matrix && cursorNode != null && cursorNode.has("maxSeq")) ? cursorNode.path("maxSeq").asLong(-1) : -1;
        if (cursor >= 0) {
            ObjectNode incr = payload.putObject("incremental");
            incr.put("cursor", cursor);
            if (cursorNode != null && cursorNode.has("seqColumn")) incr.put("seqColumn", cursorNode.path("seqColumn").asText());
            if (cursorNode != null && cursorNode.has("tombstoneColumn")) incr.put("tombstoneColumn", cursorNode.path("tombstoneColumn").asText());
        }
        payload.put("icebergConfig", "湖参数待接湖环境(设计 §13): catalog/namespace 现为占位, 湖落地后平台配置下发");
        String json = jstr(payload);

        // 落 runs 目录 + tag_run 记录
        try {
            Path dir = Paths.get(runsDir);
            Files.createDirectories(dir);
            KeyHolder kh = new GeneratedKeyHolder();
            metaJdbc.update(con -> {
                PreparedStatement ps = con.prepareStatement(
                        "INSERT INTO tag_run(task_id,task_name,trigger_type,status) VALUES(?,?,?,'submitted')",
                        Statement.RETURN_GENERATED_KEYS);
                ps.setLong(1, taskId);
                ps.setString(2, (String) task.get("name"));
                ps.setString(3, trigger == null || trigger.isBlank() ? "manual" : trigger);
                return ps;
            }, kh);
            long runId = ((Number) kh.getKey()).longValue();
            Path file = dir.resolve("task-" + taskId + "-run-" + runId + ".json");
            Files.writeString(file, json, StandardCharsets.UTF_8);

            // 提交命令(演练模式不真正执行; spark 模式尝试启动, 失败置 failed)
            String submitCmd = "(未配置)";
            String status = "submitted";
            String err = null;
            if ("spark".equalsIgnoreCase(runnerMode) && !executorJar.isBlank()) {
                Path cursorOut = dir.resolve("task-" + taskId + "-run-" + runId + ".cursor.json");
                submitCmd = sparkSubmitBin + " --class com.dcp.tagjob.TagBatchJob "
                        + env("DCP_TAGENGINE_SPARK_MASTER", "local[*]") + " --task-json " + file;
                if (matrix) {
                    submitCmd += " --matrix --matrix-out " + dir.resolve("task-" + taskId + "-run-" + runId + ".matrix.json");
                } else {
                    submitCmd += " --cursor-out " + cursorOut;
                }
                try {
                    Process p = new ProcessBuilder(submitCmd.split(" ")).redirectErrorStream(true).start();
                    status = "running";
                    log.info("[tagengine] run#{} spark-submit 已启动 pid={}, 异步等待 exit0 回写", runId, p.pid());
                    sparkRuns.submit(() -> awaitSpark(runId, taskId, p, cursorOut));
                } catch (IOException ex) {
                    status = "failed";
                    err = "spark-submit 不可用: " + ex.getMessage();
                    log.warn("[tagengine] run#{} 提交失败: {}", runId, err);
                }
            } else {
                submitCmd = "演练(simulate): java -cp " + executorJar + " com.dcp.tagjob.TagBatchJob --task-json " + file
                        + (matrix ? " --matrix --limit 1000" : "")
                        + " | runner-mode=" + runnerMode + "(spark 模式 + executor-jar 配置后自动切换真实提交)";
                log.info("[tagengine] run#{} 演练{}: task-json 已生成 {}", runId, matrix ? "(试跑 matrix)" : "", file);
            }
            metaJdbc.update("UPDATE tag_run SET status=?, payload_path=?, submit_cmd=?, error_msg=? WHERE id=?",
                    status, file.toString(), submitCmd, err, runId);
            Map<String, Object> out = new LinkedHashMap<>();
            out.put("runId", runId);
            out.put("taskId", taskId);
            out.put("status", status);
            out.put("trigger", trigger == null ? "manual" : trigger);
            out.put("payloadPath", file.toString());
            out.put("submitCmd", submitCmd);
            out.put("payload", payload);
            return out;
        } catch (java.io.IOException e) {
            throw new IllegalArgumentException("runs 目录写入失败: " + e.getMessage());
        }
    }

    public List<Map<String, Object>> listRuns(Long taskId, int limit) {
        String sql = "SELECT id,task_id,task_name,trigger_type,status,payload_path,submit_cmd,exit_code,summary_json,matrix_json,cursor_after,error_msg,started_at,finished_at FROM tag_run";
        List<Object> args = new ArrayList<>();
        if (taskId != null) {
            sql += " WHERE task_id=?";
            args.add(taskId);
        }
        sql += " ORDER BY id DESC LIMIT ?";
        args.add(Math.min(Math.max(limit, 1), 200));
        List<Map<String, Object>> rows = metaJdbc.queryForList(sql, args.toArray());
        List<Map<String, Object>> out = new ArrayList<>();
        for (Map<String, Object> r : rows) {
            Map<String, Object> m = new LinkedHashMap<>(r);
            m.put("summary", r.get("summary_json") == null ? null : jnode((String) r.get("summary_json")));
            m.remove("summary_json");
            m.put("matrix", r.get("matrix_json") == null ? null : jnode((String) r.get("matrix_json")));
            m.remove("matrix_json");
            out.add(m);
        }
        return out;
    }

    public Map<String, Object> getRun(long runId) {
        List<Map<String, Object>> rows = metaJdbc.queryForList(
                "SELECT id,task_id,task_name,trigger_type,status,payload_path,submit_cmd,exit_code,summary_json,matrix_json,cursor_after,error_msg,started_at,finished_at FROM tag_run WHERE id=?",
                runId);
        if (rows.isEmpty()) throw new IllegalArgumentException("run 不存在 id=" + runId);
        Map<String, Object> m = new LinkedHashMap<>(rows.get(0));
        m.put("summary", m.get("summary_json") == null ? null : jnode((String) m.get("summary_json")));
        m.remove("summary_json");
        m.put("matrix", m.get("matrix_json") == null ? null : jnode((String) m.get("matrix_json")));
        m.remove("matrix_json");
        String path = (String) m.get("payload_path");
        if (path != null) {
            try {
                String txt = Files.readString(Paths.get(path), StandardCharsets.UTF_8);
                if (txt.length() > 300000) txt = txt.substring(0, 300000) + "...(截断)";
                m.put("payloadText", txt);
            } catch (IOException ignore) {
                m.put("payloadText", null);
            }
        }
        return m;
    }

    /** 执行器回写 run 结果(状态/exit/summary/错误); 增量轮带 cursor{seqColumn,maxSeq} → 成功后回写任务游标(§14.3) */
    public Map<String, Object> finishRun(long runId, Map<String, Object> body) {
        Map<String, Object> run = getRun(runId);
        String status = str(body.getOrDefault("status", "failed"));
        if (!List.of("succeeded", "failed", "running").contains(status)) {
            throw new IllegalArgumentException("status 须为 succeeded|failed|running");
        }
        metaJdbc.update("UPDATE tag_run SET status=?, exit_code=?, summary_json=?, matrix_json=?, error_msg=?, finished_at=NOW() WHERE id=?",
                status,
                body.get("exitCode") == null ? null : ((Number) body.get("exitCode")).intValue(),
                jstr(body.getOrDefault("summary", new LinkedHashMap<String, Object>())),
                body.get("matrix") == null ? null : jstr(body.get("matrix")),
                str(body.get("error")).isEmpty() ? null : str(body.get("error")),
                runId);
        // 游标回写(仅 succeeded + 显式给 cursor; 失败不推进 → 下轮重试幂等 §14.2)
        if ("succeeded".equals(status) && body.get("cursor") instanceof Map<?, ?> cursorMap) {
            Long taskId = (Long) run.get("task_id");
            if (taskId != null && !cursorMap.isEmpty()) {
                metaJdbc.update("UPDATE tag_task SET cursor_json=? WHERE id=?", jstr(cursorMap), taskId);
                log.info("[tagengine] run#{} 游标回写任务#{}: {}", runId, taskId, jstr(cursorMap));
            }
        }
        return Map.of("runId", runId, "status", status);
    }

    /** spark 提交异步等待: exit0 → 读 cursor-out 回写 tag_task.cursor_json + run succeeded; 非 0 → failed 游标不动(下轮重试幂等) */
    private void awaitSpark(long runId, long taskId, Process p, Path cursorOut) {
        try {
            int exit = p.waitFor();
            String cursorAfter = null;
            String matrixJson = null;
            if (exit == 0) {
                // 试跑矩阵回写(④): matrix-out 与 cursor-out 同目录同 run 号
                Path matrixOut = cursorOut.resolveSibling(cursorOut.getFileName().toString().replace(".cursor.json", ".matrix.json"));
                if (Files.exists(matrixOut)) {
                    matrixJson = Files.readString(matrixOut, StandardCharsets.UTF_8);
                    metaJdbc.update("UPDATE tag_run SET matrix_json=? WHERE id=?", matrixJson, runId);
                    log.info("[tagengine] run#{} exit0 命中矩阵回写", runId);
                }
                if (Files.exists(cursorOut)) {
                    cursorAfter = Files.readString(cursorOut, StandardCharsets.UTF_8);
                    JsonNode c = jnodeSafe(cursorAfter);
                    if (c != null && c.has("seqColumn") && c.has("maxSeq")) {
                        metaJdbc.update("UPDATE tag_task SET cursor_json=? WHERE id=?", cursorAfter, taskId);
                        log.info("[tagengine] run#{} exit0 游标回写任务#{}: {}", runId, taskId, cursorAfter);
                    } else {
                        cursorAfter = null;   // 非增量任务/无游标输出
                        log.info("[tagengine] run#{} exit0 无游标输出(非增量任务)", runId);
                    }
                }
            }
            metaJdbc.update("UPDATE tag_run SET status=?, exit_code=?, cursor_after=?, finished_at=NOW() WHERE id=?",
                    exit == 0 ? "succeeded" : "failed", exit, cursorAfter, runId);
            log.info("[tagengine] run#{} spark 作业结束 exit={}{}", runId, exit,
                    exit == 0 ? "" : "(游标未推进, 下轮重试)");
        } catch (Exception e) {
            metaJdbc.update("UPDATE tag_run SET status='failed', error_msg=?, finished_at=NOW() WHERE id=?",
                    "提交器异常: " + e.getMessage(), runId);
            log.error("[tagengine] run#{} 提交器异常", runId, e);
        }
    }

    /** 15s tick: cron 定时任务自动触发(需 dcp.tagengine.auto-run=true; 每分钟至多一次, 秒字段按整分匹配) */
    @Scheduled(fixedDelay = 15000)
    public void autoTick() {
        if (!autoRun) return;
        List<Map<String, Object>> due = metaJdbc.queryForList(
                "SELECT id, schedule, interval_min, cron_expr, last_auto_run_at FROM tag_task WHERE status='published' AND schedule IN ('interval','cron')");
        LocalDateTime now = LocalDateTime.now();
        LocalDateTime minuteStart = now.withSecond(0).withNano(0);
        for (Map<String, Object> t : due) {
            long taskId = ((Number) t.get("id")).longValue();
            String schedule = str(t.get("schedule"));
            // MySQL 驱动 DATETIME 经 JdbcTemplate 返回 LocalDateTime; 兼容两种类型(lakesync 同坑)
            java.sql.Timestamp last;
            Object lastRaw = t.get("last_auto_run_at");
            if (lastRaw instanceof java.sql.Timestamp ts) last = ts;
            else if (lastRaw instanceof LocalDateTime ldt) last = java.sql.Timestamp.valueOf(ldt);
            else last = null;
            boolean dueNow;
            if ("interval".equals(schedule)) {
                int min = ((Number) t.getOrDefault("interval_min", 15)).intValue();
                dueNow = last == null || last.toLocalDateTime().plusMinutes(min).isBefore(now);
            } else {
                if (!cronMatch(str(t.get("cron_expr")), now)) continue;
                dueNow = last == null || last.toLocalDateTime().isBefore(minuteStart);   // cron 整分至多一次
            }
            if (!dueNow) continue;
            try {
                startRun(taskId, "cron");
                metaJdbc.update("UPDATE tag_task SET last_auto_run_at=NOW() WHERE id=?", taskId);
                log.info("[tagengine] {} 自动触发任务#{} 成功", schedule, taskId);
            } catch (Exception e) {
                log.warn("[tagengine] cron 自动触发任务#{} 失败: {}", taskId, e.getMessage());
            }
        }
    }

    /** 极简 cron 匹配(6-7 段: 秒 分 时 日 月 周 [年]): 支持 * 与单值; 秒/日/月忽略, 周支持 * 或数字(Spring 语义 1=周日? 保守: 仅当 token 为 * 或等于当前 dayOfWeek.getValue()+1 对齐按 1=周一记, 见注释) */
    private boolean cronMatch(String expr, LocalDateTime now) {
        try {
            String[] f = expr.trim().split("\\s+");
            if (f.length < 6) return false;
            if (!"*".equals(f[1]) && Integer.parseInt(f[1]) != now.getMinute()) return false;
            if (!"*".equals(f[2]) && Integer.parseInt(f[2]) != now.getHour()) return false;
            String dow = f[5];
            if (!"*".equals(dow)) {
                int cur = now.getDayOfWeek().getValue();   // 1=周一..7=周日
                if (!List.of(String.valueOf(cur), String.valueOf(cur % 7)).contains(dow)) return false; // 兼容 0/7=周日
            }
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private void collectDepsOf(Map<String, Object> rule, Set<Long> seen, Set<String> out) {
        long id = ((Number) rule.get("id")).longValue();
        if (!seen.add(id)) return;
        JsonNode def = jnode((String) rule.get("def_json"));
        if ("flow".equals(rule.get("kind"))) {
            JsonNode steps = def.path("steps");
            for (JsonNode st : steps) {
                Map<String, Object> ref = ruleById(st.path("refId").asLong(0));
                if (ref != null) collectDepsOf(ref, seen, out);
            }
        } else {
            String mode = def.path("mode").asText("visual");
            if ("visual".equals(mode)) {
                JsonNode when = def.path("visual").path("when");
                for (JsonNode g : when) for (JsonNode c : g.path("conds")) {
                    String f = c.path("field").asText("");
                    if (!f.isEmpty()) out.add(f);
                }
            } else if ("table".equals(mode)) {
                String f = def.path("table").path("field").asText("");
                if (!f.isEmpty()) out.add(f);
            }
        }
    }

    private static String str(Object o) {
        return o == null ? "" : String.valueOf(o);
    }

    /* ---------- ③ 发布标签校验: 动作 tag 已注册 + 跨任务同 tag 冲突拦截 ---------- */

    /** 从规则定义提取动作标签集(visual: actions.tag; table: table.tag; drl: tag($f,"路径")) */
    private java.util.Set<String> collectTagsOf(Map<String, Object> rule) {
        java.util.Set<String> out = new java.util.LinkedHashSet<>();
        JsonNode def = jnode((String) rule.get("def_json"));
        String mode = def.path("mode").asText("visual");
        if ("visual".equals(mode)) {
            for (JsonNode a : def.path("visual").path("actions")) {
                if ("tag".equals(a.path("type").asText())) {
                    String t = a.path("tag").asText("");
                    if (!t.isEmpty()) out.add(t);
                }
            }
        } else if ("table".equals(mode)) {
            String t = def.path("table").path("tag").asText("");
            if (!t.isEmpty()) out.add(t);
        } else if ("drl".equals(mode)) {
            String drl = def.path("drl").asText("");
            java.util.regex.Matcher m = java.util.regex.Pattern.compile("tag\\(\\$f,\\s*\\\"([^\\\"]+)\"")
                    .matcher(drl);
            while (m.find()) out.add(m.group(1));
        }
        return out;
    }

    /** 已注册标签路径集合(registry tagPath, 规则动作 tag 必须精确匹配) */
    private java.util.Set<String> registeredTagPaths() {
        java.util.Map<Long, String> name = new java.util.HashMap<>();
        java.util.Map<Long, Long> parent = new java.util.HashMap<>();
        for (Map<String, Object> r : metaJdbc.queryForList("SELECT id,parent_id,name FROM tag_dict")) {
            long id = ((Number) r.get("id")).longValue();
            name.put(id, String.valueOf(r.get("name")));
            parent.put(id, ((Number) r.get("parent_id")).longValue());
        }
        java.util.Set<String> paths = new java.util.LinkedHashSet<>();
        for (Map<String, Object> r : metaJdbc.queryForList("SELECT tag_dict_id FROM tag_registry ORDER BY id")) {
            long id = ((Number) r.get("tag_dict_id")).longValue();
            java.util.List<String> chain = new java.util.ArrayList<>();
            Long cur = id;
            java.util.Set<Long> guard = new java.util.HashSet<>();
            while (cur != null && cur != 0 && guard.add(cur) && name.containsKey(cur)) {
                chain.add(0, name.get(cur));
                cur = parent.get(cur);
            }
            paths.add(String.join("/", chain));
        }
        return paths;
    }

    /** 已发布任务快照中的动作标签 → 认领任务名(冲突拦截用; 排除自身) */
    private java.util.Map<String, String> claimedTagsBy(long excludeTaskId, String mode) {
        java.util.Map<String, String> claim = new java.util.LinkedHashMap<>();
        java.util.List<Map<String, Object>> tasks = metaJdbc.queryForList(
                "SELECT id,name,mode,snapshot_json FROM tag_task WHERE status='published' AND id<>? AND mode=?", excludeTaskId, mode);
        for (Map<String, Object> t : tasks) {
            JsonNode snap = jnode((String) t.get("snapshot_json"));
            for (JsonNode a : snap.path("assets")) {
                Map<String, Object> fake = new java.util.HashMap<>();
                fake.put("kind", a.path("kind").asText());
                fake.put("def_json", a.path("def").toString());
                for (String tag : collectTagsOf(fake)) {
                    claim.putIfAbsent(tag, String.valueOf(t.get("name")));
                }
            }
        }
        return claim;
    }

    private void checkPublishTags(long taskId, List<Map<String, Object>> ordered, String mode) {
        java.util.Set<String> registered = registeredTagPaths();
        java.util.Set<String> myTags = new java.util.LinkedHashSet<>();
        for (Map<String, Object> r : ordered) {
            if ("flow".equals(r.get("kind"))) continue;   // 流不直接产标签(引用规则已展开收录)
            myTags.addAll(collectTagsOf(r));
        }
        java.util.List<String> unreg = myTags.stream().filter(t -> !registered.contains(t)).toList();
        if (!unreg.isEmpty()) {
            throw new IllegalArgumentException("以下动作标签未注册(tag 路径须与注册表 tagPath 精确一致): "
                    + String.join("、", unreg) + " —— 请先在 标签注册表 V2 注册, 或在规则里改选已注册标签路径");
        }
        java.util.Map<String, String> claimed = claimedTagsBy(taskId, mode);
        java.util.List<String> conflict = myTags.stream().filter(claimed::containsKey).toList();
        if (!conflict.isEmpty()) {
            java.util.List<String> who = new java.util.ArrayList<>();
            for (String c : conflict) who.add(c + "(已被任务「" + claimed.get(c) + "」产出)");
            throw new IllegalArgumentException("同标签跨任务冲突(§11⑧, 标签由首次发布任务认领): "
                    + String.join("、", who) + " —— 同一标签只能由一个任务产出; 如需多规则协同请用决策表/规则流编排在同一任务内");
        }
    }

    /* ---------- ⑥ 标签认领/定位查询(§13: 标签由首次发布任务认领; 实体×标签当前值在认领任务的 tag_fact 湖表) ---------- */

    public List<Map<String, Object>> listClaims() {
        Map<String, Map<String, Object>> claim = new java.util.TreeMap<>();
        List<Map<String, Object>> tasks = metaJdbc.queryForList(
                "SELECT id,name,dataset,entity_key,ver,snapshot_json,cursor_json FROM tag_task WHERE status='published' ORDER BY ver,id");
        for (Map<String, Object> t : tasks) {
            JsonNode snap = jnode((String) t.get("snapshot_json"));
            for (JsonNode a : snap.path("assets")) {
                Map<String, Object> fake = new java.util.HashMap<>();
                fake.put("kind", a.path("kind").asText());
                fake.put("def_json", a.path("def").toString());
                for (String tag : collectTagsOf(fake)) {
                    claim.putIfAbsent(tag, claimOf(t, tag));
                }
            }
        }
        return new java.util.ArrayList<>(claim.values());
    }

    public Map<String, Object> getClaim(String tagPath) {
        for (Map<String, Object> c : listClaims()) {
            if (tagPath.equals(c.get("tagPath"))) return c;
        }
        throw new IllegalArgumentException("标签未被任何已发布任务认领: " + tagPath
                + " —— 请先发布一个产出该标签的任务(注册表标签 → 规则动作 → 任务绑定 → 发布)");
    }

    private Map<String, Object> claimOf(Map<String, Object> task, String tagPath) {
        Map<String, Object> m = new java.util.LinkedHashMap<>();
        m.put("tagPath", tagPath);
        m.put("claimedByTaskId", task.get("id"));
        m.put("claimedByTask", task.get("name"));
        m.put("taskVer", task.get("ver"));
        m.put("dataset", task.get("dataset"));
        m.put("entityKey", task.get("entity_key"));
        JsonNode cur = jnodeSafe((String) task.get("cursor_json"));
        m.put("cursor", cur == null || cur.isEmpty() ? null : cur);
        String cat = env("DCP_TAGENGINE_CATALOG", "demo");
        String ns = env("DCP_TAGENGINE_NAMESPACE", "dcp");
        Map<String, Object> lake = new java.util.LinkedHashMap<>();
        lake.put("catalog", cat);
        lake.put("namespace", ns);
        lake.put("table", env("DCP_TAGENGINE_TAGFACT_TABLE", "tag_fact"));
        lake.put("query", "SELECT entity, tag, value FROM " + cat + "." + ns + ".tag_fact"
                + " WHERE tag = '" + tagPath.replace("'", "\\'") + "'");
        m.put("tagFact", lake);
        for (Map<String, Object> reg : registryRowsWithPath()) {
            if (tagPath.equals(reg.get("tagPath"))) {
                m.put("valueType", reg.get("valueType"));
                m.put("timing", reg.get("timing"));
                m.put("domain", reg.get("domain"));
                break;
            }
        }
        return m;
    }

    private List<Map<String, Object>> registryRowsWithPath() {
        Map<Long, String> name = new java.util.HashMap<>();
        Map<Long, Long> parent = new java.util.HashMap<>();
        for (Map<String, Object> r : metaJdbc.queryForList("SELECT id,parent_id,name FROM tag_dict")) {
            long id = ((Number) r.get("id")).longValue();
            name.put(id, String.valueOf(r.get("name")));
            parent.put(id, ((Number) r.get("parent_id")).longValue());
        }
        List<Map<String, Object>> out = new java.util.ArrayList<>();
        List<Map<String, Object>> regs = metaJdbc.queryForList(
                "SELECT id,tag_dict_id,value_type,timing,domain_json FROM tag_registry ORDER BY id");
        for (Map<String, Object> r : regs) {
            long id = ((Number) r.get("tag_dict_id")).longValue();
            List<String> chain = new java.util.ArrayList<>();
            Long cur = id;
            Set<Long> guard = new java.util.HashSet<>();
            while (cur != null && cur != 0 && guard.add(cur) && name.containsKey(cur)) {
                chain.add(0, name.get(cur));
                cur = parent.get(cur);
            }
            Map<String, Object> m = new java.util.LinkedHashMap<>();
            m.put("tagPath", String.join("/", chain));
            m.put("valueType", r.get("value_type"));
            m.put("timing", r.get("timing"));
            m.put("domain", jnodeSafe((String) r.get("domain_json")));
            out.add(m);
        }
        return out;
    }

    private static JsonNode jnodeSafe(String s) {
        try {
            return s == null ? null : OM.readTree(s);
        } catch (Exception e) {
            return null;
        }
    }

    /* ---------- 标签服务查询(投影表 tag_fact_view; 湖 tag_fact 权威, 投影为圈选/订阅查询面) ---------- */

    public List<Map<String, Object>> labelSummary() {
        List<Map<String, Object>> rows = metaJdbc.queryForList(
                "SELECT tag, COUNT(DISTINCT entity) AS entities, MAX(run_ts) AS lastRun FROM tag_fact_view GROUP BY tag ORDER BY tag");
        // tag_fact_view 不存在(还没跑过带投影的轮次) → 空列表而非报错
        return rows;
    }

    public Map<String, Object> labelCircle(String tagPath, String value, int limit) {
        StringBuilder sql = new StringBuilder("SELECT entity, MAX(value) AS value FROM tag_fact_view WHERE tag=?");
        List<Object> args = new java.util.ArrayList<>();
        args.add(tagPath);
        if (value != null && !value.isBlank()) {
            sql.append(" AND value=?");
            args.add(value);
        }
        sql.append(" GROUP BY entity ORDER BY entity LIMIT ?");
        args.add(limit);
        List<Map<String, Object>> rows;
        try {
            rows = metaJdbc.queryForList(sql.toString(), args.toArray());
        } catch (org.springframework.dao.DataAccessException e) {
            throw new IllegalArgumentException("标签视图不可用(投影表未建/未同步): " + e.getMessage()
                    + " —— 请先跑一轮带 tagFactView 配置的打标任务");
        }
        Map<String, Object> out = new java.util.LinkedHashMap<>();
        out.put("tagPath", tagPath);
        out.put("value", value == null || value.isBlank() ? null : value);
        out.put("count", rows.size());
        out.put("entities", rows);
        return out;
    }

    public Map<String, Object> labelEntity(String entityKey) {
        List<Map<String, Object>> rows;
        try {
            rows = metaJdbc.queryForList(
                    "SELECT task_id, tag, value, run_ts FROM tag_fact_view WHERE entity=? ORDER BY tag", entityKey);
        } catch (org.springframework.dao.DataAccessException e) {
            throw new IllegalArgumentException("标签视图不可用(投影表未建/未同步): " + e.getMessage());
        }
        Map<String, Object> out = new java.util.LinkedHashMap<>();
        out.put("entity", entityKey);
        out.put("count", rows.size());
        out.put("labels", rows);
        return out;
    }
}
