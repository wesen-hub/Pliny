package com.dcp.module.tagengine.online;

import com.dcp.module.tagengine.online.kie.AssetPlan;
import com.dcp.module.tagengine.online.kie.BuildPlan;
import com.dcp.module.tagengine.online.kie.DrlGenerator;
import com.dcp.module.tagengine.online.kie.FactSourceGen;
import com.dcp.module.tagengine.online.kie.KieBaseFactory;
import com.dcp.module.tagengine.online.kie.OnlineRule;
import com.dcp.tagjob.run.TagActions;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.kie.api.KieBase;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * 在线打标执行(P1; 设计 §4 在线态 + §9 P1):
 * 事件(fields, 含实体键)→ 动态 Fact → Drools(同执行器 KieModule 编译栈: Fact 源码+DRL 入 KieModule
 * 由 KieBuilder 编译, 无运行时动态类)→ 命中动作打标签 → Redis 置位 tag:online:{entity}(hash: tag→value)。
 *
 * <p>在线语义(§4): 单事件单 Fact 求值; 输出仅在线层(Redis), 不写湖事实层; instant 标签 TTL 由注册 timing 推导
 * (本期: 事件带 ttlSeconds 覆盖或默认 3600)。规则流按发布快照闭包展开为引用规则集求值(平铺, halt 停后续;
 * 与批一致的流内分支/跳转控制流 P1.1 —— 在线 v1 以规则集全求值+halt 短路为主)。
 */
@Slf4j
@Service
public class OnlineTagService {

    private static final ObjectMapper OM = new ObjectMapper();
    private static final String KEY_PREFIX = "tag:online:";
    private static final long DEFAULT_TTL_SECONDS = 3600;

    private final JdbcTemplate metaJdbc;
    private final StringRedisTemplate redis;
    private final Map<Long, Bundle> cache = new ConcurrentHashMap<>();

    public OnlineTagService(JdbcTemplate metaJdbc, StringRedisTemplate redis) {
        this.metaJdbc = metaJdbc;
        this.redis = redis;
    }

    /** 任务在线信息(需 published) */
    public Map<String, Object> info(long taskId) {
        List<Map<String, Object>> rows = metaJdbc.queryForList(
                "SELECT id,name,entity_key,ver FROM tag_task WHERE id=? AND status='published'", taskId);
        if (rows.isEmpty()) throw new IllegalArgumentException("在线任务不存在或未发布 id=" + taskId);
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("id", rows.get(0).get("id"));
        m.put("name", rows.get(0).get("name"));
        m.put("entityKey", rows.get(0).get("entity_key"));
        m.put("ver", rows.get(0).get("ver"));
        return m;
    }

    /** 事件求值 + Redis 置位; fields 含实体键与业务字段(与 datasetFields 对应; 类型按 Fact 声明转换) */
    public Map<String, Object> handleEvent(long taskId, Map<String, Object> fields, Long ttlSeconds) {
        Map<String, Object> task = info(taskId);
        String entityKey = String.valueOf(task.get("entityKey"));
        Object entityVal = fields.get(entityKey);
        if (entityVal == null) throw new IllegalArgumentException("事件缺实体键字段: " + entityKey);
        String entity = String.valueOf(entityVal);
        Bundle b = bundle(taskId);
        TagActions.Collector collector = new TagActions.Collector();
        try {
            Object fact = b.factClass.getDeclaredConstructor().newInstance();
            for (BuildPlan.FactField f : b.factFields) {
                Object v = fields.get(f.name);
                if (v == null) continue;
                Object set = switch (f.kind) {
                    case 'S' -> String.valueOf(v);
                    case 'L' -> (v instanceof Number n) ? n.longValue() : null;
                    case 'D' -> (v instanceof Number n) ? n.doubleValue() : null;
                    case 'B' -> (v instanceof Boolean bb) ? bb : null;
                    default -> null;
                };
                if (set != null) {
                    try {
                        b.setters.get(f.name).invoke(fact, set);
                    } catch (Exception ignore) {
                        // setter 缺失(如字段不在事件) → 该字段保持默认
                    }
                }
            }
            org.kie.api.runtime.KieSession session = b.base.newKieSession();
            try {
                TagActions.bindCollector(collector);
                TagActions.bindSession(session);
                org.kie.api.runtime.rule.FactHandle h = session.insert(fact);
                session.fireAllRules();
                session.delete(h);
            } finally {
                TagActions.clearThread();
                session.dispose();
            }
        } catch (Exception e) {
            throw new IllegalArgumentException("事件求值失败: " + e.getMessage(), e);
        }
        // Redis 置位: hash tag→value; 每标签独立字段, 整 key TTL
        String key = KEY_PREFIX + entity;
        if (collector.tags().isEmpty()) {
            // 无命中: 不清旧值(在线层保留最近状态), 仅 touch TTL
            redis.expire(key, (ttlSeconds == null ? DEFAULT_TTL_SECONDS : ttlSeconds), TimeUnit.SECONDS);
        } else {
            Map<String, String> m = new LinkedHashMap<>();
            for (String[] t : collector.tags()) {
                m.put(t[0], t[1] == null ? "" : t[1]);
            }
            redis.opsForHash().putAll(key, m);
            redis.expire(key, (ttlSeconds == null ? DEFAULT_TTL_SECONDS : ttlSeconds), TimeUnit.SECONDS);
        }
        log.info("[online] task#{} 事件 entity={} 命中标签={} key={}", taskId, entity, collector.tags().size(), key);
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("entity", entity);
        out.put("key", key);
        out.put("ttlSeconds", ttlSeconds == null ? DEFAULT_TTL_SECONDS : ttlSeconds);
        List<Map<String, String>> tags = new ArrayList<>();
        for (String[] t : collector.tags()) {
            Map<String, String> tm = new LinkedHashMap<>();
            tm.put("tag", t[0]);
            tm.put("value", t[1]);
            tags.add(tm);
        }
        out.put("tags", tags);
        return out;
    }

    /** 读在线层实体标签(Redis tag:online:{entity}) */
    public Map<String, Object> readOnline(String entity) {
        String key = KEY_PREFIX + entity;
        Map<Object, Object> raw = redis.opsForHash().entries(key);
        Map<String, String> tags = new LinkedHashMap<>();
        raw.forEach((k, v) -> tags.put(String.valueOf(k), String.valueOf(v)));
        Long ttl = redis.getExpire(key, TimeUnit.SECONDS);
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("entity", entity);
        out.put("key", key);
        out.put("ttlSeconds", ttl == null ? -1 : ttl);
        out.put("tags", tags);
        return out;
    }

    /* ---------- KieBundle: 编译缓存 ---------- */

    private Bundle bundle(long taskId) {
        Bundle hit = cache.get(taskId);
        if (hit != null) return hit;
        synchronized (this) {
            hit = cache.get(taskId);
            if (hit != null) return hit;
            JsonNode snapshot = parseSnapshot(taskId);
            List<OnlineRule> rules = collectRules(snapshot);
            if (rules.isEmpty()) throw new IllegalArgumentException("在线任务无可执行规则(快照为空)");
            String entityKey = String.valueOf(metaJdbc.queryForObject(
                    "SELECT entity_key FROM tag_task WHERE id=?", String.class, taskId));
            String pkg = "dcp.tagengine.online.fact.t" + taskId;
            String fqcn = pkg + ".Fact";
            List<BuildPlan.FactField> factFields = new ArrayList<>();
            factFields.add(new BuildPlan.FactField(entityKey, 'S'));
            // 事实字段域 = 任务 dataset_fields_json(快照只含 bindings/assets)
            JsonNode ds;
            try {
                ds = OM.readTree(metaJdbc.queryForObject(
                        "SELECT dataset_fields_json FROM tag_task WHERE id=?", String.class, taskId));
            } catch (Exception e) {
                throw new IllegalArgumentException("在线任务字段域解析失败: " + e.getMessage());
            }
            if (ds != null && ds.isArray()) {
                for (JsonNode f : ds) {
                    String name = f.isTextual() ? f.asText() : f.path("name").asText("");
                    if (!name.isEmpty() && !name.equals(entityKey)
                            && factFields.stream().noneMatch(x -> x.name.equals(name))) {
                        factFields.add(new BuildPlan.FactField(name, 'S'));
                    }
                }
            }
            String javaSource = FactSourceGen.genSource(pkg, factFields);
            List<String> errs = new ArrayList<>();
            List<AssetPlan> assets = new ArrayList<>();
            Map<String, Character> kinds = new HashMap<>();
            for (BuildPlan.FactField f : factFields) kinds.put(f.name, f.kind);
            for (OnlineRule r : rules) {
                AssetPlan ap = new AssetPlan();
                ap.id = r.id;
                ap.kind = "rule";
                ap.name = r.name;
                ap.ver = r.ver;
                ap.pkgName = DrlGenerator.assetPackage(r.id, r.ver);
                String drl = DrlGenerator.genRuleDrl(r, fqcn, kinds, errs);
                if (drl == null || drl.isBlank()) continue;
                ap.drl = drl;
                assets.add(ap);
            }
            if (assets.isEmpty()) throw new IllegalArgumentException("在线规则编译为空: " + String.join("; ", errs));
            KieBaseFactory.KieBuildResult res = KieBaseFactory.buildBase(assets, javaSource, fqcn, errs);
            if (res == null) throw new IllegalArgumentException("在线 KieBase 编译失败: " + String.join("; ", errs));
            Bundle b = new Bundle(res.kieBase(), res.moduleClassLoader(), fqcn, factFields);
            cache.put(taskId, b);
            log.info("[online] 任务#{} KieBase 就绪: 资产={} Fact={} 字段={}", taskId, assets.size(), fqcn,
                    factFields.stream().map(f -> f.name + ":" + f.kind).toList());
            return b;
        }
    }

    /** 快照闭包 → 规则资产(rule 直收; flow 的步骤引用规则展开; 去重) */
    private List<OnlineRule> collectRules(JsonNode snapshot) {
        Map<Long, JsonNode> byId = new LinkedHashMap<>();
        for (JsonNode a : snapshot.path("assets")) byId.put(a.path("id").asLong(), a);
        List<OnlineRule> rules = new ArrayList<>();
        java.util.Set<Long> seen = new java.util.LinkedHashSet<>();
        java.util.Set<Long> visiting = new java.util.LinkedHashSet<>();
        List<Long> order = new ArrayList<>();
        // 绑定顺序
        for (JsonNode b : snapshot.path("bindings")) {
            long id = b.path("id").asLong();
            JsonNode a = byId.get(id);
            if (a == null) continue;
            expand(a, byId, order, seen, visiting);
        }
        // 未在绑定但被引用(防御) —— 忽略: 快照 bindings 已含执行序
        for (Long id : order) {
            JsonNode a = byId.get(id);
            if (a != null && "rule".equals(a.path("kind").asText())) {
                rules.add(OnlineRule.from(id, "rule", a.path("name").asText(), a.path("ver").asInt(), a.path("def")));
            }
        }
        return rules;
    }

    private void expand(JsonNode a, Map<Long, JsonNode> byId, List<Long> order,
                        java.util.Set<Long> seen, java.util.Set<Long> visiting) {
        long id = a.path("id").asLong();
        if (!seen.add(id)) return;
        if (!visiting.add(id)) return;   // 环防御(发布校验已挡)
        order.add(id);
        if ("flow".equals(a.path("kind").asText())) {
            for (JsonNode st : a.path("def").path("steps")) {
                JsonNode ref = byId.get(st.path("refId").asLong());
                if (ref != null) expand(ref, byId, order, seen, visiting);
            }
        }
        visiting.remove(id);
    }

    private JsonNode parseSnapshot(long taskId) {
        List<Map<String, Object>> rows = metaJdbc.queryForList(
                "SELECT snapshot_json FROM tag_task WHERE id=?", taskId);
        if (rows.isEmpty() || rows.get(0).get("snapshot_json") == null) {
            throw new IllegalArgumentException("任务快照缺失, 请重新发布: " + taskId);
        }
        try {
            return OM.readTree((String) rows.get(0).get("snapshot_json"));
        } catch (Exception e) {
            throw new IllegalArgumentException("任务快照解析失败: " + e.getMessage());
        }
    }

    public static class Bundle {
        public final KieBase base;
        public final Class<?> factClass;
        public final List<BuildPlan.FactField> factFields;
        public final Map<String, java.lang.reflect.Method> setters = new HashMap<>();

        public Bundle(KieBase base, ClassLoader moduleLoader, String factFqcn, List<BuildPlan.FactField> factFields) {
            this.base = base;
            this.factFields = factFields;
            try {
                this.factClass = Class.forName(factFqcn, true, moduleLoader);
            } catch (ClassNotFoundException e) {
                throw new IllegalArgumentException("Fact 类加载失败: " + factFqcn, e);
            }
            for (BuildPlan.FactField f : factFields) {
                Class<?> type = switch (f.kind) {
                    case 'S' -> String.class;
                    case 'L' -> Long.class;
                    case 'D' -> Double.class;
                    case 'B' -> Boolean.class;
                    default -> String.class;
                };
                try {
                    setters.put(f.name, factClass.getMethod("set" + Character.toUpperCase(f.name.charAt(0))
                            + f.name.substring(1), type));
                } catch (NoSuchMethodException ignore) {
                    // Fact 生成器保证合法标识符 setter 存在
                }
            }
        }
    }
}
