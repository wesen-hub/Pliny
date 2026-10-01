package com.dcp.scheduler.rest;

import com.dcp.scheduler.model.InstanceMetrics;
import com.dcp.scheduler.model.NsInstanceRecord;
import com.dcp.scheduler.model.NsMetrics;
import com.dcp.scheduler.model.OpsRecord;
import com.dcp.scheduler.model.PartMetrics;
import com.dcp.scheduler.model.ScaleRules;
import com.dcp.scheduler.model.TaskMetrics;
import com.dcp.scheduler.store.EtcdStore;
import com.dcp.scheduler.store.Json;
import com.dcp.scheduler.store.KeySpace;
import io.etcd.jetcd.ByteSequence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Pattern;

/**
 * 统一调度框架键空间管理访问服务(迁自 backend SchedulerEtcdService,方案 N2):
 * 后台页面读调度主写的告警事件流/监控聚合(监控子文档 §二、§六),
 * 并通过 ops 队列下发运维指令(调度运维后台 §2:钉住/解锁/迁移/下架/暂停/调权)。
 * 连接懒建;etcd 不可达时读侧返回空(页面显示"暂无",不阻塞其他功能),写侧报错回显。
 * 宿主 = dcp-worker(唯一依赖 etcd 的服务,方案 §7.2)。
 */
@Service
public class EtcdAdminService implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(EtcdAdminService.class);

    private final String endpoints;

    private volatile EtcdStore store;

    public EtcdAdminService(@Value("${dcp.etcd.endpoints:http://localhost:2379}") String endpoints) {
        this.endpoints = endpoints;
    }

    private EtcdStore ensure() {
        if (store == null) {
            synchronized (this) {
                if (store == null) {
                    store = EtcdStore.connect(endpoints);
                    log.info("调度框架 etcd 管理连接: {}", endpoints);
                }
            }
        }
        return store;
    }

    @Override
    public void close() {
        if (store != null) {
            store.close();
            store = null;
        }
    }

    /** 读 ns 告警事件流(/dcp/{ns}/alerts/),按触发时间倒序。 */
    public List<SchedulerAlert> listAlerts(String ns) {
        if (ns == null || ns.isBlank()) return List.of();
        try {
            Map<String, byte[]> kvs = ensure().getPrefix(KeySpace.alertsPrefix(ns));
            List<SchedulerAlert> out = new ArrayList<>();
            for (byte[] v : kvs.values()) {
                SchedulerAlert a = Json.fromBytes(v, SchedulerAlert.class);
                if (a != null) out.add(a);
            }
            out.sort(Comparator.comparingLong((SchedulerAlert a) -> a.since).reversed());
            return out;
        } catch (Exception e) {
            log.warn("读调度告警事件流失败 ns={}: {}", ns, e.getMessage());
            return List.of();
        }
    }

    // ── 监控聚合只读(监控子文档 §二:四级派生键,后台直读不经计算) ──

    /** ns 级大盘(/dcp/{ns}/metrics/ns);无数据 = null。 */
    public NsMetrics readNsMetrics(String ns) {
        if (ns == null || ns.isBlank()) return null;
        return readOne(KeySpace.nsMetrics(ns), NsMetrics.class);
    }

    /** 任务级聚合(/dcp/{ns}/metrics/tasks/{taskId},含 /parts 子键需过滤)。 */
    public List<TaskMetrics> listTaskMetrics(String ns) {
        if (ns == null || ns.isBlank()) return List.of();
        return readAll(KeySpace.metricsPrefix(ns), TaskMetrics.class, key -> !key.contains("/parts/"));
    }

    /** 实例级聚合(/dcp/{ns}/metrics/instances/{id})。 */
    public List<InstanceMetrics> listInstanceMetrics(String ns) {
        if (ns == null || ns.isBlank()) return List.of();
        return readAll(KeySpace.metricsPrefix(ns), InstanceMetrics.class, key -> key.contains("/instances/"));
    }

    /** 分区级派生(全量;taskId 非空时只取该任务)。 */
    public List<PartMetrics> listPartMetrics(String ns, String taskId) {
        if (ns == null || ns.isBlank()) return List.of();
        String prefix = taskId == null || taskId.isBlank()
                ? "/dcp/" + ns + "/metrics/tasks/"
                : "/dcp/" + ns + "/metrics/tasks/" + taskId + "/parts/";
        return readAll(ByteSequence.from(prefix, StandardCharsets.UTF_8), PartMetrics.class,
                key -> key.contains("/parts/"));
    }

    // ── 运维指令通道(调度运维后台 §2:后台写 ops,PENDING → 主消费,DONE 删除 / FAILED 保留) ──

    /**
     * 提交运维指令:opId = 毫秒时间戳 + 随机后缀(键序即提交序,页面按序展示)。
     * 归属键唯一写者始终是主,后台只写 ops 队列,不直接写 parts/意图键。
     */
    public String submitOp(String ns, String type, String taskId, Integer partId,
                           String target, Double weight, Long ttlSec, ScaleRules rules,
                           String operator, String reason) {
        OpsRecord op = new OpsRecord();
        op.opId = System.currentTimeMillis() + "-" + UUID.randomUUID().toString().substring(0, 8);
        op.type = type;
        op.taskId = taskId;
        op.partId = partId;
        op.target = target;
        op.weight = weight;
        op.ttlSec = ttlSec;
        op.rules = rules;
        op.operator = operator;
        op.reason = reason;
        op.state = OpsRecord.OpState.PENDING;
        try {
            ensure().put(KeySpace.op(ns, op.opId), op);
            log.info("提交运维指令 [{}] {} target={} operator={}", ns, type, target, operator);
            return op.opId;
        } catch (Exception e) {
            log.warn("提交运维指令失败 ns={} type={}: {}", ns, type, e.getMessage());
            throw new IllegalStateException("写调度指令失败: " + e.getMessage());
        }
    }

    /** 请求体 rules(Map) → ScaleRules(缺省值补内置默认)。 */
    public ScaleRules parseRules(Object raw) {
        if (raw == null) return new ScaleRules();
        return Json.MAPPER.convertValue(raw, ScaleRules.class);
    }

    /** ops 队列(/dcp/{ns}/ops/):PENDING 待消费 + FAILED 保留展示;DONE 由主删除)。 */
    public List<OpsRecord> listOps(String ns) {
        if (ns == null || ns.isBlank()) return List.of();
        return readAll(KeySpace.opsPrefix(ns), OpsRecord.class, key -> true);
    }

    /** 实例运维视图:注册键 + 主写意图键合并(drain/paused/weight,意图优先)。 */
    public List<SchedulerInstanceView> listInstances(String ns) {
        if (ns == null || ns.isBlank()) return List.of();
        try {
            Map<String, byte[]> kvs = ensure().getPrefix(KeySpace.nsInstancesPrefix(ns));
            List<SchedulerInstanceView> out = new ArrayList<>();
            for (Map.Entry<String, byte[]> e : kvs.entrySet()) {
                String key = e.getKey();
                if (key.endsWith("/drain") || key.endsWith("/paused") || key.endsWith("/weight")) continue;
                NsInstanceRecord r = Json.fromBytes(e.getValue(), NsInstanceRecord.class);
                if (r == null || r.id == null) continue;
                SchedulerInstanceView v = new SchedulerInstanceView();
                v.id = r.id;
                v.status = r.status;
                v.capacity = r.capacity;
                v.weight = r.weight;
                v.paused = r.paused;
                v.drained = readIntent(KeySpace.instDrainIntent(ns, r.id)) != null;
                if (readIntent(KeySpace.instPauseIntent(ns, r.id)) != null) v.paused = true;
                Map<String, Object> w = readIntent(KeySpace.instWeightIntent(ns, r.id));
                if (w != null && w.get("weight") instanceof Number n) v.weight = n.doubleValue();
                out.add(v);
            }
            out.sort(Comparator.comparing(x -> x.id));
            return out;
        } catch (Exception e) {
            log.warn("读调度实例注册失败 ns={}: {}", ns, e.getMessage());
            return List.of();
        }
    }

    private Map<String, Object> readIntent(ByteSequence key) {
        try {
            return ensure().getObj(key, Map.class).orElse(null);
        } catch (Exception e) {
            return null;
        }
    }

    // ── namespace 管理(运维后台设计 §2.1:创建/删除/默认规则;ns 无注册表,前缀出现即存在) ──

    private static final Pattern NS_NAME = Pattern.compile("[a-z][a-z0-9_-]{0,62}");
    private static final Set<String> RESERVED = Set.of("instances", "audit"); // 全局保留前缀,不能当 ns 名

    /** ns 列表:遍历 /dcp/ 一级前缀(排除保留前缀),每 ns 附带轻量统计 + 默认规则。 */
    public List<NamespaceView> listNamespaces() {
        try {
            ByteSequence prefix = ByteSequence.from("/dcp/", StandardCharsets.UTF_8);
            Map<String, byte[]> kvs = ensure().getPrefix(prefix);
            Set<String> names = new HashSet<>();
            for (String key : kvs.keySet()) {
                String rest = key.substring("/dcp/".length());
                int slash = rest.indexOf('/');
                String first = slash < 0 ? rest : rest.substring(0, slash);
                if (!RESERVED.contains(first) && NS_NAME.matcher(first).matches()) names.add(first);
            }
            List<NamespaceView> out = new ArrayList<>();
            for (String ns : names) {
                NamespaceView v = new NamespaceView();
                v.ns = ns;
                v.rules = readOne(KeySpace.rules(ns), ScaleRules.class);
                v.hasLeader = ensure().getBytes(KeySpace.leader(ns)).isPresent();
                v.tasks = (int) countKeys(KeySpace.tasksPrefix(ns), key -> key.endsWith("/spec"));
                v.instances = (int) countKeys(KeySpace.nsInstancesPrefix(ns),
                        key -> !key.endsWith("/drain") && !key.endsWith("/paused") && !key.endsWith("/weight"));
                NsMetrics m = readOne(KeySpace.nsMetrics(ns), NsMetrics.class);
                if (m != null) {
                    v.parts = m.parts;
                    v.rps = m.rps;
                    v.lag = m.lag;
                }
                out.add(v);
            }
            out.sort(Comparator.comparing(x -> x.ns));
            return out;
        } catch (Exception e) {
            log.warn("读 namespace 列表失败: {}", e.getMessage());
            return List.of();
        }
    }

    /** 创建 ns = 初始化 rules 键(空前缀 + 默认规则);已存在返回 false。 */
    public boolean createNamespace(String ns, ScaleRules rules, String operator, String reason) {
        checkNsName(ns);
        if (ensure().getBytes(KeySpace.rules(ns)).isPresent()) return false;
        ScaleRules r = rules != null ? rules : new ScaleRules();
        ensure().put(KeySpace.rules(ns), r);
        audit(ns, "CREATE_NS", operator, reason, "rules=" + Json.toStr(r));
        log.info("创建 namespace [{}] 默认规则落键 operator={}", ns, operator);
        return true;
    }

    /**
     * 删除 ns:前置校验无任务且无在线实例,否则返回残留描述(非 null = 拒绝);
     * 通过后清空前缀(审计落全局前缀,不随 ns 删除消失)。
     */
    public String deleteNamespace(String ns, String operator, String reason) {
        checkNsName(ns);
        int tasks = (int) countKeys(KeySpace.tasksPrefix(ns), key -> key.endsWith("/spec"));
        if (tasks > 0) return "仍有 " + tasks + " 个任务,请先注销任务";
        int insts = (int) countKeys(KeySpace.nsInstancesPrefix(ns),
                key -> !key.endsWith("/drain") && !key.endsWith("/paused") && !key.endsWith("/weight"));
        if (insts > 0) return "仍有 " + insts + " 个在线实例,请先下架实例";
        try {
            ensure().deletePrefix(ByteSequence.from("/dcp/" + ns + "/", StandardCharsets.UTF_8));
            audit(ns, "DELETE_NS", operator, reason, "前缀已清空");
            log.info("删除 namespace [{}] operator={}", ns, operator);
            return null;
        } catch (Exception e) {
            log.warn("删除 namespace 失败 ns={}: {}", ns, e.getMessage());
            return "清空前缀失败: " + e.getMessage();
        }
    }

    /** 全局审计(/dcp/audit/,不随 ns 删除消失;创建/删除 ns、改规则落这里)。 */
    public List<Map<String, Object>> listAudits() {
        List<Map<String, Object>> out = new ArrayList<>();
        try {
            for (byte[] v : ensure().getPrefix(KeySpace.auditPrefix()).values()) {
                Map<String, Object> a = Json.fromBytes(v, Map.class);
                if (a != null) out.add(a);
            }
            out.sort((a, b) -> Long.compare(((Number) b.getOrDefault("at", 0L)).longValue(),
                    ((Number) a.getOrDefault("at", 0L)).longValue()));
        } catch (Exception e) {
            log.warn("读全局审计失败: {}", e.getMessage());
        }
        return out;
    }

    private void audit(String ns, String action, String operator, String reason, String detail) {
        Map<String, Object> a = new LinkedHashMap<>();
        a.put("at", System.currentTimeMillis());
        a.put("ns", ns);
        a.put("action", action);
        a.put("operator", operator);
        a.put("reason", reason);
        a.put("detail", detail);
        ensure().put(KeySpace.audit(System.currentTimeMillis() + "-"
                + UUID.randomUUID().toString().substring(0, 8)), a);
    }

    private void checkNsName(String ns) {
        if (ns == null || !NS_NAME.matcher(ns).matches()) {
            throw new IllegalArgumentException("ns 名非法:仅小写字母开头,可含数字/下划线/中划线,最长 63");
        }
        if (RESERVED.contains(ns)) {
            throw new IllegalArgumentException("ns 名 " + ns + " 为全局保留前缀,不可用");
        }
    }

    private long countKeys(ByteSequence prefix, java.util.function.Predicate<String> filter) {
        try {
            return ensure().getPrefix(prefix).keySet().stream().filter(filter).count();
        } catch (Exception e) {
            return 0;
        }
    }

    private <T> T readOne(ByteSequence key, Class<T> type) {
        try {
            return ensure().getObj(key, type).orElse(null);
        } catch (Exception e) {
            log.warn("读调度监控键失败 {}: {}", key, e.getMessage());
            return null;
        }
    }

    private <T> List<T> readAll(ByteSequence prefix, Class<T> type, java.util.function.Predicate<String> filter) {
        try {
            List<T> out = new ArrayList<>();
            for (Map.Entry<String, byte[]> e : ensure().getPrefix(prefix).entrySet()) {
                if (!filter.test(e.getKey())) continue;
                T v = Json.fromBytes(e.getValue(), type);
                if (v != null) out.add(v);
            }
            return out;
        } catch (Exception e) {
            log.warn("读调度监控聚合失败 {}: {}", prefix, e.getMessage());
            return List.of();
        }
    }
}
