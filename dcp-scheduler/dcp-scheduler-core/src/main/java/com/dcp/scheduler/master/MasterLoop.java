package com.dcp.scheduler.master;

import com.dcp.scheduler.alloc.Allocator;
import com.dcp.scheduler.alloc.Autoscaler;
import com.dcp.scheduler.alloc.MetricsDeriver;
import com.dcp.scheduler.alloc.Rebalancer;
import com.dcp.scheduler.alloc.Scoring;
import com.dcp.scheduler.alert.AlertSink;
import com.dcp.scheduler.exec.HookRegistry;
import com.dcp.scheduler.lag.AdminLagSource;
import com.dcp.scheduler.lag.KafkaLagSource;
import com.dcp.scheduler.model.Enums;
import com.dcp.scheduler.model.ExecutorSnapshot;
import com.dcp.scheduler.model.InstanceRecord;
import com.dcp.scheduler.model.KafkaConfig;
import com.dcp.scheduler.model.NsInstanceRecord;
import com.dcp.scheduler.model.OpsRecord;
import com.dcp.scheduler.model.PartRecord;
import com.dcp.scheduler.model.RebalanceRules;
import com.dcp.scheduler.model.ScaleRules;
import com.dcp.scheduler.model.ScoringConfig;
import com.dcp.scheduler.model.TaskSpec;
import com.dcp.scheduler.model.TaskStatus;
import com.dcp.scheduler.store.EtcdStore;
import com.dcp.scheduler.store.Json;
import com.dcp.scheduler.store.KeySpace;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Watch;
import io.etcd.jetcd.watch.WatchEvent;
import io.etcd.jetcd.watch.WatchResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 调度主(总纲 §六 / 调度算法与决策循环子文档):竞选 + 决策循环(八环节)。
 *   触发 = 周期 tick 兑底 + 事件驱动加速(watch tasks/instances/ops,1s 节流,单线程串行)。
 *   决策:ensureTasks → expirePins → consumeOps → 分配(尊重锁) → 判死交接(尊重锁)。
 * 锁机制(子文档⑧):PIN/UNPIN/MIGRATE_PIN 走 ops 队列,归属键唯一写者始终是主。
 * 每 ns 一个实例,leader 键 value = 实例 id。
 */
public class MasterLoop implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(MasterLoop.class);

    /** 保守交接双窗口时长:≈ 注册租约/快照 TTL,确保旧主死亡与快照过期均被观察到。 */
    private static final long CONSERVATIVE_WINDOW_MS = 30_000;
    /** 快照活性窗口:与 SnapshotReporter 租约 TTL 对齐(续写行为即活性信号)。 */
    private static final long SNAPSHOT_TTL_MS = 30_000;
    /** 卡死宽限期:分配后多久内不判快照停更(给新执行器写首个快照的窗口)。 */
    private static final long CARD_GRACE_MS = 30_000;
    /** 新实例观察期(调度算法子文档 §六):注册不足该时长不纳入候选,防注册抖动。 */
    private static final long NEW_INSTANCE_GRACE_MS = 10_000;
    /** SUSPECT 观察态告警限流间隔(同一分区)。 */
    private static final long SUSPECT_WARN_INTERVAL_MS = 60_000;
    /** 追平 deadline(与 Autoscaler 对齐):注销/缩容追平超时告警,不自动回滚。 */
    private static final long DRAIN_DEADLINE_MS = 10 * 60_000;
    /** 告警巡检持续期(监控子文档 §六):lag 高位/错误率持续 3min 才触发。 */
    private static final long ALERT_PERSIST_MS = 3 * 60_000;
    /** 错误率告警阈值(监控子文档 §六缺省)。 */
    private static final double ERROR_RATE_HI = 0.01;
    /** 容量水位告警阈值(监控子文档 §六缺省)。 */
    private static final double WATER_HI = 0.9;

    private final String ns;
    private final String selfId;
    private final EtcdStore store;
    private volatile Scoring scoring;
    private final Allocator allocator;
    private final Rebalancer rebalancer;
    private final Autoscaler autoscaler;
    private final MetricsDeriver metricsDeriver;
    private final AlertSink alertSink;
    private final long decideSec;

    private final Map<String, TaskSpec> specCache = new ConcurrentHashMap<>();
    private final List<Watch.Watcher> decWatchers = new CopyOnWriteArrayList<>();
    private final ScheduledExecutorService timer;
    /** SUSPECT 告警限流(分区 → 上次告警时间)。 */
    private final Map<String, Long> suspectWarnAt = new ConcurrentHashMap<>();
    /** 下架迁空提示限流(同一实例 60s 一次)。 */
    private final Map<String, Long> drainedLogAt = new ConcurrentHashMap<>();
    /** 实例首次出现时间(新实例观察期)。 */
    private final Map<String, Long> instSeenAt = new ConcurrentHashMap<>();
    // ── 告警巡检内存态(持续期计数,换主重置) ──
    private final Map<String, Long> lagHighSince = new ConcurrentHashMap<>(); // taskId/partId → 首次高位
    private final Map<String, long[]> errPrev = new ConcurrentHashMap<>();    // taskId/partId → {errorCount, processedCount}
    private final Map<String, Long> errSince = new ConcurrentHashMap<>();     // taskId/partId → 首次超阈值
    private final Map<String, Long> pendingSince = new ConcurrentHashMap<>(); // taskId/partId → 首次 PENDING

    private volatile EtcdStore.LeaseHandle leaderLease;
    private volatile Watch.Watcher leaderWatch;
    private volatile boolean leader;
    private volatile boolean decideScheduled;
    private volatile boolean closed;
    private volatile long lastTriggerNs;

    // ── Kafka endOffset 探针(lag 实查;无源回退 rps 近似) ──
    private final KafkaLagSource injectedSource;   // 实例注入优先(smoke/测试用 mock)
    private volatile KafkaLagSource lagSource;     // 当前生效源(注入或懒建 Admin)
    private volatile AdminLagSource adminSource;   // 懒建的内置 Admin 实现
    private volatile String adminBootstrap;        // 懒建时对应的 bootstrapServers(变化重建)
    private final Map<String, Map<Integer, Long>> endOffsets = new HashMap<>();
    private long lastProbeAt;
    private long probeSec = 60;

    public MasterLoop(String ns, String selfId, EtcdStore store, long decideSec, HookRegistry hooks,
                      KafkaLagSource injectedLagSource) {
        this.ns = ns;
        this.selfId = selfId;
        this.store = store;
        this.injectedSource = injectedLagSource;
        this.scoring = Scoring.defaults();
        this.alertSink = new AlertSink(ns, store);
        this.allocator = new Allocator(ns, store, scoring);
        this.rebalancer = new Rebalancer(ns, store, scoring);
        this.autoscaler = new Autoscaler(ns, store, hooks, alertSink);
        this.metricsDeriver = new MetricsDeriver(ns, store);
        this.decideSec = decideSec;
        this.timer = Executors.newSingleThreadScheduledExecutor(
                r -> { Thread t = new Thread(r, "master-" + ns); t.setDaemon(true); return t; });
    }

    public void start() {
        campaign();
    }

    // ── 竞选 ──

    private synchronized void campaign() {
        if (closed) return;
        closeDecisionWatchers(); // 重新竞选场景:先关旧决策 watch
        try {
            leaderLease = store.grantLease(30);
            boolean won = store.tryCampaign(KeySpace.leader(ns), selfId, leaderLease.id);
            if (won) {
                leader = true;
                log.info("[{}] {} 当选调度主", ns, selfId);
                // 决策循环只 schedule 一次;重新竞选后复用(decide 内校验领导权)
                if (!decideScheduled) {
                    decideScheduled = true;
                    timer.scheduleAtFixedRate(this::decide, decideSec, decideSec, TimeUnit.SECONDS);
                }
                watchLeader(); // 监督自身 leader 键:租约断裂被删时重新竞选
                watchDecisionKeys(); // 事件驱动:任务/实例/ops 变化立即决策
                trigger(); // 当选立即跑一轮,不等 tick
            } else {
                log.info("[{}] {} 竞选失败,监督 leader 键", ns, selfId);
                leaderLease.close();
                leaderLease = null;
                watchLeader();
            }
        } catch (Exception e) {
            log.warn("[{}] 竞选异常: {}, {}s 后重试", ns, e.getMessage(), decideSec);
            if (leaderLease != null) { leaderLease.close(); leaderLease = null; }
            timer.schedule(this::campaign, decideSec, TimeUnit.SECONDS);
        }
    }

    private void watchLeader() {
        if (leaderWatch != null) leaderWatch.close();
        leaderWatch = store.watchKey(KeySpace.leader(ns), new EtcdStore.Listener() {
            @Override public void onEvent(WatchResponse r) {
                for (WatchEvent e : r.getEvents()) {
                    if (WatchEvent.EventType.DELETE.equals(e.getEventType())) {
                        leader = false;
                        log.info("[{}] leader 键删除,{} 重新竞选", ns, selfId);
                        timer.execute(MasterLoop.this::campaign);
                        return;
                    }
                }
            }
        });
    }

    /** 事件驱动:watch 决策相关键(快照写不触发,避免自激)。 */
    private void watchDecisionKeys() {
        decWatchers.add(store.watchPrefix(KeySpace.tasksPrefix(ns), new EtcdStore.Listener() {
            @Override public void onEvent(WatchResponse r) {
                for (WatchEvent e : r.getEvents()) {
                    String key = e.getKeyValue().getKey().toString(StandardCharsets.UTF_8);
                    if (!key.endsWith("/snap")) { trigger(); return; }
                }
            }
        }));
        decWatchers.add(store.watchPrefix(KeySpace.nsInstancesPrefix(ns),
                new EtcdStore.Listener() {
                    @Override public void onEvent(WatchResponse r) { trigger(); }
                }));
        decWatchers.add(store.watchPrefix(KeySpace.opsPrefix(ns),
                new EtcdStore.Listener() {
                    @Override public void onEvent(WatchResponse r) { trigger(); }
                }));
    }

    /** 1s 节流 + 单线程 timer 串行:并发事件合并为一轮决策。 */
    private void trigger() {
        if (closed || !leader) return;
        long now = System.nanoTime();
        if (now - lastTriggerNs < TimeUnit.SECONDS.toNanos(1)) return;
        lastTriggerNs = now;
        timer.execute(this::decide);
    }

    private void closeDecisionWatchers() {
        for (Watch.Watcher w : decWatchers) w.close();
        decWatchers.clear();
    }

    // ── 决策循环(八环节) ──

    private void decide() {
        if (closed) return;
        // 领导权校验:值不是自己 → 让位再竞选(防租约断裂后双主决策)
        Optional<String> cur = store.getLeader(KeySpace.leader(ns), String.class);
        if (cur.isEmpty() || !selfId.equals(cur.get())) {
            if (leader) { leader = false; log.warn("[{}] {} 失去领导权,重新竞选", ns, selfId); }
            campaign();
            return;
        }
        try {
            // ① 任务生命周期(含注销推进:删 spec → DRAINING → 分区追平 → GONE)
            Map<String, byte[]> allParts = readParts();
            ensureTasks(allParts);
            // ② TTL 锁到期自动解锁
            expirePins(allParts);
            // ③ 运维指令(钉/解/定向迁移/实例下架)
            consumeOps();
            allParts = readParts(); // ops 可能改了归属,重读
            // ④⑤ 判死与交接推进(尊重锁;双确认:lease 过期 + 快照失效)
            List<NsInstanceRecord> live = stableLive(readLiveInstances());
            Set<String> drainInsts = readDrainIntents();
            List<NsInstanceRecord> accepting = new ArrayList<>();
            for (NsInstanceRecord i : live) if (!drainInsts.contains(i.id)) accepting.add(i);
            int reassigned = judgeDead(allParts, live);
            // ⑥ 补全分配(下架实例不接新单;打分系数每轮热读)
            updateScoring();
            Map<String, InstanceRecord> metas = readMetas();
            Map<String, Integer> assigned = allocator.countAssigned(allParts);
            int allocated = allocator.allocate(filterState(allParts, Enums.PartState.PENDING), accepting, metas, assigned);
            if (reassigned > 0) { // 判死后立即补一轮分配,加速闭环
                allParts = readParts();
                assigned = allocator.countAssigned(allParts);
                allocated += allocator.allocate(filterState(allParts, Enums.PartState.PENDING), accepting, metas, assigned);
            }
            // ⑦ 再平衡(稳态均衡 + cpu 驱逐;故障接管优先,有判死时本轮跳过;规则每轮热读)
            int rebalanced = 0;
            if (reassigned == 0) {
                allParts = readParts();
                assigned = allocator.countAssigned(allParts);
                rebalancer.updateRules(store.getObj(KeySpace.rebalanceRules(ns), RebalanceRules.class).orElse(null));
                rebalanced = rebalancer.rebalance(filterState(allParts, Enums.PartState.ASSIGNED),
                        accepting, assigned, metas, readHandovers(), autoscaler.rpsSnapshot());
            }
            // ⑦.5 下架实例迁出(运维意图优先;每轮每实例 ≤1 分区)
            int instDrained = drainInstanceTick(allParts, live, accepting, drainInsts, metas, assigned);
            // ⑧ 扩缩容(水位判定同轮限稳态;DELETING 追平推进独立进行;追平优先 lag 实查,rps 回退)
            allParts = readParts();
            autoscaler.refreshRps(allParts);
            ensureLagSource();
            int drained = autoscaler.drainTick(allParts, probeEndOffsets(allParts, System.currentTimeMillis()));
            int scaled = 0;
            if (reassigned == 0) {
                scaled = autoscaler.evaluate(readRunningSpecs(), allParts);
            }
            // ⑧.5 告警巡检(监控子文档 §六规则):lag 高位/错误率/容量水位/PENDING 积压
            alertTick(allParts, live, probeEndOffsets(allParts, System.currentTimeMillis()));
            // ⑧.6 监控聚合(监控子文档 §二):分区差分派生 + 任务/实例/ns 三级聚合,后台直读
            metricsDeriver.derive(allParts, metas, live, autoscaler.rpsSnapshot(),
                    probeEndOffsets(allParts, System.currentTimeMillis()));
            if (allocated > 0 || reassigned > 0 || rebalanced > 0 || instDrained > 0 || scaled > 0 || drained > 0) {
                log.info("[{}] 决策轮:分配 {} 判死交接 {} 再平衡 {} 下架迁出 {} 扩缩 {} 追平清 {}",
                        ns, allocated, reassigned, rebalanced, instDrained, scaled, drained);
            }
        } catch (Exception e) {
            log.warn("[{}] 决策轮异常", ns, e);
        }
    }

    // ── Kafka lag 探针(监控子文档:endOffset 周期实查 + lag 派生) ──

    /** 无注入源时按 ns 配置(bootstrapServers)懒建/重建内置 Admin 实现;配置删除退源回退 rps。 */
    private void ensureLagSource() {
        if (injectedSource != null) {
            lagSource = injectedSource;
            return;
        }
        KafkaConfig cfg = store.getObj(KeySpace.kafkaConfig(ns), KafkaConfig.class).orElse(null);
        probeSec = cfg != null && cfg.probeSec != null ? cfg.probeSec : 60;
        String bs = cfg != null && cfg.bootstrapServers != null ? cfg.bootstrapServers.trim() : "";
        if (bs.isEmpty()) { // 配置缺失/被删:无源,追平判定回退 rps 近似
            if (adminSource != null) {
                adminSource.close();
                adminSource = null;
                adminBootstrap = null;
                lagSource = null;
                log.info("[{}] Kafka lag 探针已退源(配置缺失),追平判定回退 rps 近似", ns);
            }
            return;
        }
        if (adminSource == null || !bs.equals(adminBootstrap)) {
            AdminLagSource old = adminSource;
            adminSource = new AdminLagSource(bs);
            adminBootstrap = bs;
            lagSource = adminSource;
            if (old != null) old.close();
            log.info("[{}] Kafka lag 探针启用: bootstrapServers={} probeSec={}", ns, bs, probeSec);
        }
    }

    /**
     * 查 endOffset 缓存:常态按 probeSec 周期实查;存在 DELETING 分区时按需缩短到决策周期。
     * 返回 null = 本轮无 lag 数据(无源/查询失败),调用方回退 rps 近似。
     */
    private Map<String, Map<Integer, Long>> probeEndOffsets(Map<String, byte[]> allParts, long now) {
        KafkaLagSource src = lagSource;
        if (src == null) return null;
        Set<String> topics = new LinkedHashSet<>();
        boolean urgent = false;
        for (TaskSpec spec : specCache.values()) {
            if (spec.topic != null && !isVirtualTopic(spec.topic)) topics.add(spec.topic);
        }
        for (Map.Entry<String, byte[]> e : allParts.entrySet()) {
            PartRecord p = Json.fromBytes(e.getValue(), PartRecord.class);
            if (p != null && Enums.PartState.DELETING.equals(p.state) && p.drain != null
                    && p.drain.topic != null && !isVirtualTopic(p.drain.topic)) {
                urgent = true;
                topics.add(p.drain.topic);
            }
        }
        long intervalMs = urgent ? Math.min(5_000, decideSec * 1000) : probeSec * 1000;
        if (now - lastProbeAt < intervalMs) {
            synchronized (endOffsets) { return endOffsets.isEmpty() ? null : endOffsets; }
        }
        try {
            Map<String, Map<Integer, Long>> fresh = new HashMap<>();
            for (String t : topics) fresh.put(t, src.endOffsets(t));
            synchronized (endOffsets) {
                endOffsets.clear();
                endOffsets.putAll(fresh);
            }
            lastProbeAt = now;
            return fresh;
        } catch (Exception ex) {
            lastProbeAt = now; // 失败也限频,防每轮重试打满日志
            log.warn("[{}] endOffset 实查失败(本轮回退 rps 判定): {}", ns, ex.getMessage());
            return null;
        }
    }

    /**
     * ⑧.5 告警巡检(监控子文档 §六规则):lag 高位/错误率/容量水位/PENDING 积压;
     * 状态翻转时 alertSink.resolve 补恢复。持续期用首现时间戳(持续 3min 才触发)。
     */
    private void alertTick(Map<String, byte[]> allParts, List<NsInstanceRecord> live,
                           Map<String, Map<Integer, Long>> endOffsets) {
        long now = System.currentTimeMillis();
        Map<String, Double> rps = autoscaler.rpsSnapshot();
        // ── 分区级:lag 高位 / 错误率 ──
        for (Map.Entry<String, byte[]> e : allParts.entrySet()) {
            if (!isPartKey(e.getKey())) continue;
            PartRecord p = parseQuiet(e.getValue());
            if (p == null || !Enums.PartState.ASSIGNED.equals(p.state)) continue;
            String taskId = KeySpace.taskIdOfPartKey(e.getKey());
            String k = taskId + "/" + p.partId;
            ExecutorSnapshot snap = store.getObj(KeySpace.snap(ns, taskId, p.partId), ExecutorSnapshot.class)
                    .orElse(null);
            if (snap == null) continue;
            // lag 高位:lag > max(100, rps×5) 持续 3min(避免 rps 过小噪声)
            Long lag = lagOf(taskId, p.partId, snap, endOffsets);
            if (lag != null && lag > Math.max(100, rps.getOrDefault(k, 0.0) * 5)) {
                long first = lagHighSince.getOrDefault(k, now);
                lagHighSince.put(k, first);
                if (now - first >= ALERT_PERSIST_MS) {
                    alertSink.fire("warning", "LAG_HIGH", p.owner, taskId, p.partId,
                            "分区 lag=" + lag + " 条(rps=" + String.format("%.1f", rps.getOrDefault(k, 0.0))
                                    + "),持续高位,观察扩缩决策");
                }
            } else {
                lagHighSince.remove(k);
                alertSink.resolve("LAG_HIGH", p.owner, taskId, p.partId, "lag 回落");
            }
            // 错误率:相邻帧差分 ΔerrorCount/ΔprocessedCount > 1% 持续 3min
            long[] prev = errPrev.get(k);
            if (prev != null && snap.processedCount > prev[1]) {
                long dCount = snap.processedCount - prev[1];
                long dErr = Math.max(0, snap.errorCount - prev[0]);
                if ((double) dErr / dCount > ERROR_RATE_HI) {
                    long first = errSince.getOrDefault(k, now);
                    errSince.put(k, first);
                    if (now - first >= ALERT_PERSIST_MS) {
                        alertSink.fire("critical", "ERROR_RATE", p.owner, taskId, p.partId,
                                "错误率 " + String.format("%.2f%%", 100.0 * dErr / dCount)
                                        + " 超阈值 1%,检查业务逻辑/数据质量");
                    }
                } else {
                    errSince.remove(k);
                    alertSink.resolve("ERROR_RATE", p.owner, taskId, p.partId, "错误率回落");
                }
            }
            errPrev.put(k, new long[]{snap.errorCount, snap.processedCount});
        }
        // ── 任务级:容量水位 > 0.9 且分区已满配 ──
        for (TaskSpec spec : specCache.values()) {
            int parts = 0;
            double flow = 0;
            for (String key : allParts.keySet()) {
                if (key.contains("/tasks/" + spec.taskId + "/parts/")) {
                    parts++;
                    flow += rps.getOrDefault(spec.taskId + "/" + KeySpace.partIdOf(key), 0.0);
                }
            }
            if (parts == 0) continue;
            double water = spec.targetRps > 0 ? flow / (parts * spec.targetRps) : 0;
            if (water > WATER_HI && parts >= spec.maxPartitions) {
                alertSink.fire("critical", "CAPACITY_FULL", null, spec.taskId, null,
                        "水位=" + String.format("%.2f", water) + " 且分区已满配(" + parts + "/"
                                + spec.maxPartitions + "),业务上调 maxPartitions");
            } else {
                alertSink.resolve("CAPACITY_FULL", null, spec.taskId, null, "水位回落或未满配");
            }
        }
        // ── PENDING 积压:>3 决策周期未分配 → 无候选实例 / 有候选迁移失败 ──
        Set<String> pendingKeys = new HashSet<>();
        Map<String, Integer> assigned = allocator.countAssigned(allParts);
        for (Map.Entry<String, byte[]> e : allParts.entrySet()) {
            if (!isPartKey(e.getKey())) continue;
            PartRecord p = parseQuiet(e.getValue());
            if (p == null || !Enums.PartState.PENDING.equals(p.state)) continue;
            String taskId = KeySpace.taskIdOfPartKey(e.getKey());
            String k = taskId + "/" + p.partId;
            pendingKeys.add(k);
            long first = pendingSince.getOrDefault(k, now);
            pendingSince.put(k, first);
            if (now - first < 3L * decideSec * 1000) continue;
            boolean hasCandidate = false;
            for (NsInstanceRecord i : live) {
                if (i.acceptable() && assigned.getOrDefault(i.id, 0) < i.capacity) {
                    hasCandidate = true;
                    break;
                }
            }
            if (hasCandidate) {
                alertSink.fire("critical", "MIGRATE_TIMEOUT", null, taskId, p.partId,
                        "PENDING 超过 3 决策周期未分配(存在候选实例),检查分配链路");
            } else {
                alertSink.fire("critical", "NO_INSTANCE", null, taskId, p.partId,
                        "PENDING 超过 3 决策周期且无可用实例,扩容实例池");
            }
        }
        for (String k : new HashSet<>(pendingSince.keySet())) {
            if (!pendingKeys.contains(k)) { // 已分配/已删:恢复
                pendingSince.remove(k);
                String[] kv = k.split("/", 2);
                alertSink.resolve("MIGRATE_TIMEOUT", null, kv[0], Integer.parseInt(kv[1]), "分区已分配");
                alertSink.resolve("NO_INSTANCE", null, kv[0], Integer.parseInt(kv[1]), "分区已分配");
            }
        }
    }

    /** 打分系数热读(/dcp/{ns}/scoring,缺省 0.4/0.3/0.2/0.1;变化时重建并注入分配/再平衡)。 */
    private void updateScoring() {
        ScoringConfig cfg = store.getObj(KeySpace.scoringConfig(ns), ScoringConfig.class).orElse(null);
        Scoring s = cfg == null ? Scoring.defaults()
                : new Scoring(cfg.alpha != null ? cfg.alpha : 0.4,
                        cfg.beta != null ? cfg.beta : 0.3,
                        cfg.gamma != null ? cfg.gamma : 0.2,
                        cfg.delta != null ? cfg.delta : 0.1);
        if (!s.same(scoring)) {
            scoring = s;
            allocator.setScoring(s);
            rebalancer.setScoring(s);
            log.info("[{}] 打分系数更新: α/β/γ/δ 已生效", ns);
        }
    }

    /** 计算 ASSIGNED 分区 lag(条);null = lag 数据不可用(无 Kafka 源/无 topic/无快照)。 */
    private Long lagOf(String taskId, int partId, ExecutorSnapshot snap,
                       Map<String, Map<Integer, Long>> endOffsets) {
        if (endOffsets == null) return null;
        TaskSpec spec = specCache.get(taskId);
        if (spec == null || spec.topic == null) return null;
        Map<Integer, Long> eo = endOffsets.get(spec.topic);
        if (eo == null) return null;
        Long end = eo.get(partId);
        if (end == null) return null;
        return Math.max(0, end - snap.committedOffset);
    }

    /** ① 任务生命周期:spec 存在 → 上线/补分区;spec 删除 → 注销推进(生命周期子文档 3.3)。 */
    private void ensureTasks(Map<String, byte[]> allParts) {
        specCache.clear(); // 每轮重读,SOFT 变更及时生效
        // 任务全集:spec/status/parts 任一存在的 taskId(spec 删除后靠 status 感知注销)
        Set<String> taskIds = new LinkedHashSet<>();
        for (String key : store.getPrefix(KeySpace.tasksPrefix(ns)).keySet()) {
            if (key.endsWith("/spec")) taskIds.add(KeySpace.taskIdOfSpecKey(key));
            else if (key.endsWith("/status")) taskIds.add(KeySpace.taskIdOfStatusKey(key));
            else if (isPartKey(key)) taskIds.add(KeySpace.taskIdOfPartKey(key));
        }
        for (String taskId : taskIds) {
            if (store.getObj(KeySpace.spec(ns, taskId), TaskSpec.class).isPresent()) {
                ensureTaskOnline(taskId, allParts);
            } else {
                drainTaskProgress(taskId, allParts); // 外壳删 spec = 注销
            }
        }
    }
    
    /**
     * 任务在线路径(spec 存在),按 spec.phase 分流:
     *  BATCHING = 批阶段:只登记 SUBMITTED,不补分区不分配(等外壳推进);
     *  PAUSED   = 任务级暂停:PENDING 删键、ASSIGNED/MIGRATING 置 DELETING 追平回收(恢复后 ensureParts 补回);
     *  null     = RUNNING:SUBMITTED → 上线补 PENDING;RUNNING → 按期望分区数补缺失 + 外壳扩 Kafka 对账;
     *  注销后复活:DRAINING/GONE + spec 重现 → 重置 SUBMITTED 重新上线(停止→重启闭环)。
     */
    private void ensureTaskOnline(String taskId, Map<String, byte[]> allParts) {
        TaskSpec spec = store.getObj(KeySpace.spec(ns, taskId), TaskSpec.class).orElse(null);
        if (spec == null) return;
        TaskStatus status = store.getObj(KeySpace.status(ns, taskId), TaskStatus.class)
                .orElseGet(() -> TaskStatus.of(Enums.TaskState.SUBMITTED, "auto"));
        // 一次性任务终态(F1): 槽位已释放, 挂起等外壳重新提交(覆盖 spec + 删 status → 重置 SUBMITTED)
        if (Enums.TaskState.SUCCEEDED.equals(status.state) || Enums.TaskState.FAILED.equals(status.state)) {
            return;
        }
        if (Enums.TaskState.GONE.equals(status.state) || Enums.TaskState.DRAINING.equals(status.state)) {
            status.state = Enums.TaskState.SUBMITTED;
            status.since = System.currentTimeMillis();
            status.reason = "spec 重新提交(重启)";
            status.reassigns = 0;
            store.put(KeySpace.status(ns, taskId), status);
            log.info("[{}] 任务 {} 注销后重新上线(重置 SUBMITTED)", ns, taskId);
        }
        if (spec.batching()) {
            if (Enums.TaskState.SUBMITTED.equals(status.state)) {
                store.put(KeySpace.status(ns, taskId), status); // 登记 SUBMITTED,框架可观察
            }
            return; // 批阶段不分配,等外壳推进
        }
        if (spec.paused()) {
            pauseTaskProgress(taskId, allParts, spec);
            return;
        }
        if (Enums.TaskState.SUBMITTED.equals(status.state)) {
            status.state = Enums.TaskState.RUNNING;
            status.since = System.currentTimeMillis();
            status.reason = "ready";
            status.expectedParts = spec.partitions;
            status.lastSpecPartitions = spec.partitions;
            status.topic = spec.topic;
            status.reassigns = 0;
            store.put(KeySpace.status(ns, taskId), status);
            ensureParts(spec, taskId, spec.partitions);
            log.info("[{}] 任务 {} 上线:{} 个分区待分配", ns, taskId, spec.partitions);
        } else if (Enums.TaskState.RUNNING.equals(status.state)) {
            int expected = status.expectedParts != null ? status.expectedParts : spec.partitions;
            int lastSpec = status.lastSpecPartitions != null ? status.lastSpecPartitions : spec.partitions;
            if (spec.partitions > lastSpec) {
                // 外壳扩了 Kafka:spec.partitions 变大 → 期望分区数对账上调(扩缩容子文档 §三)
                expected = spec.partitions;
                status.expectedParts = expected;
                status.lastSpecPartitions = spec.partitions;
                store.put(KeySpace.status(ns, taskId), status);
                log.info("[{}] 任务 {} 对账:spec.partitions {} 上调期望分区数 → {}",
                        ns, taskId, spec.partitions, expected);
            }
            ensureParts(spec, taskId, expected);
        }
        // DRAINING/GONE:spec 重新出现 = 重启(上方已重置 SUBMITTED 落入分支)
    }

    /** 任务级暂停:ASSIGNED/MIGRATING 置 DELETING 追平回收,PENDING 直接删键(恢复时 ensureParts 补回)。 */
    private void pauseTaskProgress(String taskId, Map<String, byte[]> allParts, TaskSpec spec) {
        long now = System.currentTimeMillis();
        for (Map.Entry<String, byte[]> e : allParts.entrySet()) {
            if (!isPartKey(e.getKey()) || !taskId.equals(KeySpace.taskIdOfPartKey(e.getKey()))) continue;
            PartRecord p = parseQuiet(e.getValue());
            if (p == null) continue;
            if (Enums.PartState.PENDING.equals(p.state)) {
                if (store.casDelete(KeySpace.part(ns, taskId, p.partId), e.getValue())) {
                    log.info("[{}] 暂停删 PENDING {}/{}", ns, taskId, p.partId);
                }
            } else if (Enums.PartState.ASSIGNED.equals(p.state) || Enums.PartState.MIGRATING.equals(p.state)) {
                PartRecord next = copy(p);
                next.state = Enums.PartState.DELETING;
                next.drain = new PartRecord.DrainState();
                next.drain.reason = "pause-task";
                next.drain.deadline = now + DRAIN_DEADLINE_MS;
                next.drain.topic = spec.topic;
                next.handover = null; // 交接中间态直接覆盖,追平由 owner 线程完成
                if (store.casPut(KeySpace.part(ns, taskId, p.partId), e.getValue(), next, null)) {
                    log.info("[{}] 暂停置 DELETING {}/{} 等待追平回收", ns, taskId, p.partId);
                }
            }
            // DELETING:追平清记录由 Autoscaler.drainTick(环节⑧)推进
        }
    }
    
    /** 注销推进:删 spec → DRAINING → PENDING 直删、ASSIGNED/MIGRATING 置 DELETING 追平 → 全清 → GONE。 */
    private void drainTaskProgress(String taskId, Map<String, byte[]> allParts) {
        TaskStatus status = store.getObj(KeySpace.status(ns, taskId), TaskStatus.class).orElse(null);
        if (status == null || Enums.TaskState.GONE.equals(status.state)) return;
        long now = System.currentTimeMillis();
        if (Enums.TaskState.RUNNING.equals(status.state) || Enums.TaskState.SUBMITTED.equals(status.state)) {
            status.state = Enums.TaskState.DRAINING;
            status.since = now;
            status.reason = "spec 已删除";
            store.put(KeySpace.status(ns, taskId), status);
            log.info("[{}] 任务 {} 进入注销 DRAINING(spec 已删除)", ns, taskId);
        }
        // DRAINING 推进:PENDING 直接删(无执行器);ASSIGNED/MIGRATING 置 DELETING(owner 线程继续追平)
        int remaining = 0;
        for (Map.Entry<String, byte[]> e : allParts.entrySet()) {
            if (!isPartKey(e.getKey()) || !taskId.equals(KeySpace.taskIdOfPartKey(e.getKey()))) continue;
            PartRecord p = parseQuiet(e.getValue());
            if (p == null) continue;
            remaining++;
            if (Enums.PartState.PENDING.equals(p.state)) {
                if (store.casDelete(KeySpace.part(ns, taskId, p.partId), e.getValue())) {
                    remaining--;
                    log.info("[{}] 注销删 PENDING {}/{}", ns, taskId, p.partId);
                }
            } else if (Enums.PartState.ASSIGNED.equals(p.state) || Enums.PartState.MIGRATING.equals(p.state)) {
                PartRecord next = copy(p);
                next.state = Enums.PartState.DELETING;
                next.drain = new PartRecord.DrainState();
                next.drain.reason = "drain-task";
                next.drain.deadline = now + DRAIN_DEADLINE_MS;
                next.drain.topic = status.topic; // spec 已删,靠 status 记忆的 topic 查 lag
                next.handover = null; // 交接中间态直接覆盖,追平由 owner 线程完成
                if (store.casPut(KeySpace.part(ns, taskId, p.partId), e.getValue(), next, null)) {
                    log.info("[{}] 注销置 DELETING {}/{} 等待追平", ns, taskId, p.partId);
                }
            }
            // DELETING:追平清记录由 Autoscaler.drainTick(环节⑧)推进
        }
        if (remaining == 0) {
            status.state = Enums.TaskState.GONE;
            status.since = now;
            status.reason = "全部分区追平清理";
            store.put(KeySpace.status(ns, taskId), status);
            Map<String, Object> ev = new HashMap<>();
            ev.put("type", "task-gone");
            ev.put("at", now);
            store.put(KeySpace.taskEvent(ns, taskId, "gone"), ev);
            log.info("[{}] 任务 {} 注销完成 GONE(外壳 watch 后可释放业务资源)", ns, taskId);
        }
    }
    
    /** 补 PENDING 分区到期望分区数(createIfAbsent 持久键,不带租约;缩容清记录的 尾部 id 不会被补回)。 */
    private void ensureParts(TaskSpec spec, String taskId, int expected) {
        for (int p = 0; p < expected; p++) {
            PartRecord rec = new PartRecord();
            rec.partId = p;
            store.createIfAbsent(KeySpace.part(ns, taskId, p), rec, null);
        }
    }
    
    /** ② TTL 锁到期 → CAS 清 pin,分区回到自动管理(子文档⑧ §4.1)。 */
    private void expirePins(Map<String, byte[]> allParts) {
        long now = System.currentTimeMillis();
        for (Map.Entry<String, byte[]> e : allParts.entrySet()) {
            PartRecord p = parseQuiet(e.getValue());
            if (p == null || p.pin == null || p.pin.expiresAt == null) continue;
            if (p.pin.expiresAt > now) continue;
            PartRecord next = copy(p);
            next.pin = null;
            String taskId = KeySpace.taskIdOfPartKey(e.getKey());
            if (store.casPut(KeySpace.part(ns, taskId, p.partId), e.getValue(), next, null)) {
                log.info("[{}] TTL 锁到期自动解锁 {}/{}", ns, taskId, p.partId);
            }
        }
    }

    // ── ③ ops 消费(子文档⑧ §六) ──

    private void consumeOps() {
        for (Map.Entry<String, byte[]> e : store.getPrefix(KeySpace.opsPrefix(ns)).entrySet()) {
            OpsRecord op = Json.fromBytes(e.getValue(), OpsRecord.class);
            if (!OpsRecord.OpState.PENDING.equals(op.state)) continue;
            boolean instOp = OpsRecord.Type.DRAIN_INSTANCE.equals(op.type)
                    || OpsRecord.Type.UNDRAIN_INSTANCE.equals(op.type)
                    || OpsRecord.Type.PAUSE_INSTANCE.equals(op.type)
                    || OpsRecord.Type.UNPAUSE_INSTANCE.equals(op.type)
                    || OpsRecord.Type.SET_WEIGHT.equals(op.type);
            boolean rulesOp = OpsRecord.Type.SET_RULES.equals(op.type);
            if (!instOp && !rulesOp && (op.taskId == null || op.partId == null)) {
                failOp(e, op, "缺 taskId/partId");
                continue;
            }
            switch (op.type == null ? "" : op.type) {
                case OpsRecord.Type.PIN -> doPin(e, op);
                case OpsRecord.Type.UNPIN -> doUnpin(e, op);
                case OpsRecord.Type.MIGRATE_PIN -> doMigratePin(e, op);
                case OpsRecord.Type.DRAIN_INSTANCE -> doDrainInstance(e, op);
                case OpsRecord.Type.UNDRAIN_INSTANCE -> doUndrainInstance(e, op);
                case OpsRecord.Type.PAUSE_INSTANCE -> doPauseInstance(e, op);
                case OpsRecord.Type.UNPAUSE_INSTANCE -> doUnpauseInstance(e, op);
                case OpsRecord.Type.SET_WEIGHT -> doSetWeight(e, op);
                case OpsRecord.Type.SET_RULES -> doSetRules(e, op);
                default -> failOp(e, op, "未知类型 " + op.type);
            }
        }
    }

    /** PIN:钉住当前 owner,可选 TTL。 */
    private void doPin(Map.Entry<String, byte[]> e, OpsRecord op) {
        Optional<byte[]> cur = store.getBytes(KeySpace.part(ns, op.taskId, op.partId));
        if (cur.isEmpty()) { failOp(e, op, "分区不存在"); return; }
        PartRecord p = parseQuiet(cur.get());
        if (p == null) { failOp(e, op, "归属记录损坏"); return; }
        if (!Enums.PartState.ASSIGNED.equals(p.state) || p.owner == null) {
            failOp(e, op, "分区未分配,无法钉住(state=" + p.state + ")");
            return;
        }
        PartRecord next = copy(p);
        next.pin = new PartRecord.Pin();
        next.pin.pinnedTo = p.owner;
        next.pin.pinnedAt = System.currentTimeMillis();
        next.pin.expiresAt = op.ttlSec == null ? null : System.currentTimeMillis() + op.ttlSec * 1000;
        next.pin.reason = op.reason;
        if (store.casPut(KeySpace.part(ns, op.taskId, op.partId), cur.get(), next, null)) {
            log.info("[{}] 钉住分区 {}/{} → {}(ttl={}s)", ns, op.taskId, op.partId, p.owner, op.ttlSec);
            doneOp(e);
        } else {
            failOp(e, op, "CAS 冲突,请重试");
        }
    }

    /** UNPIN:解除锁,恢复算法管理。 */
    private void doUnpin(Map.Entry<String, byte[]> e, OpsRecord op) {
        Optional<byte[]> cur = store.getBytes(KeySpace.part(ns, op.taskId, op.partId));
        if (cur.isEmpty()) { doneOp(e); return; } // 分区已不存在,视为完成
        PartRecord p = parseQuiet(cur.get());
        if (p == null || p.pin == null) { doneOp(e); return; } // 无锁,幂等完成
        PartRecord next = copy(p);
        next.pin = null;
        if (store.casPut(KeySpace.part(ns, op.taskId, op.partId), cur.get(), next, null)) {
            log.info("[{}] 解锁分区 {}/{}", ns, op.taskId, op.partId);
            doneOp(e);
        } else {
            failOp(e, op, "CAS 冲突,请重试");
        }
    }

    /** MIGRATE_PIN:定向迁移到 target 并钉住(复用 MIGRATING 状态机,子文档⑧ §五)。 */
    private void doMigratePin(Map.Entry<String, byte[]> e, OpsRecord op) {
        Optional<byte[]> cur = store.getBytes(KeySpace.part(ns, op.taskId, op.partId));
        if (cur.isEmpty()) { failOp(e, op, "分区不存在"); return; }
        PartRecord p = parseQuiet(cur.get());
        if (p == null) { failOp(e, op, "归属记录损坏"); return; }
        if (!Enums.PartState.ASSIGNED.equals(p.state) || p.owner == null) {
            failOp(e, op, "分区未分配,无法迁移(state=" + p.state + ")");
            return;
        }
        if (op.target == null || p.owner.equals(op.target)) {
            failOp(e, op, "目标实例无效或与当前 owner 相同");
            return;
        }
        // 校验目标存活且可接单
        boolean targetOk = false;
        for (NsInstanceRecord i : readLiveInstances()) {
            if (op.target.equals(i.id) && i.acceptable()) { targetOk = true; break; }
        }
        if (!targetOk) { failOp(e, op, "目标实例 " + op.target + " 不存活或不可接单"); return; }
        PartRecord next = copy(p);
        next.state = Enums.PartState.MIGRATING;
        next.handover = new PartRecord.HandoverState();
        next.handover.sourceOwner = p.owner;
        next.handover.confirmDeadline = System.currentTimeMillis() + CONSERVATIVE_WINDOW_MS;
        next.pin = new PartRecord.Pin();
        next.pin.pinnedTo = op.target;
        next.pin.pinnedAt = System.currentTimeMillis();
        next.pin.expiresAt = null; // 迁移锁:交接完成后保留(钉在目标)
        next.pin.reason = op.reason;
        if (store.casPut(KeySpace.part(ns, op.taskId, op.partId), cur.get(), next, null)) {
            log.info("[{}] 定向迁移开启 {}/{} {} → {} 并钉住", ns, op.taskId, op.partId, p.owner, op.target);
            doneOp(e);
        } else {
            failOp(e, op, "CAS 冲突,请重试");
        }
    }

    /** 实例下架(DRAIN_INSTANCE):写下架意图持久键;实例不再接新单,已有分区由下架迁出环节逐步迁移。 */
    private void doDrainInstance(Map.Entry<String, byte[]> e, OpsRecord op) {
        if (op.target == null) {
            failOp(e, op, "缺实例 id(target)");
            return;
        }
        Map<String, Object> intent = new HashMap<>();
        intent.put("at", System.currentTimeMillis());
        intent.put("operator", op.operator);
        intent.put("reason", op.reason);
        store.put(KeySpace.instDrainIntent(ns, op.target), intent);
        log.info("[{}] 实例 {} 下架意图登记(不接新单,逐步迁出)", ns, op.target);
        doneOp(e);
    }

    /** 取消下架(UNDRAIN_INSTANCE):删意图键;已迁出分区不回流(运维后台 §2.3 定论)。 */
    private void doUndrainInstance(Map.Entry<String, byte[]> e, OpsRecord op) {
        if (op.target == null) {
            failOp(e, op, "缺实例 id(target)");
            return;
        }
        store.delete(KeySpace.instDrainIntent(ns, op.target));
        log.info("[{}] 实例 {} 取消下架(已迁出分区不回流)", ns, op.target);
        doneOp(e);
    }

    /** 暂停接单(PAUSE_INSTANCE):写暂停意图持久键;不接新单,已跑分区不动(运维后台 §2 定论)。 */
    private void doPauseInstance(Map.Entry<String, byte[]> e, OpsRecord op) {
        if (op.target == null) {
            failOp(e, op, "缺实例 id(target)");
            return;
        }
        Map<String, Object> intent = new HashMap<>();
        intent.put("at", System.currentTimeMillis());
        intent.put("operator", op.operator);
        intent.put("reason", op.reason);
        store.put(KeySpace.instPauseIntent(ns, op.target), intent);
        log.info("[{}] 实例 {} 暂停接单(已跑分区不动)", ns, op.target);
        doneOp(e);
    }

    /** 恢复接单(UNPAUSE_INSTANCE):删暂停意图键。 */
    private void doUnpauseInstance(Map.Entry<String, byte[]> e, OpsRecord op) {
        if (op.target == null) {
            failOp(e, op, "缺实例 id(target)");
            return;
        }
        store.delete(KeySpace.instPauseIntent(ns, op.target));
        log.info("[{}] 实例 {} 恢复接单", ns, op.target);
        doneOp(e);
    }

    /** 调权重(SET_WEIGHT):写调权意图键(0.5~2.0),覆盖注册键 weight,影响目标打分。 */
    private void doSetWeight(Map.Entry<String, byte[]> e, OpsRecord op) {
        if (op.target == null || op.weight == null) {
            failOp(e, op, "缺实例 id(target) 或 weight");
            return;
        }
        double w = Math.max(0.5, Math.min(2.0, op.weight)); // 限幅 0.5~2.0(运维后台 §2 定论)
        Map<String, Object> intent = new HashMap<>();
        intent.put("weight", w);
        intent.put("at", System.currentTimeMillis());
        intent.put("operator", op.operator);
        intent.put("reason", op.reason);
        store.put(KeySpace.instWeightIntent(ns, op.target), intent);
        log.info("[{}] 实例 {} 调权 → {}", ns, op.target, w);
        doneOp(e);
    }

    /** 改默认规则(SET_RULES):范围校验后整包覆盖 ns 扩缩规则键,下一决策轮热生效。 */
    private void doSetRules(Map.Entry<String, byte[]> e, OpsRecord op) {
        ScaleRules r = op.rules;
        if (r == null) {
            failOp(e, op, "缺 rules 参数");
            return;
        }
        // M1: 字段装箱化后整包覆盖语义要求全字段显式配置, null 视为缺失
        if (r.up == null || r.down == null || r.upPeriods == null || r.downPeriods == null
                || r.cooldownMinutes == null || r.maxStep == null || r.downEnabled == null) {
            failOp(e, op, "rules 字段缺失(整包覆盖需全字段: up/down/upPeriods/downPeriods/cooldownMinutes/maxStep/downEnabled)");
            return;
        }
        if (r.up <= 0 || r.up > 1 || r.down < 0 || r.down >= 1 || r.up <= r.down) {
            failOp(e, op, "水位非法(需 0 < down < up ≤ 1)");
            return;
        }
        if (r.upPeriods < 1 || r.downPeriods < 1 || r.cooldownMinutes < 1) {
            failOp(e, op, "周期/冷却需为正整数");
            return;
        }
        if (r.maxStep < 1 || r.maxStep > 3) {
            failOp(e, op, "步长护栏 maxStep 需在 [1, 3]");
            return;
        }
        store.put(KeySpace.rules(ns), r);
        log.info("[{}] ns 默认扩缩规则更新: up={} down={} upPeriods={} downPeriods={} cooldown={}min maxStep={} downEnabled={}",
                ns, r.up, r.down, r.upPeriods, r.downPeriods, r.cooldownMinutes, r.maxStep, r.downEnabled);
        doneOp(e);
    }

    private void doneOp(Map.Entry<String, byte[]> e) {
        store.delete(ByteSequence.from(e.getKey(), StandardCharsets.UTF_8)); // 成功即移除(审计由后台侧记录)
    }

    private void failOp(Map.Entry<String, byte[]> e, OpsRecord op, String reason) {
        op.state = OpsRecord.OpState.FAILED;
        op.detail = reason;
        store.put(ByteSequence.from(e.getKey(), StandardCharsets.UTF_8), op); // 保留 FAILED 供后台展示
        log.warn("[{}] op {} 执行失败: {}", ns, op.opId, reason);
    }

    // ── ④⑤ 判死与交接(尊重锁 + 双确认) ──

    /**
     * 判死与交接(故障处理子文档 §一/§三 + 锁机制子文档⑧):
     *   - 双确认判死:实例 lease 过期 且 快照失效才确认死亡;lease 过期但快照仍在更新 → SUSPECT
     *     观察(防网络抖动/GC 停顿误判造成双消费);
     *   - 卡死检测:实例活着但分区快照停更超过宽限期 → 按模式迁移该分区(reason=stuck);
     *   - 锁裁决:钉住且锁未过期,钉住目标失联/卡死 → 不回收,限流告警等人工;
     *   - 手动迁移/再平衡(MIGRATING 且 pin 指向非源)不回滚,源停(stopped 快照)即推进;
     *   - 回滚条件按交接起因:death 看源 lease 复活;stuck 看源快照恢复。
     */
    private int judgeDead(Map<String, byte[]> allParts, List<NsInstanceRecord> live) {
        Set<String> liveIds = new HashSet<>();
        for (NsInstanceRecord i : live) liveIds.add(i.id);
        int n = 0;
        for (Map.Entry<String, byte[]> e : allParts.entrySet()) {
            String key = e.getKey();
            if (!isPartKey(key)) continue;
            PartRecord p = parseQuiet(e.getValue());
            if (p == null) continue;
            String taskId = KeySpace.taskIdOfPartKey(key);
            // 手动迁移/再平衡(MIGRATING 且 pin 指向非源):源存活是常态,源主动停(stopped 快照)即推进,
            // 不依赖判死;双窗口到期兜底。迁移不回滚。
            if (Enums.PartState.MIGRATING.equals(p.state) && p.handover != null && p.pin != null
                    && !p.pin.pinnedTo.equals(p.handover.sourceOwner)) {
                if (System.currentTimeMillis() >= p.handover.confirmDeadline
                        || stoppedConfirmed(taskId, p.partId)) {
                    if (toPending(p, e, taskId)) n++;
                }
                continue;
            }
            int snap = p.owner == null ? 0 : snapState(taskId, p.partId);
            boolean ownerAlive = p.owner != null && liveIds.contains(p.owner);
            boolean once = isOnceTask(taskId);
            if (Enums.PartState.ASSIGNED.equals(p.state)) {
                if (!ownerAlive) {
                    // 双确认:lease 过期但快照仍在更新 → SUSPECT 观察,不判死(防误判双消费)
                    if (snap == 1) {
                        suspectWarn(taskId, p.partId, p.owner, "实例 lease 过期但快照仍在更新");
                        continue;
                    }
                    // 锁裁决:钉住且未过期,死在钉住目标上 → 保持不回收(等 TTL 到期或人工)
                    if (p.locked() && p.pin.pinnedTo.equals(p.owner)) {
                        suspectWarn(taskId, p.partId, p.owner, "钉住实例失联,锁未过期,保持不回收等待人工裁决");
                        continue;
                    }
                    // F3 一次性任务:重派达上限 → FAILED 终态(不再重派)
                    if (once && maxAttemptsReached(taskId)) {
                        failOnceTask(p, e, taskId);
                        n++;
                        continue;
                    }
                    int opened = openHandoverOrReclaim(p, e, taskId, "death");
                    if (opened > 0 && once) recordReassign(taskId);
                    n += opened;
                } else {
                    alertSink.resolve("SUSPECT", p.owner, taskId, p.partId, "实例已恢复");
                    if (snap == 1) {
                        alertSink.resolve("EXEC_STUCK", p.owner, taskId, p.partId, "快照恢复");
                    }
                }
                if (ownerAlive && snap == 2 && once) {
                    // F1 一次性任务终态: 执行器上报 done(stopped 快照) → SUCCEEDED + 释放槽位。
                    // 豁免卡死宽限期(快完快回收); 非一次性任务的 stopped 快照仍走下方 exec-stopped 迁移。
                    completeOnceTask(p, e, taskId);
                    n++;
                    continue;
                }
                if (ownerAlive && snap != 1 && p.assignedAt > 0
                        && System.currentTimeMillis() - p.assignedAt > CARD_GRACE_MS) {
                    // 执行器失效:实例活着但快照停更(snap=0)或执行器主动停/FAILED(snap=2 快照持续上报
                    // stopped=true)超过宽限期 → 按模式迁移该分区。snap=2 是执行器 FAILED 的强信号
                    // (彻底版兜底:不留"归属仍在但消费已停"的死分区);宽限期防抖挡住"判死→重分配→
                    // 新执行器尚未启动"窗口内残留的旧 stopped 快照,避免回收-分配空转。
                    String reason = snap == 2 ? "exec-stopped" : "stuck";
                    if (p.locked() && p.pin.pinnedTo.equals(p.owner)) {
                        suspectWarn(taskId, p.partId, p.owner, "钉住分区执行器失效,保持不回收等待人工裁决");
                    } else if (once && maxAttemptsReached(taskId)) {
                        // F3 一次性任务:重派达上限 → FAILED 终态
                        failOnceTask(p, e, taskId);
                        n++;
                    } else {
                        int opened = openHandoverOrReclaim(p, e, taskId, reason);
                        if (opened > 0) {
                            if (once) recordReassign(taskId);
                            n++;
                            alertSink.fire("critical", "EXEC_STUCK", p.owner, taskId, p.partId,
                                    snap == 2 ? "执行器主动停(FAILED/异常退出),框架自动回收重分配"
                                            : "执行器卡死(快照停更超过宽限期),框架自动迁移留痕");
                        }
                    }
                }
            } else if (Enums.PartState.MIGRATING.equals(p.state) && p.handover != null) {
                // 交接推进与回滚(回滚条件按交接起因):death 源 lease 复活 / stuck 源快照恢复
                String reason = p.handover.reason == null ? "death" : p.handover.reason;
                // 回滚按起因:death 看源 lease 复活;stuck/exec-stopped 看源快照恢复(执行器重新启动)
                boolean rollback = "death".equals(reason)
                        ? liveIds.contains(p.handover.sourceOwner)
                        : snap == 1;
                if (rollback) {
                    PartRecord next = copy(p);
                    next.state = Enums.PartState.ASSIGNED;
                    next.owner = p.handover.sourceOwner;
                    next.assignedAt = System.currentTimeMillis();
                    next.handover = null;
                    store.casPut(KeySpace.part(ns, taskId, p.partId), e.getValue(), next, null);
                    log.info("[{}] 交接回滚 {}/{} → 源实例 {} 已恢复({})", ns, taskId, p.partId, p.owner, reason);
                } else if (System.currentTimeMillis() >= p.handover.confirmDeadline
                        || stoppedConfirmed(taskId, p.partId)) {
                    // 双窗口到期 / 源已主动停(stopped 快照)→ 推进
                    if (toPending(p, e, taskId)) n++;
                }
            }
        }
        return n;
    }

    /** 开保守交接(MIGRATING)或激进直接回收(reason:death/stuck,回滚判定用)。 */
    private int openHandoverOrReclaim(PartRecord p, Map.Entry<String, byte[]> e,
                                      String taskId, String reason) {
        if (isConservative(taskId)) {
            if (p.handover == null) {
                PartRecord next = copy(p);
                next.state = Enums.PartState.MIGRATING;
                next.handover = new PartRecord.HandoverState();
                next.handover.sourceOwner = p.owner;
                next.handover.confirmDeadline = System.currentTimeMillis() + CONSERVATIVE_WINDOW_MS;
                next.handover.reason = reason;
                if (store.casPut(KeySpace.part(ns, taskId, p.partId), e.getValue(), next, null)) {
                    log.info("[{}] 保守交接开启 {}/{} 源={} 起因={} 截止={}ms",
                            ns, taskId, p.partId, p.owner, reason, next.handover.confirmDeadline);
                    return 1;
                }
            } else if (System.currentTimeMillis() >= p.handover.confirmDeadline
                    || stoppedConfirmed(taskId, p.partId)) {
                return toPending(p, e, taskId) ? 1 : 0;
            }
            return 0;
        }
        return toPending(p, e, taskId) ? 1 : 0;
    }

    private boolean toPending(PartRecord p, Map.Entry<String, byte[]> e, String taskId) {
        PartRecord next = copy(p);
        next.owner = null;
        next.state = Enums.PartState.PENDING;
        next.handover = null;
        // pin 保留:判死场景 pin 指向死实例会被 4.2 裁决挡在回收前,走到这里说明锁已过期/无锁;
        // 手动迁移场景 pin 指向目标,toPending 后由定向分配交给 pinnedTo(交接完成,锁保留)
        boolean ok = store.casPut(KeySpace.part(ns, taskId, p.partId), e.getValue(), next, null);
        if (ok) log.info("[{}] 回收分区 {}/{} 源={} → PENDING", ns, taskId, p.partId, p.owner);
        return ok;
    }

    /** 保守双窗口快通道:源实例主动停写了 stopped 快照 → 立即确认。 */
    private boolean stoppedConfirmed(String taskId, int partId) {
        return store.getObj(KeySpace.snap(ns, taskId, partId), ExecutorSnapshot.class)
                .map(s -> s.stopped).orElse(false);
    }

    // ── F1/F2/F3: 一次性任务与非 Kafka 任务约定(微服务架构方案 §7.4) ──

    /** 非 Kafka 任务约定(虚拟 topic): topic 以 virtual:// 前缀时 lag 探针跳过(无真实分区可查)。 */
    private static boolean isVirtualTopic(String topic) {
        return topic != null && topic.startsWith("virtual://");
    }

    private boolean isOnceTask(String taskId) {
        TaskSpec s = specOf(taskId);
        return s != null && s.once();
    }

    /** 重试上限(判死重派 N 次 → FAILED): spec.bizMeta.maxAttempts 覆盖, 缺省 3。 */
    private int maxAttemptsOf(String taskId) {
        TaskSpec s = specOf(taskId);
        if (s == null || s.bizMeta == null) return 3;
        try {
            return Integer.parseInt(String.valueOf(s.bizMeta.getOrDefault("maxAttempts", "3")).trim());
        } catch (NumberFormatException e) {
            return 3;
        }
    }

    private boolean maxAttemptsReached(String taskId) {
        TaskStatus st = store.getObj(KeySpace.status(ns, taskId), TaskStatus.class).orElse(null);
        int n = st == null ? 0 : st.reassigns;
        return n >= maxAttemptsOf(taskId);
    }

    /** F3 计数: 判死重派(开交接/直接回收)成功 → reassigns+1(状态键留存供控制面观察)。 */
    private void recordReassign(String taskId) {
        TaskStatus st = store.getObj(KeySpace.status(ns, taskId), TaskStatus.class).orElse(null);
        if (st == null) return;
        st.reassigns++;
        store.put(KeySpace.status(ns, taskId), st);
        log.warn("[{}] 一次性任务 {} 判死重派 第 {} 次(上限 {})", ns, taskId, st.reassigns, maxAttemptsOf(taskId));
    }

    /** F1 完成终态: 执行器上报 done → SUCCEEDED + 删分区键(释放槽位) + 事件。 */
    private void completeOnceTask(PartRecord p, Map.Entry<String, byte[]> e, String taskId) {
        if (!store.casDelete(KeySpace.part(ns, taskId, p.partId), e.getValue())) return;
        TaskStatus st = store.getObj(KeySpace.status(ns, taskId), TaskStatus.class)
                .orElseGet(() -> TaskStatus.of(Enums.TaskState.SUCCEEDED, "auto"));
        st.state = Enums.TaskState.SUCCEEDED;
        st.since = System.currentTimeMillis();
        st.reason = "执行器上报 done(一次性任务完成)";
        store.put(KeySpace.status(ns, taskId), st);
        Map<String, Object> ev = new HashMap<>();
        ev.put("type", "task-succeeded");
        ev.put("at", st.since);
        store.put(KeySpace.taskEvent(ns, taskId, "succeeded"), ev);
        log.info("[{}] 一次性任务 {} 完成 → SUCCEEDED(分区槽位已释放)", ns, taskId);
    }

    /** F3 失败终态: 判死重派达上限 → FAILED + 删分区键(释放槽位) + 事件(控制面对账收尾用)。 */
    private void failOnceTask(PartRecord p, Map.Entry<String, byte[]> e, String taskId) {
        if (!store.casDelete(KeySpace.part(ns, taskId, p.partId), e.getValue())) return;
        TaskStatus st = store.getObj(KeySpace.status(ns, taskId), TaskStatus.class)
                .orElseGet(() -> TaskStatus.of(Enums.TaskState.FAILED, "auto"));
        st.state = Enums.TaskState.FAILED;
        st.since = System.currentTimeMillis();
        st.reason = "判死重派达上限(" + st.reassigns + " 次)不再重试";
        store.put(KeySpace.status(ns, taskId), st);
        Map<String, Object> ev = new HashMap<>();
        ev.put("type", "task-failed");
        ev.put("at", st.since);
        ev.put("reassigns", st.reassigns);
        store.put(KeySpace.taskEvent(ns, taskId, "failed"), ev);
        log.warn("[{}] 一次性任务 {} 判死重派达上限 → FAILED(分区槽位已释放)", ns, taskId);
        alertSink.fire("critical", "TASK_FAILED", p.owner, taskId, p.partId, st.reason);
    }

    /** 快照活性(故障处理子文档 §1.1):1=新鲜(活跃) 2=stopped(主动停) 0=失效(键缺失/ts 过期)。 */
    private int snapState(String taskId, int partId) {
        return store.getObj(KeySpace.snap(ns, taskId, partId), ExecutorSnapshot.class)
                .map(s -> s.stopped ? 2
                        : (s.ts + SNAPSHOT_TTL_MS > System.currentTimeMillis() ? 1 : 0))
                .orElse(0);
    }

    /** SUSPECT 观察态告警(60s 限流日志 + 告警事件流)。 */
    private void suspectWarn(String taskId, int partId, String owner, String detail) {
        long now = System.currentTimeMillis();
        String k = taskId + "/" + partId;
        if (now - suspectWarnAt.getOrDefault(k, 0L) < SUSPECT_WARN_INTERVAL_MS) return;
        suspectWarnAt.put(k, now);
        log.warn("[{}] 分区 {}/{} 实例 {} 疑似异常: {}(SUSPECT 观察中)", ns, taskId, partId, owner, detail);
        alertSink.fire("warning", "SUSPECT", owner, taskId, partId, detail + "(观察中,不判死)");
    }

    /** 新实例观察期(调度算法子文档 §六):注册不足 1 个决策周期的实例不纳入候选。 */
    private List<NsInstanceRecord> stableLive(List<NsInstanceRecord> live) {
        long now = System.currentTimeMillis();
        for (NsInstanceRecord i : live) instSeenAt.putIfAbsent(i.id, now);
        List<NsInstanceRecord> out = new ArrayList<>();
        for (NsInstanceRecord i : live) {
            if (now - instSeenAt.get(i.id) >= NEW_INSTANCE_GRACE_MS) out.add(i);
        }
        return out;
    }

    /** 读下架意图:有 /instances/{id}/drain 键的实例集合(键存在即意图在)。 */
    private Set<String> readDrainIntents() {
        Set<String> out = new HashSet<>();
        for (String key : store.getPrefix(KeySpace.nsInstancesPrefix(ns)).keySet()) {
            if (key.endsWith("/drain")) {
                out.add(KeySpace.instIdOfNsKey(key.substring(0, key.length() - "/drain".length())));
            }
        }
        return out;
    }

    /** 下架实例迁出(运维后台 §2.3):有下架意图的实例不接新单,其 ASSIGNED 分区每轮每实例迁 1 个(跳过锁)。
     *  复用 Rebalancer.migrate 的 MIGRATING 交接(reason=instance-drain);迁空后提示可安全关机。 */
    private int drainInstanceTick(Map<String, byte[]> allParts, List<NsInstanceRecord> live,
                                  List<NsInstanceRecord> accepting, Set<String> drainInsts,
                                  Map<String, InstanceRecord> metas, Map<String, Integer> assigned) {
        if (drainInsts.isEmpty() || accepting.isEmpty()) return 0;
        Map<String, String> handovers = readHandovers();
        int n = 0;
        for (String inst : drainInsts) {
            // 该实例 ASSIGNED 分区(跳过锁),取第一个
            Map.Entry<String, byte[]> picked = null;
            for (Map.Entry<String, byte[]> e : allParts.entrySet()) {
                if (!isPartKey(e.getKey())) continue;
                PartRecord p = parseQuiet(e.getValue());
                if (p == null || !Enums.PartState.ASSIGNED.equals(p.state)
                        || !inst.equals(p.owner) || p.locked()) continue;
                picked = e;
                break;
            }
            if (picked == null) {
                // 迁空判定:该实例无任何归属分区
                boolean any = false;
                for (Map.Entry<String, byte[]> e : allParts.entrySet()) {
                    if (!isPartKey(e.getKey())) continue;
                    PartRecord p = parseQuiet(e.getValue());
                    if (p != null && inst.equals(p.owner)
                            && !Enums.PartState.PENDING.equals(p.state)) { any = true; break; }
                }
                boolean alive = live.stream().anyMatch(i -> inst.equals(i.id));
                if (!any && alive) {
                    long now = System.currentTimeMillis();
                    Long last = drainedLogAt.get(inst);
                    if (last == null || now - last > 60_000) {
                        drainedLogAt.put(inst, now);
                        log.info("[{}] 实例 {} 已迁空,可安全关机(下架完成)", ns, inst);
                    }
                }
                continue;
            }
            PartRecord prev = parseQuiet(picked.getValue());
            String taskId = KeySpace.taskIdOfPartKey(picked.getKey());
            // 迁出目标:接单实例中打分最高(Scoring.pick 已排除槽位满;源不在 accepting)
            Optional<String> target = scoring.pick(accepting, metas, assigned, prev);
            if (target.isEmpty()) {
                log.warn("[{}] 下架迁出 {}/{} 无可用目标(接单实例不足)", ns, taskId, prev.partId);
                continue;
            }
            if (rebalancer.migrate(picked.getKey(), picked.getValue(), prev, taskId, target.get(),
                    handovers.getOrDefault(taskId, ""), "instance-drain")) {
                n++;
                log.info("[{}] 下架迁出 {}/{} {} → {} 已交接", ns, taskId, prev.partId, inst, target.get());
            }
        }
        return n;
    }

    /** taskId → 交接模式(再平衡按模式选迁移执行方式;直读 spec,不依赖缓存)。 */
    private Map<String, String> readHandovers() {
        Map<String, String> out = new HashMap<>();
        for (Map.Entry<String, byte[]> e : store.getPrefix(KeySpace.tasksPrefix(ns)).entrySet()) {
            if (!e.getKey().endsWith("/spec")) continue;
            try {
                TaskSpec s = Json.fromBytes(e.getValue(), TaskSpec.class);
                out.put(s.taskId, s.handover);
            } catch (Exception ignored) { /* 坏 spec 跳过 */ }
        }
        return out;
    }

    /** taskId → RUNNING 任务 spec(扩缩容水位判定输入;直读 spec+status,不依赖缓存;冻结任务(phase 非空)不参与)。 */
    private Map<String, TaskSpec> readRunningSpecs() {
        Map<String, TaskSpec> out = new HashMap<>();
        for (Map.Entry<String, byte[]> e : store.getPrefix(KeySpace.tasksPrefix(ns)).entrySet()) {
            if (!e.getKey().endsWith("/spec")) continue;
            try {
                TaskSpec s = Json.fromBytes(e.getValue(), TaskSpec.class);
                if (s.phase != null && !s.phase.isBlank()) continue; // BATCHING/PAUSED 冻结,不扩缩
                TaskStatus st = store.getObj(KeySpace.status(ns, s.taskId), TaskStatus.class).orElse(null);
                if (st != null && Enums.TaskState.RUNNING.equals(st.state)) out.put(s.taskId, s);
            } catch (Exception ignored) { /* 坏 spec 跳过 */ }
        }
        return out;
    }

    // ── 读取辅助 ──

    private Map<String, byte[]> readParts() {
        Map<String, byte[]> out = new HashMap<>();
        for (Map.Entry<String, byte[]> e : store.getPrefix(KeySpace.tasksPrefix(ns)).entrySet()) {
            if (isPartKey(e.getKey())) out.put(e.getKey(), e.getValue());
        }
        return out;
    }

    private List<NsInstanceRecord> readLiveInstances() {
        List<NsInstanceRecord> out = new ArrayList<>();
        String prefix = "/dcp/" + ns + "/instances/";
        Map<String, byte[]> all = store.getPrefix(KeySpace.nsInstancesPrefix(ns));
        // 先收集主写意图子键(暂停/调权 overlay),再解析直接注册键
        Set<String> pausedIds = new HashSet<>();
        Map<String, Double> weightOverlay = new HashMap<>();
        for (Map.Entry<String, byte[]> e : all.entrySet()) {
            String rest = e.getKey().substring(prefix.length());
            if (rest.endsWith("/paused")) {
                pausedIds.add(KeySpace.instIdOfNsKey(e.getKey().substring(0, e.getKey().length() - "/paused".length())));
            } else if (rest.endsWith("/weight")) {
                try {
                    Map<String, Object> w = Json.fromBytes(e.getValue(), Map.class);
                    Object v = w.get("weight");
                    if (v instanceof Number) {
                        weightOverlay.put(KeySpace.instIdOfNsKey(
                                e.getKey().substring(0, e.getKey().length() - "/weight".length())),
                                ((Number) v).doubleValue());
                    }
                } catch (Exception ignored) { /* 坏记录 */ }
            }
        }
        for (Map.Entry<String, byte[]> e : all.entrySet()) {
            // 只认直接注册键 /instances/{id};子键(下架/暂停/调权意图)跳过
            String rest = e.getKey().substring(prefix.length());
            if (rest.contains("/")) continue;
            try {
                NsInstanceRecord r = Json.fromBytes(e.getValue(), NsInstanceRecord.class);
                if (pausedIds.contains(r.id)) r.paused = true;
                Double w = weightOverlay.get(r.id);
                if (w != null) r.weight = w;
                out.add(r);
            } catch (Exception ignored) { /* 坏记录 */ }
        }
        return out;
    }

    private Map<String, InstanceRecord> readMetas() {
        Map<String, InstanceRecord> out = new HashMap<>();
        for (Map.Entry<String, byte[]> e : store.getPrefix(KeySpace.instancesPrefix()).entrySet()) {
            try {
                InstanceRecord r = Json.fromBytes(e.getValue(), InstanceRecord.class);
                out.put(r.id, r);
            } catch (Exception ignored) { /* 坏记录 */ }
        }
        return out;
    }

    private static Map<String, byte[]> filterState(Map<String, byte[]> parts, String state) {
        Map<String, byte[]> out = new HashMap<>();
        for (Map.Entry<String, byte[]> e : parts.entrySet()) {
            PartRecord p = parseQuiet(e.getValue());
            if (p != null && state.equals(p.state)) out.put(e.getKey(), e.getValue());
        }
        return out;
    }

    private static PartRecord parseQuiet(byte[] b) {
        try {
            return Json.fromBytes(b, PartRecord.class);
        } catch (Exception e) {
            return null;
        }
    }

    private static PartRecord copy(PartRecord p) {
        PartRecord n = new PartRecord();
        n.partId = p.partId;
        n.owner = p.owner;
        n.state = p.state;
        n.epoch = p.epoch + 1;
        n.assignedAt = p.assignedAt; // 卡死宽限期基准,解锁/推进等 CAS 写回必须保留
        n.handover = p.handover;
        n.pin = p.pin;
        n.drain = p.drain; // DELETING 追平中间态,expirePins/推进等 CAS 写回必须保留
        return n;
    }

    private boolean isConservative(String taskId) {
        TaskSpec s = specOf(taskId);
        return s != null && s.conservative();
    }

    private TaskSpec specOf(String taskId) {
        return specCache.computeIfAbsent(taskId,
                k -> store.getObj(KeySpace.spec(ns, k), TaskSpec.class).orElse(null));
    }

    private static boolean isPartKey(String key) {
        return key.contains("/parts/") && !key.endsWith("/snap");
    }

    // ── 退出 ──

    @Override
    public void close() {
        closed = true;
        timer.shutdownNow();
        closeDecisionWatchers();
        if (leaderWatch != null) leaderWatch.close();
        if (leader && leaderLease != null) {
            // 主动让位:仅当 leader 键仍是自己时删除
            if (selfId.equals(store.getLeader(KeySpace.leader(ns), String.class).orElse(null))) {
                store.delete(KeySpace.leader(ns));
            }
        }
        if (leaderLease != null) leaderLease.close();
        log.info("[{}] 调度主 {} 退出", ns, selfId);
    }
}
