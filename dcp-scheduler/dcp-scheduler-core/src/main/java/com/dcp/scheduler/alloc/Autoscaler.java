package com.dcp.scheduler.alloc;

import com.dcp.scheduler.alert.AlertSink;
import com.dcp.scheduler.exec.HookRegistry;
import com.dcp.scheduler.exec.ScaleDownHooks;
import com.dcp.scheduler.model.Enums;
import com.dcp.scheduler.model.ExecutorSnapshot;
import com.dcp.scheduler.model.PartRecord;
import com.dcp.scheduler.model.ScaleRules;
import com.dcp.scheduler.model.TaskSpec;
import com.dcp.scheduler.model.TaskStatus;
import com.dcp.scheduler.store.EtcdStore;
import com.dcp.scheduler.store.Json;
import com.dcp.scheduler.store.KeySpace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * 扩缩容(扩缩容子文档 §二~§四):每决策周期对 RUNNING 任务做水位判定 + 逻辑缩容编排。
 *   - 规则继承:TaskSpec.scaleRules 非空逐字段覆盖,否则继承 ns 默认规则(KeySpace.rules,缺省内置默认);
 *   - 扩容:水位 > up 持续 upPeriods 周期 + 冷却 + 上限 → 尾部补 PENDING(最多翻倍),写扩容建议事件;
 *   - 缩容:水位 < down 持续 downPeriods 周期 + 冷却 + 下限 + 双闸门(ns.downEnabled AND 钩子非空)
 *           → 调钩子 routeCut → 置 DELETING → 追平判 rps 归零后清记录(单轮 1 分区,尾部收缩);
 *   - 护栏:步长翻倍/减半、同轮全 ns 缩容任务 ≤ 2、追平 deadline 超时告警不自动回滚;
 *   - 内存态(持续期计数/冷却/追平计数)与 Rebalancer 一致:换主重置,决策结果负反馈兜底。
 * 线程模型:主决策单线程调用;钩子 routeCut 放独立线程 + 30s 超时(决策线程不阻塞)。
 */
public class Autoscaler {

    private static final Logger log = LoggerFactory.getLogger(Autoscaler.class);

    /** 追平判定:rps 低于该阈值视为流量归零(条/秒);无 Kafka 源时回退该近似。 */
    private static final double RPS_FLOOR = 0.1;
    /** lag 归零阈值(条):lag = endOffset − committedOffset < 此值视为追平(监控子文档定论)。 */
    private static final long DRAIN_LAG_ZERO = 100;
    /** 追平确认:连续周期数(lag/rps 持续归零才清记录)。 */
    private static final int DRAIN_PERIODS = 2;
    /** 追平 deadline:超时告警 + 人工介入,不自动回滚路由。 */
    private static final long DRAIN_DEADLINE_MS = 10 * 60_000;
    /** 钩子执行超时(扩缩容子文档 §五)。 */
    private static final long HOOK_TIMEOUT_MS = 30_000;
    /** 全 ns 同轮缩容任务数限量(防路由切换风暴)。 */
    private static final int MAX_DRAIN_TASKS_PER_ROUND = 2;
    /** 追平超时告警限流(同分区)。 */
    private static final long DRAIN_WARN_INTERVAL_MS = 60_000;

    private final String ns;
    private final EtcdStore store;
    private final HookRegistry hooks;
    private final AlertSink alertSink;

    // ── 决策内存态(持续期计数/冷却;换主重置) ──
    private final Map<String, Integer> upStreak = new HashMap<>();
    private final Map<String, Integer> downStreak = new HashMap<>();
    private final Map<String, Long> lastDecisionAt = new HashMap<>();
    // ── DELETING 推进内存态 ──
    private final Map<String, Integer> drainStreak = new HashMap<>();
    private final Map<String, Long> drainWarnAt = new HashMap<>();
    // ── rps 派生(快照相邻帧差分) ──
    private final Map<String, long[]> prevFrame = new HashMap<>(); // taskId/partId → {count, ts}
    private final Map<String, Double> rpsCache = new HashMap<>();

    /** 钩子执行线程(独立,防 routeCut 阻塞决策线程)。构造器里建(需 ns 命名线程)。 */
    private final ExecutorService hookExecutor;

    public Autoscaler(String ns, EtcdStore store, HookRegistry hooks, AlertSink alertSink) {
        this.ns = ns;
        this.store = store;
        this.hooks = hooks;
        this.alertSink = alertSink;
        this.hookExecutor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "scale-hook-" + ns);
            t.setDaemon(true);
            return t;
        });
    }

    // ── rps 派生 ──

    /** 快照相邻帧差分派生 rps(监控子文档 §二):每轮读全部快照,与上一帧对比;无快照/已停 = 0。 */
    public void refreshRps(Map<String, byte[]> allParts) {
        Map<String, long[]> freshPrev = new HashMap<>();
        Map<String, Double> fresh = new HashMap<>();
        for (Map.Entry<String, byte[]> e : allParts.entrySet()) {
            String taskId = KeySpace.taskIdOfPartKey(e.getKey());
            int partId = KeySpace.partIdOf(e.getKey());
            String key = taskId + "/" + partId;
            ExecutorSnapshot snap = store.getObj(KeySpace.snap(ns, taskId, partId), ExecutorSnapshot.class)
                    .orElse(null);
            if (snap == null || snap.stopped) continue; // 无快照/主动停:rps 视为 0
            long[] prev = prevFrame.get(key);
            if (prev != null && snap.ts > prev[1]) {
                double rps = (snap.processedCount - prev[0]) * 1000.0 / Math.max(1, snap.ts - prev[1]);
                fresh.put(key, Math.max(0, rps));
            }
            freshPrev.put(key, new long[]{snap.processedCount, snap.ts});
        }
        prevFrame.clear();
        prevFrame.putAll(freshPrev); // 重建:删除的分区不残留
        rpsCache.clear();
        rpsCache.putAll(fresh);
    }

    /** 分区 rps(条/秒),无数据 = 0。 */
    public double rpsOf(String taskId, int partId) {
        return rpsCache.getOrDefault(taskId + "/" + partId, 0.0);
    }

    /** 分区 rps 全量快照(再平衡选迁移代价最小分区用;上一轮 refresh 结果,滞后一轮可接受)。 */
    public Map<String, Double> rpsSnapshot() {
        return new HashMap<>(rpsCache);
    }

    // ── ⑧ 水位判定(扩容 / 缩容决策) ──

    /**
     * 一轮扩缩决策。
     *
     * @param runningSpecs RUNNING 任务的 spec(taskId → spec)
     * @param allParts     全部归属记录的"读时字节"
     * @return 动作数(补分区数 + 进入 DELETING 数)
     */
    public int evaluate(Map<String, TaskSpec> runningSpecs, Map<String, byte[]> allParts) {
        long now = System.currentTimeMillis();
        int actions = 0;
        int drainTasks = 0;
        for (Map.Entry<String, TaskSpec> e : runningSpecs.entrySet()) {
            String taskId = e.getKey();
            TaskSpec spec = e.getValue();
            if (spec.targetRps <= 0 || spec.minPartitions < 0) continue;
            ScaleRules rules = effectiveRules(spec);
            int curParts = countParts(taskId, allParts);
            double flow = sumRps(taskId, allParts);
            if (curParts <= 0) continue;
            double waterLevel = flow / (curParts * spec.targetRps);

            // 冷却:距上次扩/缩决策不足冷却期 → 只观察不决策(防抖负反馈由水位自然回落承担)
            if (now - lastDecisionAt.getOrDefault(taskId, 0L) < rules.cooldownMinutes * 60_000L) {
                continue;
            }

            // 扩容判定
            if (waterLevel > rules.up && curParts < spec.maxPartitions) {
                upStreak.merge(taskId, 1, Integer::sum);
                downStreak.put(taskId, 0);
                if (upStreak.get(taskId) >= rules.upPeriods) {
                    int desired = Math.max(curParts + 1,
                            (int) Math.min(Math.ceil(flow / spec.targetRps), spec.maxPartitions));
                    // 步长护栏:单次最多翻倍(扩缩容子文档 §七)
                    int step = (int) Math.min(desired - curParts, Math.ceil(curParts * (rules.maxStep - 1)));
                    if (scaleUp(taskId, curParts, step, desired, flow, waterLevel)) {
                        actions += step;
                        lastDecisionAt.put(taskId, now);
                    }
                    upStreak.put(taskId, 0);
                }
            } else {
                upStreak.put(taskId, 0);
            }

            // 缩容判定(双闸门:ns.downEnabled AND 钩子非空;同轮互斥:水位不可能同时 >up 且 <down)
            // 冷启动护栏:任务任一分区都未产出过 rps 差分(执行器刚起、快照未到)不判缩容,
            // 防 ramp-up 期 flow=0 被误读为低流量(与“有快照但 rps=0”的真低流量区分)
            boolean anyRps = false;
            for (String k : rpsCache.keySet()) {
                if (k.startsWith(taskId + "/")) { anyRps = true; break; }
            }
            if (anyRps && rules.downEnabled && hooks.get(spec.scaleDownHooks) != null
                    && curParts > spec.minPartitions && waterLevel < rules.down) {
                downStreak.merge(taskId, 1, Integer::sum);
                if (downStreak.get(taskId) >= rules.downPeriods) {
                    downStreak.put(taskId, 0);
                    if (drainTasks >= MAX_DRAIN_TASKS_PER_ROUND) break; // 同轮限量
                    // 缩容目标(下限护栏),单轮只下尾部 1 个分区
                    int target = (int) Math.max(Math.floor(flow / spec.targetRps), spec.minPartitions);
                    if (curParts - 1 >= target
                            && scaleDown(taskId, spec, curParts, now, flow, waterLevel)) {
                        actions++;
                        drainTasks++;
                        lastDecisionAt.put(taskId, now);
                    }
                }
            } else {
                downStreak.put(taskId, 0);
            }
        }
        return actions;
    }

    // ── 扩容执行 ──

    /** 尾部补 PENDING 分区 + 推进期望分区数 + 写扩容建议事件(业务外壳观察,框架对 Kafka 只读)。 */
    private boolean scaleUp(String taskId, int curParts, int step, int desired,
                            double flow, double waterLevel) {
        for (int p = curParts; p < curParts + step; p++) {
            PartRecord rec = new PartRecord();
            rec.partId = p;
            store.createIfAbsent(KeySpace.part(ns, taskId, p), rec, null);
        }
        updateExpectedParts(taskId, curParts + step);
        Map<String, Object> ev = new HashMap<>();
        ev.put("type", "scale-up-suggest");
        ev.put("desire", desired);
        ev.put("flow", flow);
        ev.put("at", System.currentTimeMillis());
        store.put(KeySpace.taskEvent(ns, taskId, "scale-up"), ev);
        log.info("[{}] 扩容 {}/{} +{} → {} 分区(水位={} 流量={} 期望={})",
                ns, taskId, curParts, step, curParts + step,
                String.format("%.2f", waterLevel), String.format("%.0f", flow), desired);
        return true;
    }

    // ── 缩容执行(逻辑缩容三步,扩缩容子文档 §四) ──

    /** 缩容一个分区(尾部收缩:分区 id 连续对齐,清记录后 ensureParts 不会补回)。 */
    private boolean scaleDown(String taskId, TaskSpec spec, int curParts, long now,
                              double flow, double waterLevel) {
        int partId = curParts - 1; // 尾部:当前最大分区 id
        Optional<byte[]> cur = store.getBytes(KeySpace.part(ns, taskId, partId));
        if (cur.isEmpty()) return false;
        PartRecord p;
        try {
            p = Json.fromBytes(cur.get(), PartRecord.class);
        } catch (Exception ex) {
            return false;
        }
        if (!Enums.PartState.ASSIGNED.equals(p.state) || p.locked()) return false; // 锁分区跳过

        ScaleDownHooks hook = hooks.get(spec.scaleDownHooks);
        if (hook == null) return false;
        // ① 调钩子切路由(独立线程 + 超时;幂等重调由业务侧保证)
        boolean cut;
        try {
            Future<Boolean> f = hookExecutor.submit(() -> hook.routeCut(taskId, List.of(partId)));
            cut = f.get(HOOK_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        } catch (Exception ex) {
            log.warn("[{}] 缩容中止 {}/{}: 钩子 routeCut 失败/超时(冷却期重置)", ns, taskId, partId);
            return false;
        }
        if (!cut) {
            log.warn("[{}] 缩容中止 {}/{}: 钩子拒绝切路由(冷却期重置)", ns, taskId, partId);
            return false;
        }
        // ② 置 DELETING(追平中间态持久化,换主可续)
        PartRecord next = copy(p);
        next.state = Enums.PartState.DELETING;
        next.drain = new PartRecord.DrainState();
        next.drain.reason = "scale-in";
        next.drain.deadline = now + DRAIN_DEADLINE_MS;
        next.drain.routeCutAt = now;
        next.drain.topic = spec.topic;
        if (!store.casPut(KeySpace.part(ns, taskId, partId), cur.get(), next, null)) return false;
        log.info("[{}] 缩容开启 {}/{} → DELETING(水位={} 流量={} 追平截止={}ms)",
                ns, taskId, partId, String.format("%.2f", waterLevel), String.format("%.0f", flow),
                next.drain.deadline);
        return true;
    }

    // ── DELETING 推进(追平判定 → 清记录) ──

    /**
     * 推进 DELETING 分区(每周期;不受判死轮限制):
     *   - 追平判定:lag = endOffset − committedOffset 实查 < 阈值(无 Kafka 源回退 rps 归零近似),
     *     连续 DRAIN_PERIODS 周期 + drainReady 钩子确认 → 清记录 + 期望分区数递减;
     *   - deadline 超时 → 限流告警 + 人工介入(不自动回滚路由)。
     * 注意顺序:先递减 expectedParts 再删记录,中间崩溃不会导致 ensureParts 把分区补回。
     *
     * @param endOffsets topic → partId → 最新 endOffset(主侧 Kafka 探针缓存;null = 无源,回退 rps 近似)
     */
    public int drainTick(Map<String, byte[]> allParts, Map<String, Map<Integer, Long>> endOffsets) {
        long now = System.currentTimeMillis();
        int n = 0;
        for (Map.Entry<String, byte[]> e : allParts.entrySet()) {
            PartRecord p;
            try {
                p = Json.fromBytes(e.getValue(), PartRecord.class);
            } catch (Exception ex) {
                continue;
            }
            if (!Enums.PartState.DELETING.equals(p.state) || p.drain == null) continue;
            String taskId = KeySpace.taskIdOfPartKey(e.getKey());
            String k = taskId + "/" + p.partId;

            if (now >= p.drain.deadline) { // 追平超时:告警事件流 + 人工(不自动回滚)
                if (now - drainWarnAt.getOrDefault(k, 0L) >= DRAIN_WARN_INTERVAL_MS) {
                    drainWarnAt.put(k, now);
                    log.warn("[{}] 追平超时 {}/{}: deadline 已过仍未追平(lag/rps 未归零),需人工介入(路由不自动回滚)",
                            ns, taskId, p.partId);
                }
                alertSink.fire("warning", "DRAIN_TIMEOUT", p.owner, taskId, p.partId,
                        "追平 deadline 已过仍未归零,需人工介入(路由不自动回滚)");
                continue;
            }
            if (!drained(p, taskId, endOffsets)) { // 还在追平:计数清零
                drainStreak.put(k, 0);
                continue;
            }
            // 追平确认:连续周期
            drainStreak.merge(k, 1, Integer::sum);
            if (drainStreak.get(k) < DRAIN_PERIODS) continue;
            // 注销/暂停:直接清记录(期望分区数不变);缩容:业务钩子二次确认 + 期望分区数先减后删(防 ensureParts 补回)
            boolean skipShrink = "drain-task".equals(p.drain.reason) || "pause-task".equals(p.drain.reason);
            if (!skipShrink) {
                if (!drainReadyOk(taskId, p.partId)) { drainStreak.put(k, 0); continue; }
                if (!shrinkExpectedParts(taskId, e.getValue())) continue;
            }
            if (!store.casDelete(KeySpace.part(ns, taskId, p.partId), e.getValue())) continue;
            drainStreak.remove(k);
            Long lag = lagOf(p, taskId, endOffsets);
            alertSink.resolve("DRAIN_TIMEOUT", p.owner, taskId, p.partId, "分区已追平清理");
            n++;
            log.info("[{}] {} {}/{} 追平清记录({})", ns, skipShrink ? "注销/暂停" : "缩容完成", taskId, p.partId,
                    lag != null ? "lag=" + lag : "rps 回退");
        }
        return n;
    }

    /**
     * 追平判定:lag = endOffset − committedOffset 实查(监控子文档口径);
     * lag 数据不可用(无 Kafka 源/无 topic/无快照)回退 rps 归零近似——护栏,不阻塞缩容/注销。
     */
    private boolean drained(PartRecord p, String taskId, Map<String, Map<Integer, Long>> endOffsets) {
        Long lag = lagOf(p, taskId, endOffsets);
        if (lag != null) return lag < DRAIN_LAG_ZERO;
        return rpsOf(taskId, p.partId) < RPS_FLOOR;
    }

    /** 计算分区 lag(条);null = lag 数据不可用(回退 rps 近似)。 */
    private Long lagOf(PartRecord p, String taskId, Map<String, Map<Integer, Long>> endOffsets) {
        if (endOffsets != null && p.drain != null && p.drain.topic != null) {
            Map<Integer, Long> eo = endOffsets.get(p.drain.topic);
            if (eo != null) {
                Long end = eo.get(p.partId);
                ExecutorSnapshot snap = store.getObj(KeySpace.snap(ns, taskId, p.partId), ExecutorSnapshot.class).orElse(null);
                if (end != null && snap != null) {
                    return Math.max(0, end - snap.committedOffset);
                }
            }
        }
        return null;
    }

    /** 业务侧确认无在途数据(drainReady 钩子,缺省放行)。 */
    private boolean drainReadyOk(String taskId, int partId) {
        TaskSpec spec = store.getObj(KeySpace.spec(ns, taskId), TaskSpec.class).orElse(null);
        if (spec == null) return true;
        ScaleDownHooks hook = hooks.get(spec.scaleDownHooks);
        if (hook == null) return true;
        try {
            Future<Boolean> f = hookExecutor.submit(() -> hook.drainReady(taskId, List.of(partId)));
            return f.get(HOOK_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        } catch (Exception ex) {
            log.warn("[{}] drainReady 钩子异常 {}/{}: {}", ns, taskId, partId, ex.getMessage());
            return false;
        }
    }

    /** 期望分区数递减(先于清记录,防 ensureParts 补回;status 为主自写键,单主直写)。 */
    private boolean shrinkExpectedParts(String taskId, byte[] readPartValue) {
        TaskStatus st = store.getObj(KeySpace.status(ns, taskId), TaskStatus.class).orElse(null);
        if (st == null || st.expectedParts == null) return false;
        st.expectedParts = Math.max(1, st.expectedParts - 1);
        store.put(KeySpace.status(ns, taskId), st);
        return true;
    }

    private void updateExpectedParts(String taskId, int newVal) {
        TaskStatus st = store.getObj(KeySpace.status(ns, taskId), TaskStatus.class).orElse(null);
        if (st == null) return;
        st.expectedParts = newVal;
        store.put(KeySpace.status(ns, taskId), st);
    }

    // ── 规则继承与读取辅助 ──

    /** 任务级规则逐字段覆盖 ns 默认规则(扩缩容子文档 §二)。 */
    private ScaleRules effectiveRules(TaskSpec spec) {
        ScaleRules base = store.getObj(KeySpace.rules(ns), ScaleRules.class)
                .orElseGet(ScaleRules::new);
        if (spec.scaleRules == null) return base;
        ScaleRules r = new ScaleRules();
        r.up = spec.scaleRules.up;
        r.upPeriods = spec.scaleRules.upPeriods;
        r.down = spec.scaleRules.down;
        r.downPeriods = spec.scaleRules.downPeriods;
        r.cooldownMinutes = spec.scaleRules.cooldownMinutes;
        r.maxStep = spec.scaleRules.maxStep;
        r.downEnabled = spec.scaleRules.downEnabled;
        return r;
    }

    /** 任务当前分区数(以 parts 记录为准,含中间态;扩缩容子文档 §二边界语义)。 */
    private int countParts(String taskId, Map<String, byte[]> allParts) {
        int n = 0;
        for (String key : allParts.keySet()) {
            if (key.contains("/tasks/" + taskId + "/parts/")) n++;
        }
        return n;
    }

    private double sumRps(String taskId, Map<String, byte[]> allParts) {
        double sum = 0;
        for (String key : allParts.keySet()) {
            if (key.contains("/tasks/" + taskId + "/parts/")) {
                sum += rpsOf(taskId, KeySpace.partIdOf(key));
            }
        }
        return sum;
    }

    private static PartRecord copy(PartRecord p) {
        PartRecord n = new PartRecord();
        n.partId = p.partId;
        n.owner = p.owner;
        n.state = p.state;
        n.epoch = p.epoch + 1;
        n.assignedAt = p.assignedAt;
        n.handover = p.handover;
        n.pin = p.pin;
        return n;
    }
}
