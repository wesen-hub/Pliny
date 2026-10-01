package com.dcp.outlet.control;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * 巡检器:周期对账「期望状态 vs 实际状态」,**自动修复分场景化**(D12 已收敛):
 *
 * <p>CP1 激活:期望 running 但框架无 spec → 宽限期(180s,避正常流程执行中)后重放 onCreated(幂等 put 覆盖);
 * CP2b 交接:批 SUCCEEDED 但 handover_at 空(activate 失败/控制面中断)→ 宽限期后补交接(读 t0 重放 activate,幂等);
 * CP3 停止:期望 stopped 但框架 spec 残留 → 宽限期后补 cancelTask(deleteSpec 幂等)。
 * 批失败(CP2c 不猜起点)保持人工;宽限期用进程内首次发现时间戳(不改 schema)。
 * 修复失败不重试在本轮,下轮判据再命中自然重试;持续失败由告警流人工介入(文档 4.4-6)。
 *
 * <p>统一调度框架接管后,租约/停滞/失联/防双消费已收编(框架实例判死 + 快照活性检测
 * + 归属键唯一),这里只保留**业务外壳侧**的对账。
 */
public class Supervisor {

    private static final long GRACE_MS = 180_000L;   // 宽限期(静态租约):隔离「补偿 vs 正常流程」

    private final TaskStore tasks;
    private final TaskFrameworkProbe framework;
    private final TaskScheduler scheduler;
    private final HandoverFacts handoverFacts;
    private final AlertSink alerts;

    /** CP1/CP2b/CP3 判据首次命中时间(taskId → 毫秒):连续命中超过宽限期才执行修复。 */
    private final Map<String, Long> missingSince = new HashMap<>();
    private final Map<String, Long> handoverSince = new HashMap<>();
    private final Map<String, Long> orphanSince = new HashMap<>();

    public Supervisor(TaskStore tasks, TaskFrameworkProbe framework, TaskScheduler scheduler,
                      HandoverFacts handoverFacts, AlertSink alerts) {
        this.tasks = tasks;
        this.framework = framework;
        this.scheduler = scheduler;
        this.handoverFacts = handoverFacts;
        this.alerts = alerts;
    }

    /** 针对单个任务巡检(调用方按任务驱动,避免内核依赖"全量列举")。 */
    public List<Alert> tickTask(String taskId) {
        List<Alert> out = new ArrayList<>();
        TaskRecord t = tasks.byId(taskId);
        if (t == null) return out;

        // 批失败必须人工重做批(不猜起点);由框架事件键 batch-failed 承载
        if (framework.batchFailed(t.namespace(), taskId)) {
            out.add(new Alert(Alert.Kind.BATCH_FAILED, taskId, null,
                    "批阶段失败:需人工触发重做批(换新 T0)", System.currentTimeMillis()));
        }

        // 外部引擎任务(方案 C:增量段=Flink)不落框架 —— 若按常规巡检会持续误报 SPEC_MISSING
        if ("external".equals(t.incrementalEngine())) {
            return fire(out);
        }

        String ns = t.namespace();
        String phase = framework.phaseOf(ns, taskId);
        long now = System.currentTimeMillis();

        // CP2b 交接:批 SUCCEEDED ∧ handover_at 空(activate 失败/控制面中断)→ 宽限期后补交接
        // (读 t0 重放 activate,幂等 put 覆盖;与批失败区分:批本身成功,重做批是错误药方)
        Optional<Long> pendingT0 = handoverFacts.pendingT0(taskId);
        if (pendingT0.isPresent() && "running".equals(t.desiredState())) {
            if (pastGrace(handoverSince, taskId, now)) {
                try {
                    scheduler.handover(taskId, pendingT0.get());
                    handoverSince.remove(taskId);
                    out.add(new Alert(Alert.Kind.HANDOVER_PENDING, taskId, null,
                            "批已完成但交接未落地,已自动补交接", now));
                } catch (Exception e) {
                    out.add(new Alert(Alert.Kind.HANDOVER_PENDING, taskId, null,
                            "自动补交接失败(" + e.getMessage() + "),下轮重试", now));
                }
            }
        } else {
            handoverSince.remove(taskId);
        }

        // CP1 激活:期望 running ∧ 框架无 spec → 宽限期后重放 onCreated(幂等;失败下轮再试)
        if ("running".equals(t.desiredState()) && phase == null) {
            if (pastGrace(missingSince, taskId, now)) {
                try {
                    scheduler.onCreated(t);
                    missingSince.remove(taskId);
                    out.add(new Alert(Alert.Kind.UNIT_MISSING, taskId, null,
                            "期望运行但框架无任务,已自动重放 onCreated 修复", now));
                } catch (Exception e) {
                    out.add(new Alert(Alert.Kind.UNIT_MISSING, taskId, null,
                            "自动重放 onCreated 失败(" + e.getMessage() + "),下轮重试", now));
                }
            }
        } else {
            missingSince.remove(taskId);
        }

        // CP3 停止:期望 stopped ∧ 框架 spec 残留 → 宽限期后补注销(幂等;失败下轮再试)
        if ("stopped".equals(t.desiredState()) && phase != null) {
            if (pastGrace(orphanSince, taskId, now)) {
                try {
                    framework.cancelTask(ns, taskId);
                    orphanSince.remove(taskId);
                    out.add(new Alert(Alert.Kind.SPEC_ORPHAN, taskId, null,
                            "期望停止但框架 spec 残留,已自动补注销", now));
                } catch (Exception e) {
                    out.add(new Alert(Alert.Kind.SPEC_ORPHAN, taskId, null,
                            "自动补注销失败(" + e.getMessage() + "),下轮重试", now));
                }
            }
        } else {
            orphanSince.remove(taskId);
        }
        return fire(out);
    }

    /** 宽限期判定:首次命中记时间戳,连续命中超过 {@link #GRACE_MS} 才允许修复。 */
    private boolean pastGrace(Map<String, Long> since, String taskId, long now) {
        long first = since.computeIfAbsent(taskId, k -> now);
        return now - first >= GRACE_MS;
    }

    private List<Alert> fire(List<Alert> list) {
        if (alerts != null) list.forEach(alerts::fire);
        return list;
    }
}
