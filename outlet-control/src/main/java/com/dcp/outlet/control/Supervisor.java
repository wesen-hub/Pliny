package com.dcp.outlet.control;

import java.util.ArrayList;
import java.util.List;
import java.util.function.LongSupplier;

/**
 * 巡检器:周期性对账「期望状态 vs 实际状态」与「租约 / 心跳」,**只告警、不自动补齐**(D12)。
 *
 * <p>与计算面自停机制互补:计算面负责"自己不该跑的时候停下",巡检器负责"该跑的没跑/跑不动时说出来"。
 */
public class Supervisor {

    private final TaskStore tasks;
    private final UnitStore units;
    private final UnitStatusStore statuses;
    private final LeaseManager leases;
    private final AlertSink alerts;
    private final long leaseTtlMillis;
    private final long stallMillis;
    private final long dispatchGraceMillis;
    private final LongSupplier clock;

    public Supervisor(TaskStore tasks, UnitStore units, UnitStatusStore statuses, LeaseManager leases,
                      AlertSink alerts, long leaseTtlMillis, long stallMillis,
                      long dispatchGraceMillis, LongSupplier clock) {
        this.tasks = tasks; this.units = units; this.statuses = statuses; this.leases = leases;
        this.alerts = alerts; this.leaseTtlMillis = leaseTtlMillis; this.stallMillis = stallMillis;
        this.dispatchGraceMillis = dispatchGraceMillis; this.clock = clock;
    }

    /** 默认:租约 30s · 停滞 5min · 下发宽限 60s。 */
    public static Supervisor defaults(TaskStore tasks, UnitStore units, UnitStatusStore statuses,
                                      LeaseManager leases, AlertSink alerts) {
        return new Supervisor(tasks, units, statuses, leases, alerts, 30_000L, 300_000L, 60_000L,
                System::currentTimeMillis);
    }

    /** 巡检一轮:返回本轮告警(同时已推给 {@link AlertSink})。 */
    public List<Alert> tick() {
        long now = clock.getAsLong();
        List<Alert> out = new ArrayList<>();

        // ① 租约过期 → 释放并告警(可能是单元失联;也可能只是平台没续上)
        for (LeaseGrant g : leases.expireScan(now)) {
            out.add(new Alert(Alert.Kind.LEASE_EXPIRED, g.taskId(), g.unitId(),
                    "租约过期(分区 p" + g.partition() + " · " + g.topic() + "),请确认该单元是否已失联", now));
        }

        // 任务级对账由调用方按任务驱动(见 tickTask):内核不依赖"全量列举任务"能力
        return fire(out);
    }

    /** 针对单个任务巡检(调用方按任务驱动,避免内核依赖"全量列举")。 */
    public List<Alert> tickTask(String taskId) {
        long now = clock.getAsLong();
        List<Alert> out = new ArrayList<>();
        TaskRecord t = tasks.byId(taskId);
        if (t == null) return out;

        // 批失败必须人工重做批(不猜起点)
        if (t.status() == TaskStatus.BATCH_FAILED) {
            out.add(new Alert(Alert.Kind.BATCH_FAILED, taskId, null,
                    "批阶段失败:需人工触发重做批(换新 T0)", now));
        }

        // 外部引擎任务(方案 C:增量段=Flink)不巡检 v2 单元 —— 单元从未下发,
        // 若按常规巡检会持续误报 UNIT_MISSING/TASK_NOT_RUNNING;进度由 Kafka 消费组读取(另查)
        if ("external".equals(t.incrementalEngine())) {
            return fire(out);
        }

        List<PlannedUnit> planned = units.byTask(taskId);
        List<UnitStatusStore.UnitStatus> actual = statuses.byTask(taskId);
        boolean wantRunning = "running".equals(t.desiredState()) && t.status() == TaskStatus.RUNNING;

        if (wantRunning) {
            if (planned.isEmpty()) {
                out.add(new Alert(Alert.Kind.UNIT_MISSING, taskId, null, "期望运行但没有任何消费单元", now));
            }
            boolean anyRunning = false;
            for (PlannedUnit pu : planned) {
                UnitStatusStore.UnitStatus st = actual.stream()
                        .filter(s -> s.unitId.equals(pu.unitId())).findFirst().orElse(null);
                if (st == null) {
                    out.add(new Alert(Alert.Kind.UNIT_MISSING, taskId, pu.unitId(),
                            "单元在期望状态中缺失(可能已宕机/未创建)", now));
                    continue;
                }
                if ("RUNNING".equals(st.state)) anyRunning = true;
                if (st.lastSeenAt > 0 && now - st.lastSeenAt > stallMillis) {
                    out.add(new Alert(Alert.Kind.UNIT_STALLED, taskId, pu.unitId(),
                            "超过 " + (stallMillis / 1000) + "s 无心跳(最近 " + (now - st.lastSeenAt) / 1000 + "s 前)", now));
                } else if (st.lastSeenAt == 0 && "DISPATCHED".equals(st.state)
                        && st.dispatchedAt > 0 && now - st.dispatchedAt > dispatchGraceMillis) {
                    out.add(new Alert(Alert.Kind.UNIT_STALLED, taskId, pu.unitId(),
                            "已下发但从未上报心跳(超过宽限 " + (dispatchGraceMillis / 1000) + "s)", now));
                }
            }
            if (!anyRunning) {
                out.add(new Alert(Alert.Kind.TASK_NOT_RUNNING, taskId, null,
                        "期望运行但没有单元处于 RUNNING", now));
            }
        }
        return fire(out);
    }

    private List<Alert> fire(List<Alert> list) {
        if (alerts != null) list.forEach(alerts::fire);
        return list;
    }

    public long leaseTtlMillis() { return leaseTtlMillis; }
}
