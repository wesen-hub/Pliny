package com.dcp.outlet.control;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.LongSupplier;

/**
 * 调度器:把任务推进到"单元已下发"。
 *
 * <pre>
 * onCreated ── startPolicy=snapshot ──▶ 批阶段(不碰 Kafka)──成功──▶ 交接
 *            └ startPolicy=latest   ──▶ 直接交接(t0 = 最新水位)
 * 交接:逐单元逐分区算起点(每分区第一个 commitTs > T0)→ 授予租约 → 下发 CreateUnit
 * </pre>
 */
public class TaskScheduler {

    private final TaskStore tasks;
    private final UnitStore units;
    private final UnitDispatcher dispatcher;
    private final StartOffsetService startOffsets;
    private final LeaseManager leases;
    private final LatestTsoProvider latestTso;
    private final BatchRunner batchRunner;
    private final ExternalIncrementalDispatcher externalDispatcher;
    private final LongSupplier clock;
    private final long leaseTtlMillis;

    public TaskScheduler(TaskStore tasks, UnitStore units, UnitDispatcher dispatcher,
                         StartOffsetService startOffsets, LeaseManager leases, LatestTsoProvider latestTso,
                         BatchRunner batchRunner, LongSupplier clock, long leaseTtlMillis) {
        this(tasks, units, dispatcher, startOffsets, leases, latestTso, batchRunner, null, clock, leaseTtlMillis);
    }

    public TaskScheduler(TaskStore tasks, UnitStore units, UnitDispatcher dispatcher,
                         StartOffsetService startOffsets, LeaseManager leases, LatestTsoProvider latestTso,
                         BatchRunner batchRunner, ExternalIncrementalDispatcher externalDispatcher,
                         LongSupplier clock, long leaseTtlMillis) {
        this.tasks = tasks; this.units = units; this.dispatcher = dispatcher;
        this.startOffsets = startOffsets; this.leases = leases; this.latestTso = latestTso;
        this.batchRunner = batchRunner; this.externalDispatcher = externalDispatcher;
        this.clock = clock; this.leaseTtlMillis = leaseTtlMillis;
    }

    /** 任务创建后:决定"先批打底"还是"直接从最新起"。 */
    public void onCreated(TaskRecord task) {
        if ("snapshot".equals(task.startPolicy())) {
            task.status(TaskStatus.BATCH_RUNNING);
            tasks.update(task);
            batchRunner.runAsync(task.taskId(), (t0, ok) -> onBatchDone(task.taskId(), t0, ok));
        } else {
            handover(task.taskId(), latestOf(task));
        }
    }

    /** 批阶段完成回调。失败 → {@code BATCH_FAILED}(**不猜起点**,人工触发重做批换新 T0)。 */
    public void onBatchDone(String taskId, long t0, boolean ok) {
        TaskRecord t = tasks.byId(taskId);
        if (!ok) {
            t.status(TaskStatus.BATCH_FAILED);
            tasks.update(t);
            return;
        }
        t.startTso(t0).status(TaskStatus.BATCH_DONE);
        tasks.update(t);
        handover(taskId, t0);                       // 批流交接
    }

    /** 交接:算起点(每分区第一个 {@code commitTs > T0})→ 授租约 → 下发(单元) 或 播给外部引擎。 */
    public void handover(String taskId, long t0) {
        TaskRecord t = tasks.byId(taskId);
        List<PlannedUnit> planned = units.byTask(taskId);

        // 方案 C:增量段 = 外部引擎(Flink)—— 算全部分区起点,一次性播种,不规划/下发 v2 单元
        if ("external".equals(t.incrementalEngine()) && externalDispatcher != null) {
            Map<Integer, Long> starts = new LinkedHashMap<>();
            String topic = null, stream = null;
            for (PlannedUnit pu : planned) {
                topic = pu.topic();
                stream = pu.stream();
                for (int p : pu.partitions()) {
                    StartOffset so = startOffsets.startOffset(pu.topic(), p, t.epoch(), t0);
                    starts.put(p, so.offset());
                }
            }
            externalDispatcher.dispatch(t, topic, stream, starts);
            t.status(TaskStatus.RUNNING);
            tasks.update(t);
            return;
        }

        long now = clock.getAsLong();
        for (PlannedUnit pu : planned) {
            Map<Integer, Long> starts = new LinkedHashMap<>();
            for (int p : pu.partitions()) {
                StartOffset so = startOffsets.startOffset(pu.topic(), p, t.epoch(), t0);
                starts.put(p, so.offset());
                leases.grant(pu.unitId(), t.taskId(), pu.topic(), p, t.epoch(), leaseTtlMillis, now);
            }
            dispatcher.dispatch(new DispatchRequest(
                    "req-" + pu.unitId() + "-" + t.epoch(), pu.unitId(), t.taskId(), pu.topic(), pu.stream(),
                    t.contractRef(),
                    pu.partitions(), starts, t.consumerGroup(), t.logicId(), t.logicVersion(),
                    t.params(), t.failure(), leaseTtlMillis));
            units.markDispatched(pu.unitId());
        }
        t.status(TaskStatus.RUNNING);
        tasks.update(t);
    }

    /** 只从最新起:t0 = 各分区水位的最大值。 */
    private long latestOf(TaskRecord t) {
        long max = 0L;
        for (PlannedUnit pu : units.byTask(t.taskId())) {
            for (int p : pu.partitions()) {
                max = Math.max(max, latestTso.latestTso(pu.topic(), p, t.epoch()));
            }
        }
        return max;
    }

    public LeaseManager leases() { return leases; }
}
