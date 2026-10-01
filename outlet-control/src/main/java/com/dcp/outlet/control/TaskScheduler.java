package com.dcp.outlet.control;

import com.dcp.outlet.sdk.SdkLogger;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 调度器:把任务推进到"已交接给统一调度框架"。
 *
 * <pre>
 * onCreated ── startPolicy=snapshot ──▶ 登记批阶段(spec phase=BATCHING,框架不分配)──成功──▶ 交接
 *            └ startPolicy=latest   ──▶ 直接交接(t0 = 最新水位)
 * 交接:逐分区算起点(每分区第一个 commitTs > T0)→ activate(spec 去 phase + bizMeta 起点表)
 *       —— 一个任务 = 框架一个 TaskSpec(partitions = 真实分区数),归属分配/判死/迁移/防双消费
 *       (框架归属键唯一)全部由框架统一负责;租约与推模式下发已退位,唯一出口 = {@link FrameworkTaskSink}。
 * </pre>
 */
public class TaskScheduler {

    private final TaskStore tasks;
    private final StreamRegistry streams;
    private final StartOffsetService startOffsets;
    private final LatestTsoProvider latestTso;
    private final BatchRunner batchRunner;
    private final ExternalIncrementalDispatcher externalDispatcher;
    private final FrameworkTaskSink frameworkSink;
    private final SdkLogger log;
    private final HandoverFacts handoverFacts;   // 批交接物化事实(null=测试/无批场景, 跳过 handover_at 回写)
    private final int epoch;   // 全局规划代(锚点索引维度,统一由配置 outlet.epoch 管理)

    public TaskScheduler(TaskStore tasks, StreamRegistry streams, StartOffsetService startOffsets,
                         LatestTsoProvider latestTso, BatchRunner batchRunner,
                         ExternalIncrementalDispatcher externalDispatcher, FrameworkTaskSink frameworkSink,
                         int epoch) {
        this(tasks, streams, startOffsets, latestTso, batchRunner, externalDispatcher,
                frameworkSink, epoch, null, null);
    }

    public TaskScheduler(TaskStore tasks, StreamRegistry streams, StartOffsetService startOffsets,
                         LatestTsoProvider latestTso, BatchRunner batchRunner,
                         ExternalIncrementalDispatcher externalDispatcher, FrameworkTaskSink frameworkSink,
                         int epoch, SdkLogger log, HandoverFacts handoverFacts) {
        this.tasks = tasks; this.streams = streams;
        this.startOffsets = startOffsets; this.latestTso = latestTso;
        this.batchRunner = batchRunner; this.externalDispatcher = externalDispatcher;
        this.frameworkSink = frameworkSink; this.log = log; this.handoverFacts = handoverFacts;
        this.epoch = epoch;
    }

    /** 任务创建后:决定"先批打底"还是"直接从最新起"。 */
    public void onCreated(TaskRecord task) {
        if ("external".equals(task.incrementalEngine())) {
            // 外部引擎任务(Flink):不落框架,直接批打底后一次性播种
            if ("snapshot".equals(task.startPolicy())) {
                batchRunner.runAsync(task.taskId(), (t0, ok) -> onBatchDone(task.taskId(), t0, ok));
            } else {
                handover(task.taskId(), latestOf(task));
            }
            return;
        }
        if ("snapshot".equals(task.startPolicy())) {
            // 批阶段登记:spec(BATCHING)先入框架,状态统一由框架表达
            for (String s : task.streams()) {
                streams.get(s).ifPresent(si -> frameworkSink.submitBatching(task, si.topic(), si.stream(), si.partitions()));
            }
            batchRunner.runAsync(task.taskId(), (t0, ok) -> onBatchDone(task.taskId(), t0, ok));
        } else {
            handover(task.taskId(), latestOf(task));
        }
    }

    /** 批阶段完成回调。失败 → markBatchFailed(框架事件,巡检告警;**不猜起点**,人工触发重做批换新 T0)。
     *
     *  <p>成功 → handover 包 try-catch:activate 失败**不归并批失败**(批已 SUCCEEDED, t0 已物化)——
     *  batch_job 行 SUCCEEDED ∧ handover_at 空即 handover-pending 判据,由 Supervisor 巡检补交接
     *  (幂等 put 覆盖)。归并会让"交接失败"误报成"批失败需人工重做批"(重做批是错误药方)。 */
    public void onBatchDone(String taskId, long t0, boolean ok) {
        TaskRecord t = tasks.byId(taskId);
        if (!ok) {
            frameworkSink.markBatchFailed(t == null ? "outlet" : t.namespace(), taskId, "批阶段失败,需人工重做批(换新 T0)");
            return;
        }
        try {
            handover(taskId, t0);                       // 批流交接
        } catch (Exception e) {
            if (log != null) log.error("批完成但交接失败(留 handover-pending 判据,巡检补交接): task={} err={}",
                    taskId, e.getMessage());
        }
    }

    /** 交接:算起点(每分区第一个 {@code commitTs > T0})→ activate(框架接管) 或 播给外部引擎。 */
    public void handover(String taskId, long t0) {
        TaskRecord t = tasks.byId(taskId);

        // 方案 C:增量段 = 外部引擎(Flink)—— 算全部分区起点,一次性播种
        if ("external".equals(t.incrementalEngine())) {
            if (externalDispatcher == null) {
                // 未接线:不能静默回退(语义变了),fail-closed 置批失败(巡检会告警,人工介入接线)
                frameworkSink.markBatchFailed(t.namespace(), taskId, "外部引擎未接线(externalDispatcher=null)");
                return;
            }
            Map<Integer, Long> starts = new LinkedHashMap<>();
            String topic = null, stream = null;
            for (String s : t.streams()) {
                var si = streams.get(s).orElse(null);
                if (si == null) continue;
                topic = si.topic(); stream = si.stream();
                for (int p = 0; p < si.partitions(); p++) {
                    StartOffset so = startOffsets.startOffset(si.topic(), p, epoch, t0);
                    starts.put(p, so.offset());
                }
            }
            externalDispatcher.dispatch(t, topic, stream, starts);
            markHandoverDone(taskId);
            return;
        }

        for (String s : t.streams()) {
            var si = streams.get(s).orElse(null);
            if (si == null) continue;
            Map<Integer, Long> starts = new LinkedHashMap<>();
            Map<Integer, Long> tsos = new LinkedHashMap<>();
            for (int p = 0; p < si.partitions(); p++) {
                StartOffset so = startOffsets.startOffset(si.topic(), p, epoch, t0);
                starts.put(p, so.offset());
                tsos.put(p, t0);          // 会话过滤边界 = 起点查询所用的 T0(口径一致, C2)
            }
            // 框架接管(唯一出口):任务级 activate,防双消费由框架归属键统一保证;bootstrap 随指令透传(ns 与 Kafka 解耦)
            frameworkSink.activate(t, si.topic(), si.kafkaBootstrap(), si.stream(), starts, tsos);
        }
        markHandoverDone(taskId);
    }

    /** 交接完成回写(CP2b 判据收敛依据:回写失败静默,下轮判据再命中,补交接幂等重放)。 */
    private void markHandoverDone(String taskId) {
        if (handoverFacts != null) handoverFacts.markHandover(taskId);
    }

    /** 只从最新起:t0 = 各分区水位的最大值。 */
    private long latestOf(TaskRecord t) {
        long max = 0L;
        for (String s : t.streams()) {
            var si = streams.get(s).orElse(null);
            if (si == null) continue;
            for (int p = 0; p < si.partitions(); p++) {
                max = Math.max(max, latestTso.latestTso(si.topic(), p, epoch));
            }
        }
        return max;
    }
}
