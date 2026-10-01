package com.dcp.outlet.service.schedulerbridge;

import com.dcp.outlet.adapters.TaskCommandCodec;
import com.dcp.outlet.control.StreamRegistry;
import com.dcp.outlet.control.TaskFrameworkProbe;
import com.dcp.outlet.control.TaskRecord;
import com.dcp.scheduler.model.Enums;
import com.dcp.scheduler.model.ExecutorSnapshot;
import com.dcp.scheduler.model.TaskSpec;
import com.dcp.scheduler.model.TaskStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 统一调度框架 · outlet 外壳提交端(方案 C2 ② 终态):控制面进程只负责把消费任务转成框架任务
 * 经 dcp-worker REST 提交(ns 随任务透传),**不参选 outlet 调度主、不跑执行器、不直连 etcd**;
 * 归属分配 / 判死 / 迁移 / **防双消费(框架归属键唯一)** 由 worker 进程内嵌的调度实例统一完成。
 *
 * <pre>
 *   TaskSpec 形态(一个消费任务 = 每流一个框架任务,框架 taskId = taskId#stream):
 *     namespace=任务登记 ns(随任务透传: tag/lake/sub/ingest; 缺省 outlet),
 *     topic=消费 topic, partitions=真实分区数(min=max, 禁自动扩缩),
 *     handover=CONSERVATIVE(租约双窗口保守交接,防双消费), handlerRef={SPI, outlet-task},
 *     phase=BATCHING(批阶段,框架只登记不分配)| PAUSED(任务级暂停)| null(RUNNING),
 *     bizMeta = TaskCommandCodec.encode(...)(全分区起点表/契约/逻辑版本全透传,执行器按 ctx.partId 取本分区起点)。
 * </pre>
 */
@Component
public class OutletFrameworkRuntime implements TaskFrameworkProbe {

    private static final Logger log = LoggerFactory.getLogger(OutletFrameworkRuntime.class);

    private final StreamRegistry streams;
    private final WorkerClient worker;

    public OutletFrameworkRuntime(StreamRegistry streams, WorkerClient worker) {
        this.streams = streams;
        this.worker = worker;
    }

    /* ── 任务生命周期(外壳 → 框架) ── */

    /** 批阶段登记:spec(phase=BATCHING)先入框架,状态统一由框架表达,框架只登记不分配。 */
    public void submitBatching(TaskRecord task, String topic, String stream, int partitions) {
        TaskSpec spec = baseSpec(task, topic, stream, partitions);
        spec.phase = "BATCHING";
        worker.submitSpec(spec);
        log.info("[scheduler] 批阶段登记 outlet TaskSpec: task={} stream={} topic={} partitions={}",
                task.taskId(), stream, topic, partitions);
    }

    /** 批完成/直接交接:去 phase + bizMeta 起点表 → 框架推进 RUNNING 分配。 */
    public void activate(TaskRecord task, String topic, String kafkaBootstrap, String stream,
                         Map<Integer, Long> startOffsets, Map<Integer, Long> startTsos) {
        int partitions = startOffsets.size();
        TaskSpec spec = baseSpec(task, topic, stream, partitions);
        spec.bizMeta = new HashMap<>(TaskCommandCodec.encode(task, topic, kafkaBootstrap, stream,
                new ArrayList<>(startOffsets.keySet()), startOffsets, startTsos));
        worker.submitSpec(spec);
        log.info("[scheduler] 激活 outlet TaskSpec: task={} stream={} topic={} kafka={} 起点分区={}",
                task.taskId(), stream, topic, kafkaBootstrap, partitions);
    }

    /** 任务级暂停/恢复:spec.phase = PAUSED / null(框架追平回收 / 重新分配)。 */
    public void setPhase(String ns, String taskId, String phase) {
        for (String ftid : frameworkTaskIdsOf(ns, taskId)) {
            TaskSpec spec = worker.getSpec(ns, ftid).orElse(null);
            if (spec == null) continue;
            spec.phase = phase;
            worker.submitSpec(spec);
            log.info("[scheduler] 任务 {} 框架阶段 → {}: ftid={}", taskId, phase, ftid);
        }
    }

    /** 任务停止:删全部流的 TaskSpec(框架推进 DRAINING → GONE;重写 spec 即重新上线)。 */
    public void cancelTask(String ns, String taskId) {
        for (String ftid : frameworkTaskIdsOf(ns, taskId)) {
            worker.deleteSpec(ns, ftid);
            log.info("[scheduler] 注销 TaskSpec(ns={}): ftid={}", ns, ftid);
        }
    }

    /** 批失败:写框架事件键 batch-failed(巡检读此告警;spec 保持 BATCHING 供重做批)。 */
    public void markBatchFailed(String ns, String taskId, String detail) {
        Map<String, Object> ev = new HashMap<>();
        ev.put("type", "batch-failed");
        ev.put("detail", detail);
        ev.put("at", System.currentTimeMillis());
        worker.putEvent(ns, taskId, "batch", ev);
        log.warn("[scheduler] 任务 {} 批阶段失败: {}", taskId, detail);
    }

    /* ── 探针(巡检/视图读框架事实,经 worker 读 API) ── */

    /** 任务是否仍有框架 TaskSpec(任一流的 spec 存在即视为托管中;stop 删 spec 后为 false)。 */
    public boolean hasTask(String ns, String taskId) {
        return !frameworkTaskIdsOf(ns, taskId).isEmpty();
    }

    @Override
    public String phaseOf(String ns, String taskId) {
        List<String> ftids = frameworkTaskIdsOf(ns, taskId);
        if (ftids.isEmpty()) return null;
        String agg = null;
        for (String ftid : ftids) {
            TaskSpec spec = worker.getSpec(ns, ftid).orElse(null);
            if (spec == null) return null; // 任一流的 spec 缺失 → 框架无完整任务
            if (spec.batching()) agg = "BATCHING";
            else if (spec.paused() && !"BATCHING".equals(agg)) agg = "PAUSED";
        }
        return agg == null ? "RUNNING" : agg;
    }

    @Override
    public boolean batchFailed(String ns, String taskId) {
        return worker.getEvent(ns, taskId, "batch").isPresent();
    }

    @Override
    public String stateOf(String ns, String taskId) {
        List<String> ftids = frameworkTaskIdsOf(ns, taskId);
        if (ftids.isEmpty()) return null;
        // 框架任务按流拆键,聚合取"最活跃"状态
        String agg = null;
        for (String ftid : ftids) {
            TaskStatus s = worker.getStatus(ns, ftid).orElse(null);
            if (s == null) continue;
            if (agg == null || Enums.TaskState.RUNNING.equals(s.state)) agg = s.state;
        }
        return agg;
    }

    /** 任务各流的分区快照(进度页数据源):框架任务 partitions=真实分区数,快照键 parts/{p}/snap。 */
    public Map<String, Map<Integer, ExecutorSnapshot>> partitionSnapshotsOfTask(String ns, String taskId) {
        Map<String, Map<Integer, ExecutorSnapshot>> out = new LinkedHashMap<>();
        for (String ftid : frameworkTaskIdsOf(ns, taskId)) {
            Map<Integer, ExecutorSnapshot> byPart = new LinkedHashMap<>();
            String stream = ftid.substring(taskId.length() + 1);
            int parts = partitionsOf(stream);
            Map<Integer, ExecutorSnapshot> snaps = worker.getSnaps(ns, ftid);
            for (int p = 0; p < parts; p++) {
                ExecutorSnapshot snap = snaps.get(p);
                if (snap != null) byPart.put(p, snap);
            }
            out.put(stream, byPart);
        }
        return out;
    }

    /* ── 内部辅助 ── */

    /** 框架任务 id = 任务 id#逻辑表名(多流任务每流一个框架任务,防双消费仍由框架归属键统一保证)。 */
    private static String frameworkTaskId(String taskId, String stream) {
        return taskId + "#" + stream;
    }

    /** 任务的全部框架任务 id(按流):经 worker 读 API 列举该 ns 的 spec 键。 */
    private List<String> frameworkTaskIdsOf(String ns, String taskId) {
        List<String> out = new ArrayList<>();
        for (String ftid : worker.listTasks(ns)) {
            if (ftid.startsWith(taskId + "#")) out.add(ftid);
        }
        return out;
    }

    /** 流的物理分区数(登记表)。 */
    private int partitionsOf(String stream) {
        return streams.get(stream).map(s -> s.partitions()).orElse(0);
    }

    private TaskSpec baseSpec(TaskRecord task, String topic, String stream, int partitions) {
        TaskSpec spec = new TaskSpec();
        spec.namespace = task.namespace();
        spec.taskId = frameworkTaskId(task.taskId(), stream);
        spec.topic = topic;
        // 一个任务 = 每流一个框架任务,partitions = 真实分区数(min=max 禁自动扩缩)
        spec.partitions = partitions;
        spec.minPartitions = partitions;
        spec.maxPartitions = partitions;
        spec.targetRps = 0;
        spec.handlerRef = new TaskSpec.HandlerRef();
        spec.handlerRef.type = Enums.HandlerType.SPI;
        spec.handlerRef.ref = "outlet-task";
        spec.handover = Enums.Handover.CONSERVATIVE;  // 消费底座:租约双窗口保守交接,防双消费
        return spec;
    }
}
