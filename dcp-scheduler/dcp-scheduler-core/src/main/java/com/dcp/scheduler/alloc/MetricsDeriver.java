package com.dcp.scheduler.alloc;

import com.dcp.scheduler.model.Enums;
import com.dcp.scheduler.model.ExecutorSnapshot;
import com.dcp.scheduler.model.InstanceMetrics;
import com.dcp.scheduler.model.InstanceRecord;
import com.dcp.scheduler.model.NsInstanceRecord;
import com.dcp.scheduler.model.NsMetrics;
import com.dcp.scheduler.model.PartMetrics;
import com.dcp.scheduler.model.PartRecord;
import com.dcp.scheduler.model.TaskMetrics;
import com.dcp.scheduler.model.TaskSpec;
import com.dcp.scheduler.model.TaskStatus;
import com.dcp.scheduler.store.EtcdStore;
import com.dcp.scheduler.store.Json;
import com.dcp.scheduler.store.KeySpace;
import io.etcd.jetcd.ByteSequence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 监控聚合管线(监控子文档 §二):主侧每决策轮把四个原始计数器派生为分区级指标,
 * 再向上三级聚合(任务/实例/ns)落 etcd,后台页面直读不经计算。
 *   - 分区级:rps 复用 Autoscaler 相邻帧差分(同一决策轮,不重复差分);
 *     errorRate = Δerror/Δprocessed;avgLatency = ΔtotalLatency/Δprocessed;
 *     lag = endOffset − committedOffset(无 Kafka 源 = -1)。
 *   - 差分帧独立维护(与 Autoscaler/告警巡检各自为政),换主重置,首帧无差分记 0。
 *   - 失效键清理:任务注销/实例下线后残留的 metrics 键随决策轮清除(后台永不读到陈旧数据)。
 * 线程模型:主决策单线程调用(与 Autoscaler 同轮,读一致快照)。
 */
public class MetricsDeriver {

    private static final Logger log = LoggerFactory.getLogger(MetricsDeriver.class);

    private final String ns;
    private final EtcdStore store;
    /** 分区级差分帧:taskId/partId → {processedCount, errorCount, totalLatencyMs}(ts 用快照 ts 判定新旧)。 */
    private final Map<String, long[]> prevFrame = new HashMap<>();

    public MetricsDeriver(String ns, EtcdStore store) {
        this.ns = ns;
        this.store = store;
    }

    /** 任务聚合累加器(分区计数 + Σ指标 + Σ差分)。 */
    private static final class TaskAgg {
        int parts;
        double rps;
        long processed;
        long error;
        long lag;
        long dProcessed;
        long dError;
        long dLatency;
    }

    /** 实例聚合累加器(承接分区计数 + Σ指标 + Σ差分)。 */
    private static final class InstAgg {
        int parts;
        double rps;
        long processed;
        long error;
        long dProcessed;
        long dError;
        long dLatency;
    }

    /**
     * 一轮派生与四级聚合落键。输入均为当前决策轮已读数据(一致快照):
     *
     * @param allParts   分区归属记录(仅 part 键)
     * @param metas      全局实例身份(机器指标/addr)
     * @param live       本 ns 存活实例注册
     * @param rps        Autoscaler 分区 rps 快照(同轮 refreshRps 结果)
     * @param endOffsets Kafka endOffset 探针结果;null = 无源,lag 置 -1
     */
    public void derive(Map<String, byte[]> allParts, Map<String, InstanceRecord> metas,
                       List<NsInstanceRecord> live, Map<String, Double> rps,
                       Map<String, Map<Integer, Long>> endOffsets) {
        long now = System.currentTimeMillis();
        // 任务全集(spec/status 一次读齐,分区循环内做 topic/lag 缓存)
        Map<String, TaskSpec> specs = new HashMap<>();
        Map<String, TaskStatus> statuses = new HashMap<>();
        for (Map.Entry<String, byte[]> e : store.getPrefix(KeySpace.tasksPrefix(ns)).entrySet()) {
            if (e.getKey().endsWith("/spec")) {
                try {
                    TaskSpec s = Json.fromBytes(e.getValue(), TaskSpec.class);
                    specs.put(s.taskId, s);
                } catch (Exception ignored) { /* 坏 spec 跳过 */ }
            } else if (e.getKey().endsWith("/status")) {
                try {
                    TaskStatus st = Json.fromBytes(e.getValue(), TaskStatus.class);
                    statuses.put(KeySpace.taskIdOfStatusKey(e.getKey()), st);
                } catch (Exception ignored) { /* 坏 status 跳过 */ }
            }
        }
        Map<String, TaskAgg> tasks = new HashMap<>();
        Map<String, InstAgg> insts = new HashMap<>();
        for (NsInstanceRecord i : live) insts.put(i.id, new InstAgg()); // 无分区实例也上榜(机器指标)
        Map<String, long[]> freshPrev = new HashMap<>();

        // ① 分区级:差分派生 + 累加
        for (Map.Entry<String, byte[]> e : allParts.entrySet()) {
            PartRecord p = parseQuiet(e.getValue());
            if (p == null) continue;
            String taskId = KeySpace.taskIdOfPartKey(e.getKey());
            int partId = KeySpace.partIdOf(e.getKey());
            String key = taskId + "/" + partId;
            TaskAgg ta = tasks.computeIfAbsent(taskId, k -> new TaskAgg());
            ta.parts++;
            ExecutorSnapshot snap = store.getObj(KeySpace.snap(ns, taskId, partId), ExecutorSnapshot.class)
                    .orElse(null);
            double r = rps.getOrDefault(key, 0.0);
            long dProcessed = 0, dError = 0, dLatency = 0;
            if (snap != null && !snap.stopped) {
                long[] prev = prevFrame.get(key);
                if (prev != null && snap.processedCount > prev[0]) {
                    dProcessed = snap.processedCount - prev[0];
                    dError = Math.max(0, snap.errorCount - prev[1]);
                    dLatency = Math.max(0, snap.totalLatencyMs - prev[2]);
                }
                freshPrev.put(key, new long[]{snap.processedCount, snap.errorCount, snap.totalLatencyMs});
                ta.rps += r;
                ta.processed += snap.processedCount;
                ta.error += snap.errorCount;
                ta.dProcessed += dProcessed;
                ta.dError += dError;
                ta.dLatency += dLatency;
                Long lag = lagOf(specs.get(taskId), statuses.get(taskId), partId, snap, endOffsets);
                if (lag != null) ta.lag += lag;
            }
            // 实例累加:承接分区(owner 非空)不管快照是否新鲜都计数
            if (p.owner != null && !Enums.PartState.PENDING.equals(p.state)) {
                InstAgg ia = insts.computeIfAbsent(p.owner, k -> new InstAgg());
                ia.parts++;
                if (snap != null && !snap.stopped) {
                    ia.rps += r;
                    ia.processed += snap.processedCount;
                    ia.error += snap.errorCount;
                    ia.dProcessed += dProcessed;
                    ia.dError += dError;
                    ia.dLatency += dLatency;
                }
            }
            store.put(KeySpace.partMetrics(ns, taskId, partId),
                    partMetricsOf(now, taskId, partId, p, snap, r, dProcessed, dError, dLatency,
                            lagOf(specs.get(taskId), statuses.get(taskId), partId, snap, endOffsets)));
        }
        prevFrame.clear();
        prevFrame.putAll(freshPrev); // 重建:删除的分区不残留

        // ② 任务级聚合
        for (Map.Entry<String, TaskAgg> e : tasks.entrySet()) {
            String taskId = e.getKey();
            TaskAgg ta = e.getValue();
            TaskSpec spec = specs.get(taskId);
            TaskStatus st = statuses.get(taskId);
            TaskMetrics m = new TaskMetrics();
            m.ts = now;
            m.taskId = taskId;
            m.state = st != null ? st.state : null;
            m.parts = ta.parts;
            m.expectedParts = st != null && st.expectedParts != null ? st.expectedParts
                    : (spec != null ? spec.partitions : 0);
            m.minParts = spec != null ? spec.minPartitions : 0;
            m.maxParts = spec != null ? spec.maxPartitions : 0;
            m.targetRps = spec != null ? spec.targetRps : 0;
            m.rps = ta.rps;
            m.processedCount = ta.processed;
            m.errorCount = ta.error;
            m.errorRate = ta.dProcessed > 0 ? (double) ta.dError / ta.dProcessed : 0;
            m.avgLatencyMs = ta.dProcessed > 0 ? (double) ta.dLatency / ta.dProcessed : 0;
            m.lag = ta.lag;
            store.put(KeySpace.taskMetrics(ns, taskId), m);
        }

        // ③ 实例级聚合(含机器指标与槽位水位)
        Map<String, Integer> capacity = new HashMap<>();
        for (NsInstanceRecord i : live) capacity.put(i.id, i.capacity);
        for (Map.Entry<String, InstAgg> e : insts.entrySet()) {
            String instId = e.getKey();
            InstAgg ia = e.getValue();
            InstanceRecord meta = metas.get(instId);
            InstanceMetrics m = new InstanceMetrics();
            m.ts = now;
            m.instanceId = instId;
            m.addr = meta != null ? meta.addr : null;
            m.parts = ia.parts;
            m.rps = ia.rps;
            m.processedCount = ia.processed;
            m.errorCount = ia.error;
            m.errorRate = ia.dProcessed > 0 ? (double) ia.dError / ia.dProcessed : 0;
            m.avgLatencyMs = ia.dProcessed > 0 ? (double) ia.dLatency / ia.dProcessed : 0;
            if (meta != null) {
                m.cpu = meta.meta.cpu;
                m.mem = meta.meta.mem;
                m.load = meta.meta.load;
            }
            m.capacity = capacity.getOrDefault(instId, 0);
            m.occupancy = m.capacity > 0 ? (double) ia.parts / m.capacity : 0;
            store.put(KeySpace.instMetrics(ns, instId), m);
        }

        // ④ ns 级大盘
        NsMetrics nm = new NsMetrics();
        nm.ts = now;
        nm.ns = ns;
        nm.tasks = specs.size();
        nm.instances = live.size();
        int totalCapacity = 0;
        for (TaskAgg ta : tasks.values()) {
            nm.parts += ta.parts;
            nm.rps += ta.rps;
            nm.processedCount += ta.processed;
            nm.errorCount += ta.error;
            nm.lag += ta.lag;
        }
        for (InstAgg ia : insts.values()) nm.assignedParts += ia.parts;
        for (NsInstanceRecord i : live) totalCapacity += i.capacity;
        nm.totalCapacity = totalCapacity;
        nm.occupancy = totalCapacity > 0 ? (double) nm.assignedParts / totalCapacity : 0;
        store.put(KeySpace.nsMetrics(ns), nm);

        // ⑤ 失效键清理(任务注销/实例下线后残留;metrics 前缀整体对账)
        Set<String> alive = new HashSet<>();
        for (String key : store.getPrefix(KeySpace.metricsPrefix(ns)).keySet()) alive.add(key);
        for (String taskId : tasks.keySet()) {
            for (Map.Entry<String, byte[]> e : allParts.entrySet()) {
                if (taskId.equals(KeySpace.taskIdOfPartKey(e.getKey()))) {
                    alive.remove(KeySpace.partMetrics(ns, taskId, KeySpace.partIdOf(e.getKey()))
                            .toString(StandardCharsets.UTF_8));
                }
            }
            alive.remove(KeySpace.taskMetrics(ns, taskId).toString(StandardCharsets.UTF_8));
        }
        for (String instId : insts.keySet()) {
            alive.remove(KeySpace.instMetrics(ns, instId).toString(StandardCharsets.UTF_8));
        }
        alive.remove(KeySpace.nsMetrics(ns).toString(StandardCharsets.UTF_8));
        for (String stale : alive) {
            store.delete(ByteSequence.from(stale, StandardCharsets.UTF_8));
            log.debug("[{}] 清理失效 metrics 键: {}", ns, stale);
        }
    }

    private PartMetrics partMetricsOf(long now, String taskId, int partId, PartRecord p,
                                      ExecutorSnapshot snap, double rps,
                                      long dProcessed, long dError, long dLatency, Long lag) {
        PartMetrics m = new PartMetrics();
        m.ts = now;
        m.taskId = taskId;
        m.partId = partId;
        m.state = p.state;
        m.owner = p.owner;
        m.rps = snap != null && !snap.stopped ? rps : 0;
        m.processedCount = snap != null ? snap.processedCount : 0;
        m.committedOffset = snap != null ? snap.committedOffset : 0;
        m.errorCount = snap != null ? snap.errorCount : 0;
        m.errorRate = dProcessed > 0 ? (double) dError / dProcessed : 0;
        m.avgLatencyMs = dProcessed > 0 ? (double) dLatency / dProcessed : 0;
        m.lag = lag != null ? lag : -1;
        return m;
    }

    /** 分区 lag(条);null = 无 Kafka 源/无 topic(调用方置 -1)。注销任务靠 status.topic 兜底。 */
    private Long lagOf(TaskSpec spec, TaskStatus st, int partId, ExecutorSnapshot snap,
                       Map<String, Map<Integer, Long>> endOffsets) {
        if (endOffsets == null || snap == null) return null;
        String topic = spec != null ? spec.topic : (st != null ? st.topic : null);
        if (topic == null) return null;
        Map<Integer, Long> eo = endOffsets.get(topic);
        if (eo == null) return null;
        Long end = eo.get(partId);
        if (end == null) return null;
        return Math.max(0, end - snap.committedOffset);
    }

    private static PartRecord parseQuiet(byte[] b) {
        try {
            return Json.fromBytes(b, PartRecord.class);
        } catch (Exception e) {
            return null;
        }
    }
}
