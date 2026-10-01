package com.dcp.outlet.control;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.LongSupplier;

/**
 * 进度口径:把「单元上报的 offset/TSO」+「分区水位」+「分区末端」合成前端要看的滞后。
 *
 * <p>两个口径各自有意义:
 * <ul>
 *   <li>{@code lagSeconds} = 水位 TSO 与已处理 TSO 的时间差 —— **业务延迟**(SLA 主指标)</li>
 *   <li>{@code lagRows} = 分区末端 - 已提交位点 —— **积压条数**(容量/背压参考)</li>
 * </ul>
 * 注意 {@code safeWatermark} 的语义是"≤此 TSO 的事件已全部到齐",**不是**"已处理到",两者不要混用。
 */
public class ProgressService {

    private final PartitionReader reader;
    private final WatermarkStore watermarks;
    private final LongSupplier clock;

    public ProgressService(PartitionReader reader, WatermarkStore watermarks, LongSupplier clock) {
        this.reader = reader; this.watermarks = watermarks; this.clock = clock;
    }

    public ProgressService(PartitionReader reader, WatermarkStore watermarks) {
        this(reader, watermarks, System::currentTimeMillis);
    }

    /** 只算时间滞后(不查分区末端,避免每轮都打 Kafka)。 */
    public PartitionProgress progressOfUnit(String unitId, String topic, int partition, int epoch,
                                            String state, long offset, long tso, long lastSeenAt) {
        Watermark w = watermarks.latest(topic, partition, epoch);
        long wm = w == null ? 0L : w.watermarkTs();
        long lagSeconds = (wm > 0 && tso > 0) ? Math.max(0L, (wm - tso) >> 18) / 1000 : 0L;
        return new PartitionProgress(unitId, topic, partition, state, offset, tso, wm,
                -1L, -1L, lagSeconds, lastSeenAt);
    }

    /** 完整进度:额外查一次分区末端,得出积压条数(前端列表页用)。 */
    public PartitionProgress fullProgressOfUnit(String unitId, String topic, int partition, int epoch,
                                                String state, long offset, long tso, long lastSeenAt) {
        PartitionProgress p = progressOfUnit(unitId, topic, partition, epoch, state, offset, tso, lastSeenAt);
        long latest;
        try {
            latest = reader.latestOffset(topic, partition);
        } catch (RuntimeException e) {
            latest = -1L;                     // Kafka 不可达时不阻塞页面
        }
        long lagRows = latest >= 0 ? Math.max(0L, latest - offset) : -1L;
        return new PartitionProgress(unitId, topic, partition, state, offset, tso,
                p.watermarkTs(), latest, lagRows, p.lagSeconds(), lastSeenAt);
    }

    /** 任务级:逐分区展开。任务 = 框架一个 TaskSpec(partitions = 真实分区数),
     *  每分区状态/位点由框架快照派生(调用方注入 committed/state 映射);
     *  框架快照无 TSO,故业务延迟(lagSeconds)不可算、归 0,积压条数(lagRows)由 offset 与分区末端得出。 */
    public List<PartitionProgress> progressOfTask(String taskId, String topic, List<Integer> partIds,
                                                  int epoch, Map<Integer, Long> committed,
                                                  Map<Integer, String> states, boolean queryKafka) {
        List<PartitionProgress> out = new ArrayList<>();
        for (int p : partIds) {
            String state = states.getOrDefault(p, "SCHEDULED");
            long offset = committed.getOrDefault(p, 0L);
            out.add(queryKafka
                    ? fullProgressOfUnit(taskId, topic, p, epoch, state, offset, 0L, 0L)
                    : progressOfUnit(taskId, topic, p, epoch, state, offset, 0L, 0L));
        }
        return out;
    }
}
