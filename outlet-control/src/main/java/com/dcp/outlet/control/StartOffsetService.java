package com.dcp.outlet.control;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 起点服务(v2 核心算法):把批边界 TSO 换算成"每分区第一个 {@code commitTs > TSO} 的 offset"。
 *
 * <p><b>为什么不能用"最新 offset"当起点</b>:批读的是 {@code AS OF T0} 的快照,
 * 而从 T0 到交接时刻之间产生的事件永远不会被批读到 —— 若起点跳到最新,这段事件就**静默丢失**。
 * 只有"由 T0 反算"才能保证批({@code ≤T0})与流({@code >T0})恰好接上。
 *
 * <p><b>算法</b>(单分区):
 * <ol>
 *   <li>二分锚点表,取 {@code commitTs ≤ T0} 的最大锚点(无则退化为时间索引粗定位,再退化从头扫)</li>
 *   <li>从该位置**短扫**,逐条读 {@code commitTs},直到第一条 {@code > T0}</li>
 *   <li>超过 {@code maxScanRows} → 抛 {@link StartTooOldException}(要求重做批)</li>
 * </ol>
 *
 * <p><b>语义</b>:严格 {@code > T0}(半开区间);快照已包含 {@code ≤ T0} 的状态。
 */
public class StartOffsetService {

    private final AnchorStore anchors;
    private final PartitionReader reader;
    private final AnchorIndexPolicy policy;
    /** 可空:命中"单调性回退"的分区强制降级为回扫。 */
    private final MonotonicityGuard guard;
    private final int maxScanRows;

    public StartOffsetService(AnchorStore anchors, PartitionReader reader,
                              AnchorIndexPolicy policy, MonotonicityGuard guard, int maxScanRows) {
        this.anchors = anchors;
        this.reader = reader;
        this.policy = policy;
        this.guard = guard;
        this.maxScanRows = maxScanRows;
    }

    /** 默认:回扫上限 20 万条/分区。 */
    public StartOffsetService(AnchorStore anchors, PartitionReader reader) {
        this(anchors, reader, AnchorIndexPolicy.defaults(), null, 200_000);
    }

    /** 多分区一次性求起点(一任务多分区时用)。 */
    public Map<Integer, StartOffset> offsetsOf(String topic, List<Integer> partitions, int epoch, long tso) {
        Map<Integer, StartOffset> out = new LinkedHashMap<>();
        for (int p : partitions) {
            out.put(p, startOffset(topic, p, epoch, tso));
        }
        return out;
    }

    /** 单分区求起点。 */
    public StartOffset startOffset(String topic, int partition, int epoch, long tso) {
        boolean degraded = guard != null && guard.tainted(topic, partition);

        long from;
        StartOffset.Source source;
        Anchor anchor = degraded ? null : anchors.lastAtOrBefore(topic, partition, epoch, tso);

        if (anchor != null) {
            from = anchor.offset();
            source = StartOffset.Source.ANCHOR;
        } else if (degraded) {
            from = reader.earliestOffset(topic, partition);
            source = StartOffset.Source.RESCAN_DEGRADED;
        } else {
            Long approx = reader.offsetForTime(topic, partition, TimeUtil.tsoToMillis(tso));
            if (approx != null) {
                from = approx;
                source = StartOffset.Source.TIME_INDEX;
            } else {
                from = reader.earliestOffset(topic, partition);
                source = StartOffset.Source.RESCAN_FROM_EARLIEST;
            }
        }

        long cur = Math.max(from, reader.earliestOffset(topic, partition));
        int scanned = 0;
        while (scanned < maxScanRows) {
            int want = Math.min(policy.readBatchRows(), maxScanRows - scanned);
            List<TsoSample> batch = reader.readFrom(topic, partition, cur, want);
            if (batch.isEmpty()) {
                // 已到分区末尾:当前没有需要消费的事件,起点 = 末尾
                return new StartOffset(partition, reader.latestOffset(topic, partition),
                        StartOffset.Source.TAIL, scanned);
            }
            for (TsoSample s : batch) {
                if (s.commitTs() > tso) {
                    return new StartOffset(partition, s.offset(), source, scanned + 1);
                }
                scanned++;
            }
            cur = batch.get(batch.size() - 1).offset() + 1;
        }
        throw new StartTooOldException(topic, partition, tso, scanned);
    }
}
