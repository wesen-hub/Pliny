package com.dcp.outlet.control;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 起点服务(v2 核心算法, 栅栏方案 §3.2):把批边界 TSO 换算成"每分区从哪个 offset 开始消费"。
 *
 * <p><b>为什么不能用"最新 offset"当起点</b>:批读的是 {@code AS OF T0} 的快照,
 * 而从 T0 到交接时刻之间产生的事件永远不会被批读到 —— 若起点跳到最新,这段事件就**静默丢失**。
 * 只有"由 T0 反算"才能保证批({@code ≤T0})与流({@code >T0})恰好接上。
 *
 * <p><b>算法(单分区, 纯索引查询, 永不失败)</b>:
 * <ol>
 *   <li>查 {@code commitTs ≤ T0} 的最大<b>栅栏点</b> F → 起点 = max(F.offset, logStart)(retention 规范化);</li>
 *   <li>F 缺失 → 起点 = earliest, 并做边界审计:logStart 首条 commitTs > T0 则告警(可能越界或被删);</li>
 *   <li>精确边界由<b>出口持续过滤</b>(§3.3)承接:从 F 起消费, 逐条丢弃 tso ≤ T0。</li>
 * </ol>
 *
 * <p>退役机制:短扫定位、maxScanRows、StartTooOldException、Kafka 时间索引粗定位、单调性回退降级。
 */
public class StartOffsetService {

    private final AnchorStore anchors;
    private final PartitionReader reader;
    /** 可空:边界审计告警出口(栅栏缺失且 logStart 首条越过 T0) */
    private final java.util.function.BiConsumer<String, String> alert;

    public StartOffsetService(AnchorStore anchors, PartitionReader reader) {
        this(anchors, reader, null);
    }

    public StartOffsetService(AnchorStore anchors, PartitionReader reader,
                              java.util.function.BiConsumer<String, String> alert) {
        this.anchors = anchors;
        this.reader = reader;
        this.alert = alert;
    }

    /** 多分区一次性求起点(一任务多分区时用)。 */
    public Map<Integer, StartOffset> offsetsOf(String topic, List<Integer> partitions, int epoch, long tso) {
        Map<Integer, StartOffset> out = new LinkedHashMap<>();
        for (int p : partitions) {
            out.put(p, startOffset(topic, p, epoch, tso));
        }
        return out;
    }

    /** 单分区求起点(纯索引查询; 返回的 offset 是过滤起点, 精确边界由出口持续过滤承接)。 */
    public StartOffset startOffset(String topic, int partition, int epoch, long tso) {
        long earliest = reader.earliestOffset(topic, partition);
        Anchor fence = anchors.lastFenceAtOrBefore(topic, partition, epoch, tso);
        if (fence != null) {
            // retention 规范化(P):F 被物理删除时退化为 logStart, 不越界
            long from = Math.max(fence.offset(), earliest);
            return new StartOffset(partition, from, StartOffset.Source.FENCE, 0);
        }
        // F 缺失 → fallback earliest(永远安全)+ 边界审计(§3.2-②)
        List<TsoSample> first = reader.readFrom(topic, partition, earliest, 1);
        if (!first.isEmpty() && first.get(0).commitTs() > tso && alert != null) {
            alert.accept(topic + "-" + partition,
                    "栅栏缺失且 logStart 首条 commitTs=" + first.get(0).commitTs()
                            + " > T0=" + tso + ":可能越过边界或被 retention 删除(出口过滤兜底)");
        }
        return new StartOffset(partition, earliest, StartOffset.Source.FALLBACK_EARLIEST, 0);
    }
}
