package com.dcp.outlet.control;

import java.util.ArrayList;
import java.util.List;

/**
 * 切片策略(D18,由**平台策略**控制,任务不可配):
 * <ul>
 *   <li>{@code < 10 万} → 单片(WHOLE)</li>
 *   <li>{@code 10 万 ~ 100 万} → 按分区(PARTITION)</li>
 *   <li>{@code ≥ 100 万} → **区间 × 分区**(RANGE):每分区按量再分区间</li>
 * </ul>
 * 切片的意义:失败重做时只需重跑坏片;并且**全片成功才合并**提交(批阶段不碰 Kafka,
 * 但目标表的写入仍需一致视图 → 见批流交接)。
 */
public class SlotPlanner {

    private final long singleThreshold;
    private final long partitionThreshold;
    private final long rangeChunk;
    private final int maxSlices;

    public SlotPlanner(long singleThreshold, long partitionThreshold, long rangeChunk, int maxSlices) {
        this.singleThreshold = singleThreshold;
        this.partitionThreshold = partitionThreshold;
        this.rangeChunk = rangeChunk;
        this.maxSlices = maxSlices;
    }

    /** 默认:10 万 / 100 万 / 每区间 100 万 / 最多 512 片。 */
    public static SlotPlanner defaults() {
        return new SlotPlanner(100_000L, 1_000_000L, 1_000_000L, 512);
    }

    public List<Slice> plan(String stream, long estimatedRows, int partitions) {
        List<Slice> out = new ArrayList<>();
        if (partitions <= 0) partitions = 1;

        if (estimatedRows >= 0 && estimatedRows < singleThreshold) {
            out.add(new Slice(stream, Slice.Kind.WHOLE, 0, Long.MIN_VALUE, Long.MAX_VALUE, 0, 1));
            return out;
        }
        if (estimatedRows < 0 || estimatedRows < partitionThreshold) {
            for (int p = 0; p < partitions; p++) {
                out.add(new Slice(stream, Slice.Kind.PARTITION, p, Long.MIN_VALUE, Long.MAX_VALUE, p, partitions));
            }
            return out;
        }
        // 区间 × 分区
        long perPartition = estimatedRows / partitions;
        int chunksPerPartition = (int) Math.max(1, (perPartition + rangeChunk - 1) / rangeChunk);
        long start = 0;
        for (int p = 0; p < partitions && out.size() < maxSlices; p++) {
            long lo = start;
            for (int c = 0; c < chunksPerPartition && out.size() < maxSlices; c++) {
                long hi = lo + rangeChunk;
                out.add(new Slice(stream, Slice.Kind.RANGE, p, lo, hi, out.size(), -1));
                lo = hi;
            }
            start = lo;
        }
        return out;
    }
}
