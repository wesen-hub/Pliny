package com.dcp.outlet.control.support;

import com.dcp.outlet.control.PartitionReader;
import com.dcp.outlet.control.TimeUtil;
import com.dcp.outlet.control.TsoSample;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 数组版分区读取器:用 {@code commitTs[offset]} 模拟一个分区的消息。
 * 同时模拟 Kafka 原生时间索引(近似:返回第一个 wallTime ≥ 目标的 offset)。
 */
public class ArrayPartitionReader implements PartitionReader {

    private final Map<String, long[]> commitTs = new LinkedHashMap<>();
    private long readRows;      // 统计:累计读取行数(用于验证短扫成本)

    private static String key(String topic, int partition) { return topic + "#" + partition; }

    public void put(String topic, int partition, long[] commitTsByOffset) {
        commitTs.put(key(topic, partition), commitTsByOffset);
    }

    public void putMonotonic(String topic, int partition, int rows, long startTso, long step, int txSize) {
        long[] arr = new long[rows];
        long tso = startTso;
        for (int i = 0; i < rows; i++) {
            if (i > 0 && i % txSize == 0) tso += step;   // 同一事务内的多行共享同一 commitTs
            arr[i] = tso;
        }
        commitTs.put(key(topic, partition), arr);
    }

    private long[] arr(String topic, int partition) {
        long[] a = commitTs.get(key(topic, partition));
        if (a == null) throw new IllegalArgumentException("no data for " + topic + "#" + partition);
        return a;
    }

    @Override
    public long earliestOffset(String topic, int partition) { return 0; }

    @Override
    public long latestOffset(String topic, int partition) { return arr(topic, partition).length; }

    @Override
    public List<TsoSample> readFrom(String topic, int partition, long fromOffset, int maxRows) {
        long[] a = arr(topic, partition);
        List<TsoSample> out = new ArrayList<>();
        for (long i = Math.max(0, fromOffset); i < a.length && out.size() < maxRows; i++) {
            out.add(new TsoSample(i, a[(int) i]));
        }
        readRows += out.size();
        return out;
    }

    @Override
    public Long offsetForTime(String topic, int partition, long timeMillis) {
        long[] a = arr(topic, partition);
        for (int i = 0; i < a.length; i++) {
            if (TimeUtil.tsoToMillis(a[i]) >= timeMillis) return (long) i;
        }
        return (long) a.length;      // 时间超过所有事件 → 指向末尾
    }

    public long readRows() { return readRows; }

    public void resetStats() { readRows = 0; }

    /** 暴力解(对照用):第一个 commitTs > tso 的 offset,无则返回末尾。 */
    public long bruteForce(String topic, int partition, long tso) {
        long[] a = arr(topic, partition);
        for (int i = 0; i < a.length; i++) {
            if (a[i] > tso) return i;
        }
        return a.length;
    }
}
