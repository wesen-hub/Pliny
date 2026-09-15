package com.dcp.outlet.control;

import java.util.List;

/**
 * 分区读取端口:索引器采样与起点短扫共用。
 *
 * <p><b>只读轻量字段</b>(offset / commitTs),不物化业务数据 —— 这是锚点方案成本可控的前提。
 */
public interface PartitionReader {

    long earliestOffset(String topic, int partition);

    long latestOffset(String topic, int partition);

    /**
     * 从 {@code fromOffset} 起顺序读,最多 {@code maxRows} 条。
     * 到末尾时返回长度可能小于 {@code maxRows}(空列表表示已到末尾)。
     */
    List<TsoSample> readFrom(String topic, int partition, long fromOffset, int maxRows);

    /** Kafka 原生时间索引:第一个时间 ≥ {@code timeMillis} 的 offset;不可用时返回 {@code null}。 */
    Long offsetForTime(String topic, int partition, long timeMillis);
}
