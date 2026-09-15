package com.dcp.outlet.worker;

/** 单元指标出口(生产实现 → Prometheus / 上报控制面)。 */
public interface UnitMetrics {

    void batchProcessed(int partition, int rows, long durationMillis, long tso);

    void emptyBatch(int partition);

    void failure(int partition, Throwable error);

    void retry(int partition, int attempt);

    void deadLetter(int partition);

    void commit(int partition, long offset);
}
