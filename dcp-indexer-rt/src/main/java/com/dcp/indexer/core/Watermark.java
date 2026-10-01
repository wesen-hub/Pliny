package com.dcp.indexer.core;

/** 分区水位:≤ 该 TSO 的事件"已全部到齐"(来自 TiCDC canal-json 的 TIDB_WATERMARK)。 */
public final class Watermark {

    private final String topic;
    private final int partition;
    private final int epoch;
    private final long watermarkTs;
    private final long offset;
    private final long updatedAt;

    public Watermark(String topic, int partition, int epoch, long watermarkTs, long offset, long updatedAt) {
        this.topic = topic;
        this.partition = partition;
        this.epoch = epoch;
        this.watermarkTs = watermarkTs;
        this.offset = offset;
        this.updatedAt = updatedAt;
    }

    public String topic() { return topic; }
    public int partition() { return partition; }
    public int epoch() { return epoch; }
    public long watermarkTs() { return watermarkTs; }
    public long offset() { return offset; }
    public long updatedAt() { return updatedAt; }
}
