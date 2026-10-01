package com.dcp.indexer.core;

/**
 * 稀疏锚点:{@code (topic, partition, epoch, offset) → commitTs}。
 *
 * <p><b>锚点是加速器,不是真相源</b> —— 可随时重建;损坏时退化为回扫。
 *
 * <p>{@code fence} = 栅栏点(commitTs ≥ 分区历史最大, 即前缀上界刷新点):
 * 起点查询只信栅栏(栅栏方案 §3.1);非栅栏点仅供排障观测, 不参与起点计算。
 */
public final class Anchor {

    private final String topic;
    private final int partition;
    private final int epoch;
    private final long offset;
    private final long commitTs;
    private final long wallTime;
    private final boolean fence;

    public Anchor(String topic, int partition, int epoch, long offset, long commitTs, long wallTime, boolean fence) {
        this.topic = topic;
        this.partition = partition;
        this.epoch = epoch;
        this.offset = offset;
        this.commitTs = commitTs;
        this.wallTime = wallTime;
        this.fence = fence;
    }

    /** 兼容构造(旧调用点):非栅栏点。 */
    public Anchor(String topic, int partition, int epoch, long offset, long commitTs, long wallTime) {
        this(topic, partition, epoch, offset, commitTs, wallTime, false);
    }

    public String topic() { return topic; }

    public int partition() { return partition; }

    public int epoch() { return epoch; }

    public long offset() { return offset; }

    public long commitTs() { return commitTs; }

    public long wallTime() { return wallTime; }

    public boolean fence() { return fence; }

    @Override public String toString() {
        return "Anchor{" + topic + "-" + partition + " e" + epoch
                + " offset=" + offset + " commitTs=" + commitTs + (fence ? " fence" : "") + "}";
    }
}
