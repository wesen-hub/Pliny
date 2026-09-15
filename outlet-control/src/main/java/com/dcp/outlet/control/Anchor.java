package com.dcp.outlet.control;

/**
 * 稀疏锚点:{@code (topic, partition, epoch, offset) → commitTs}。
 *
 * <p><b>锚点是加速器,不是真相源</b> —— 可随时重建;损坏时退化为"时间索引粗定位 + 短扫"。
 */
public final class Anchor {

    private final String topic;
    private final int partition;
    private final int epoch;
    private final long offset;
    private final long commitTs;
    private final long wallTime;

    public Anchor(String topic, int partition, int epoch, long offset, long commitTs, long wallTime) {
        this.topic = topic;
        this.partition = partition;
        this.epoch = epoch;
        this.offset = offset;
        this.commitTs = commitTs;
        this.wallTime = wallTime;
    }

    public String topic() { return topic; }

    public int partition() { return partition; }

    public int epoch() { return epoch; }

    public long offset() { return offset; }

    public long commitTs() { return commitTs; }

    public long wallTime() { return wallTime; }

    @Override public String toString() {
        return "Anchor{" + topic + "-" + partition + " e" + epoch
                + " offset=" + offset + " commitTs=" + commitTs + "}";
    }
}
