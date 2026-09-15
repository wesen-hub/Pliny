package com.dcp.outlet.control;

/** 单分区的消费进度(前端"进度表"一行)。 */
public final class PartitionProgress {

    private final String unitId;
    private final String topic;
    private final int partition;
    private final String state;
    private final long offset;         // 已提交位点
    private final long tso;            // 已处理到的 commitTs
    private final long watermarkTs;    // 该分区水位("≤此 TSO 的事件已全部到齐")
    private final long latestOffset;   // 分区末端(用于估算未消费条数)
    private final long lagRows;        // 估算未消费条数
    private final long lagSeconds;     // 滞后秒数(水位 TSO - 已处理 TSO)
    private final long lastSeenAt;     // 最近心跳

    public PartitionProgress(String unitId, String topic, int partition, String state, long offset, long tso,
                             long watermarkTs, long latestOffset, long lagRows, long lagSeconds, long lastSeenAt) {
        this.unitId = unitId; this.topic = topic; this.partition = partition; this.state = state;
        this.offset = offset; this.tso = tso; this.watermarkTs = watermarkTs; this.latestOffset = latestOffset;
        this.lagRows = lagRows; this.lagSeconds = lagSeconds; this.lastSeenAt = lastSeenAt;
    }

    public String unitId() { return unitId; }
    public String topic() { return topic; }
    public int partition() { return partition; }
    public String state() { return state; }
    public long offset() { return offset; }
    public long tso() { return tso; }
    public long watermarkTs() { return watermarkTs; }
    public long latestOffset() { return latestOffset; }
    public long lagRows() { return lagRows; }
    public long lagSeconds() { return lagSeconds; }
    public long lastSeenAt() { return lastSeenAt; }

    @Override public String toString() {
        return "p" + partition + " off=" + offset + " tso=" + tso + " wm=" + watermarkTs
                + " lag=" + lagSeconds + "s/" + lagRows + "rows via=" + state;
    }
}
