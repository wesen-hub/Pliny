package com.dcp.outlet.control;

/** 已登记的流:逻辑表名 → 物理 topic + 分区数。 */
public final class StreamInfo {

    private final String stream;       // 逻辑表名(rt_sync_c)
    private final String topic;        // dcp.cdc.target_db.rt_sync_c
    private final int partitions;

    public StreamInfo(String stream, String topic, int partitions) {
        this.stream = stream; this.topic = topic; this.partitions = partitions;
    }

    public String stream() { return stream; }
    public String topic() { return topic; }
    public int partitions() { return partitions; }
}
