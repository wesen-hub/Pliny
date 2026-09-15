package com.dcp.outlet.control;

/** 单元租约:同一 (topic, partition, epoch) 同时只允许一个单元持有 —— 防双消费。 */
public final class LeaseGrant {

    private final String unitId;
    private final String taskId;
    private final String topic;
    private final int partition;
    private final int epoch;
    private long expireAt;

    public LeaseGrant(String unitId, String taskId, String topic, int partition, int epoch, long expireAt) {
        this.unitId = unitId; this.taskId = taskId; this.topic = topic;
        this.partition = partition; this.epoch = epoch; this.expireAt = expireAt;
    }

    public String unitId() { return unitId; }
    public String taskId() { return taskId; }
    public String topic() { return topic; }
    public int partition() { return partition; }
    public int epoch() { return epoch; }
    public long expireAt() { return expireAt; }

    void expireAt(long v) { this.expireAt = v; }
}
