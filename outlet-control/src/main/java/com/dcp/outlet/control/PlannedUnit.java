package com.dcp.outlet.control;

import java.util.List;

/** 规划出的消费单元:一个单元只服务一个流(不跨 topic),持有 1..N 个分区。 */
public final class PlannedUnit {

    private final String unitId;
    private final String stream;
    private final String topic;
    private final List<Integer> partitions;

    public PlannedUnit(String unitId, String stream, String topic, List<Integer> partitions) {
        this.unitId = unitId; this.stream = stream; this.topic = topic; this.partitions = partitions;
    }

    public String unitId() { return unitId; }
    public String stream() { return stream; }
    public String topic() { return topic; }
    public List<Integer> partitions() { return partitions; }

    @Override public String toString() { return unitId + "[" + stream + " p" + partitions + "]"; }
}
