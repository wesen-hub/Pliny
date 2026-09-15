package com.dcp.outlet.control;

import java.util.List;
import java.util.Map;

/** 下发内容(对应控制面 → 计算面的 CreateUnit 指令)。 */
public final class DispatchRequest {

    private final String reqId;
    private final String unitId;
    private final String taskId;
    private final String topic;
    private final String stream;                 // 逻辑表名(计算面据此填 batch.stream())
    private final List<Integer> partitions;
    private final Map<Integer, Long> startOffsets;
    private final String consumerGroup;
    private final String logicId;
    private final String logicVersion;
    private final Map<String, Object> params;
    private final FailureSpec failure;
    private final long leaseTtlMillis;

    public DispatchRequest(String reqId, String unitId, String taskId, String topic, String stream,
                           List<Integer> partitions, Map<Integer, Long> startOffsets, String consumerGroup,
                           String logicId, String logicVersion, Map<String, Object> params,
                           FailureSpec failure, long leaseTtlMillis) {
        this.reqId = reqId; this.unitId = unitId; this.taskId = taskId; this.topic = topic; this.stream = stream;
        this.partitions = partitions; this.startOffsets = startOffsets; this.consumerGroup = consumerGroup;
        this.logicId = logicId; this.logicVersion = logicVersion; this.params = params;
        this.failure = failure; this.leaseTtlMillis = leaseTtlMillis;
    }

    public String reqId() { return reqId; }
    public String unitId() { return unitId; }
    public String taskId() { return taskId; }
    public String topic() { return topic; }
    public String stream() { return stream; }
    public List<Integer> partitions() { return partitions; }
    public Map<Integer, Long> startOffsets() { return startOffsets; }
    public String consumerGroup() { return consumerGroup; }
    public String logicId() { return logicId; }
    public String logicVersion() { return logicVersion; }
    public Map<String, Object> params() { return params; }
    public FailureSpec failure() { return failure; }
    public long leaseTtlMillis() { return leaseTtlMillis; }
}
