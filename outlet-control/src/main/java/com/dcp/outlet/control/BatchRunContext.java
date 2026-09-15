package com.dcp.outlet.control;

import com.dcp.outlet.sdk.ConsumerContext;
import com.dcp.outlet.sdk.Mode;
import com.dcp.outlet.sdk.SdkLogger;

import java.util.Map;

/** 批阶段上下文:{@code mode=SNAPSHOT}、{@code safeWatermark=T0}(批视图的水位就是 T0)。 */
public class BatchRunContext implements ConsumerContext {

    private final String taskId;
    private final String consumerGroup;
    private final long t0;
    private final Map<String, Object> params;
    private final String logicId;
    private final String logicVersion;
    private final SdkLogger log;

    public BatchRunContext(String taskId, String consumerGroup, long t0, Map<String, Object> params,
                           String logicId, String logicVersion, SdkLogger log) {
        this.taskId = taskId; this.consumerGroup = consumerGroup; this.t0 = t0;
        this.params = params; this.logicId = logicId; this.logicVersion = logicVersion; this.log = log;
    }

    @Override public String taskId() { return taskId; }
    @Override public String consumerGroup() { return consumerGroup; }
    @Override public Mode mode() { return Mode.SNAPSHOT; }
    @Override public int partition() { return -1; }
    @Override public long safeWatermark() { return t0; }
    @Override public boolean caughtUp() { return true; }              // 批是一次性全量,无"追赶"概念
    @Override public Map<String, Object> params() { return params; }
    @Override public String logicId() { return logicId; }
    @Override public String logicVersion() { return logicVersion; }
    @Override public SdkLogger log() { return log; }
    @Override public void commit() { /* 批阶段不提交位点:它不碰 Kafka */ }
}
