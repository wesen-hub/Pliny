package com.dcp.outlet.control;

import com.dcp.outlet.sdk.LogicInitContext;
import com.dcp.outlet.sdk.SdkLogger;

import java.util.List;
import java.util.Map;

/** 批阶段的初始化上下文(批运行器构造;单元启动时的对应物在计算面)。 */
public class BatchInitContext implements LogicInitContext {

    private final String taskId;
    private final String logicVersion;
    private final String topic;
    private final Map<String, Object> params;
    private final SdkLogger log;

    public BatchInitContext(String taskId, String logicVersion, String topic,
                            Map<String, Object> params, SdkLogger log) {
        this.taskId = taskId; this.logicVersion = logicVersion; this.topic = topic;
        this.params = params; this.log = log;
    }

    @Override public Map<String, Object> params() { return params; }
    @Override public String taskId() { return taskId; }
    @Override public String logicVersion() { return logicVersion; }
    @Override public String topic() { return topic; }
    @Override public List<Integer> partitions() { return List.of(); }   // 批阶段不绑定分区
    @Override public SdkLogger log() { return log; }
}
