package com.dcp.outlet.worker;

import com.dcp.outlet.sdk.SdkLogger;

import java.util.List;
import java.util.Map;

/**
 * 单元级运行时(一单元一份,长生命周期):上下文对象的"背后数据"。
 *
 * <p>与 {@link UnitContextImpl}/{@link InitContextImpl} 的关系:后者是每批构造的**只读视图**,
 * 持有本对象的引用 —— 逻辑拿不到 KafkaConsumer、租约等内部件。
 */
public class UnitRuntime {

    private final String unitId;
    private final String taskId;
    private final String topic;
    private final List<Integer> partitions;
    private final String consumerGroup;
    private final String logicId;
    private final String logicVersion;
    private final Map<String, Object> params;
    private final ContractProvider contractProvider;
    private final WatermarkProvider watermarks;
    private final DeadLetterSink deadLetterSink;
    private final UnitMetrics metrics;
    private final SdkLogger log;
    private volatile boolean running = true;

    public UnitRuntime(String unitId, String taskId, String topic, List<Integer> partitions,
                       String consumerGroup, String logicId, String logicVersion,
                       Map<String, Object> params, ContractProvider contractProvider,
                       WatermarkProvider watermarks, DeadLetterSink deadLetterSink,
                       UnitMetrics metrics, SdkLogger log) {
        this.unitId = unitId; this.taskId = taskId; this.topic = topic; this.partitions = partitions;
        this.consumerGroup = consumerGroup; this.logicId = logicId; this.logicVersion = logicVersion;
        this.params = params; this.contractProvider = contractProvider; this.watermarks = watermarks;
        this.deadLetterSink = deadLetterSink; this.metrics = metrics; this.log = log;
    }

    public String unitId() { return unitId; }
    public String taskId() { return taskId; }
    public String topic() { return topic; }
    public List<Integer> partitions() { return partitions; }
    public String consumerGroup() { return consumerGroup; }
    public String logicId() { return logicId; }
    public String logicVersion() { return logicVersion; }
    public Map<String, Object> params() { return params; }
    public ContractProvider contractProvider() { return contractProvider; }
    public WatermarkProvider watermarks() { return watermarks; }
    public DeadLetterSink deadLetterSink() { return deadLetterSink; }
    public UnitMetrics metrics() { return metrics; }
    public SdkLogger log() { return log; }
    public boolean running() { return running; }
    public void stop() { this.running = false; }
}
