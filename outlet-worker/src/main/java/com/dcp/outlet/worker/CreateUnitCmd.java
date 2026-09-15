package com.dcp.outlet.worker;

import com.dcp.outlet.sdk.LogicRef;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 控制面 → 计算面的创建单元指令(单单元粒度)。
 *
 * <p>控制面下发的是 {@code CreateUnit},**不是"起容器"** —— 单元可单独创建/停止/迁移(D9)。
 */
public final class CreateUnitCmd {

    private String reqId;
    private String unitId;
    private String taskId;
    private String topic;
    private String stream;          // 逻辑表名(batch.stream() 与契约白名单的键)
    private String contractRef;     // 契约引用:计算面据此从控制面拉契约视图(**缺 = 拒绝消费**)
    private List<Integer> partitions = List.of();
    private Map<Integer, Long> startOffsets = new LinkedHashMap<>();
    private String consumerGroup;
    private LogicRef logic;
    private Map<String, Object> params = new LinkedHashMap<>();
    private FailurePolicy failure = FailurePolicy.defaults();
    private long pollMillis = 200L;
    private int leaseTtlSeconds = 30;

    public String reqId() { return reqId; }
    public CreateUnitCmd reqId(String v) { this.reqId = v; return this; }
    public String unitId() { return unitId; }
    public CreateUnitCmd unitId(String v) { this.unitId = v; return this; }
    public String taskId() { return taskId; }
    public CreateUnitCmd taskId(String v) { this.taskId = v; return this; }
    public String topic() { return topic; }
    public CreateUnitCmd topic(String v) { this.topic = v; return this; }
    public String stream() { return stream; }
    public CreateUnitCmd stream(String v) { this.stream = v; return this; }
    public String contractRef() { return contractRef; }
    public CreateUnitCmd contractRef(String v) { this.contractRef = v; return this; }
    public List<Integer> partitions() { return partitions; }
    public CreateUnitCmd partitions(List<Integer> v) { this.partitions = v; return this; }
    public Map<Integer, Long> startOffsets() { return startOffsets; }
    public CreateUnitCmd startOffsets(Map<Integer, Long> v) { this.startOffsets = v; return this; }
    public String consumerGroup() { return consumerGroup; }
    public CreateUnitCmd consumerGroup(String v) { this.consumerGroup = v; return this; }
    public LogicRef logic() { return logic; }
    public CreateUnitCmd logic(LogicRef v) { this.logic = v; return this; }
    public Map<String, Object> params() { return params; }
    public CreateUnitCmd params(Map<String, Object> v) { this.params = v; return this; }
    public FailurePolicy failure() { return failure; }
    public CreateUnitCmd failure(FailurePolicy v) { this.failure = v; return this; }
    public long pollMillis() { return pollMillis; }
    public CreateUnitCmd pollMillis(long v) { this.pollMillis = v; return this; }
    public int leaseTtlSeconds() { return leaseTtlSeconds; }
    public CreateUnitCmd leaseTtlSeconds(int v) { this.leaseTtlSeconds = v; return this; }
}
