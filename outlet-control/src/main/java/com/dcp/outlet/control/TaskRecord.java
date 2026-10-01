package com.dcp.outlet.control;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 消费任务(consumer_task 的领域对象)。
 *
 * <p><b>纯业务登记</b>(彻底版):调度状态(批阶段/运行/暂停/注销)、规划代(epoch)、批起点(T0)、
 * 并发(instances)全部由统一调度框架承担(spec.phase / 框架任务状态 / 框架自动扩缩),
 * 元库只保留"这个任务是谁、消费什么、按什么契约与逻辑"的业务事实。
 */
public final class TaskRecord {

    private final String taskId;
    private String name;
    private String consumerType;              // subscribe | tag | lake | custom
    private String namespace = "outlet";      // 调度框架 ns(消费底座多 ns 拆分: tag/lake/sub/ingest; 缺省 outlet 兼容)
    private String owner;
    private final List<String> streams = new ArrayList<>();
    private String logicId;
    private String logicVersion;
    private String contractRef;
    private String startPolicy = "snapshot";  // snapshot | latest
    private String desiredState = "running";  // 业务意图:期望运行/暂停/停止(与框架 phase 解耦)
    private String incrementalEngine = "unit"; // unit(v2 消费单元,框架接管) | external(Flink 等外部引擎)
    private FailureSpec failure = FailureSpec.defaults();
    private final Map<String, Object> params = new LinkedHashMap<>();   // 业务参数(控制面只透传)
    private String consumerGroup;

    public TaskRecord(String taskId) { this.taskId = taskId; }

    public String taskId() { return taskId; }
    public String name() { return name; }
    public TaskRecord name(String v) { this.name = v; return this; }
    public String consumerType() { return consumerType; }
    public TaskRecord consumerType(String v) { this.consumerType = v; return this; }
    /** 调度框架 ns(null/空白 → outlet 兼容旧调用; 与 Kafka 地址解耦, bootstrap 随指令透传) */
    public String namespace() { return namespace == null || namespace.isBlank() ? "outlet" : namespace; }
    public TaskRecord namespace(String v) { if (v != null && !v.isBlank()) this.namespace = v; return this; }
    public String owner() { return owner; }
    public TaskRecord owner(String v) { this.owner = v; return this; }
    public List<String> streams() { return streams; }
    public TaskRecord stream(String v) { this.streams.add(v); return this; }
    public String logicId() { return logicId; }
    public String logicVersion() { return logicVersion; }
    public TaskRecord logic(String id, String version) { this.logicId = id; this.logicVersion = version; return this; }
    public String contractRef() { return contractRef; }
    public TaskRecord contractRef(String v) { this.contractRef = v; return this; }
    public String startPolicy() { return startPolicy; }
    public TaskRecord startPolicy(String v) { this.startPolicy = v; return this; }
    public String desiredState() { return desiredState; }
    public TaskRecord desiredState(String v) { this.desiredState = v; return this; }
    public String incrementalEngine() { return incrementalEngine; }
    public TaskRecord incrementalEngine(String v) { this.incrementalEngine = v; return this; }
    public FailureSpec failure() { return failure; }
    public TaskRecord failure(FailureSpec v) { this.failure = v; return this; }
    public Map<String, Object> params() { return params; }
    public TaskRecord params(Map<String, Object> v) { if (v != null) params.putAll(v); return this; }
    public String consumerGroup() { return consumerGroup; }
    public TaskRecord consumerGroup(String v) { this.consumerGroup = v; return this; }
}
