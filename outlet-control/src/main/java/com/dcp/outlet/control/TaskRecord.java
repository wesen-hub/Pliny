package com.dcp.outlet.control;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** 消费任务(consumer_task 的领域对象)。 */
public final class TaskRecord {

    private final String taskId;
    private String name;
    private String consumerType;              // subscribe | tag | lake | custom
    private String owner;
    private final List<String> streams = new ArrayList<>();
    private String logicId;
    private String logicVersion;
    private int instances;
    private String contractRef;
    private String startPolicy = "snapshot";  // snapshot | latest
    private String desiredState = "running";
    private String incrementalEngine = "unit"; // unit(默认: v2 消费单元) | external(Flink 等外部引擎)
    private FailureSpec failure = FailureSpec.defaults();
    private final Map<String, Object> params = new LinkedHashMap<>();   // 业务参数(控制面只透传)
    private String consumerGroup;
    private int epoch = 1;
    private Long startTso;                    // 批阶段产出
    private TaskStatus status = TaskStatus.CREATED;

    public TaskRecord(String taskId) { this.taskId = taskId; }

    public String taskId() { return taskId; }
    public String name() { return name; }
    public TaskRecord name(String v) { this.name = v; return this; }
    public String consumerType() { return consumerType; }
    public TaskRecord consumerType(String v) { this.consumerType = v; return this; }
    public String owner() { return owner; }
    public TaskRecord owner(String v) { this.owner = v; return this; }
    public List<String> streams() { return streams; }
    public TaskRecord stream(String v) { this.streams.add(v); return this; }
    public String logicId() { return logicId; }
    public String logicVersion() { return logicVersion; }
    public TaskRecord logic(String id, String version) { this.logicId = id; this.logicVersion = version; return this; }
    public int instances() { return instances; }
    public TaskRecord instances(int v) { this.instances = v; return this; }
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
    public int epoch() { return epoch; }
    public TaskRecord epoch(int v) { this.epoch = v; return this; }
    public Long startTso() { return startTso; }
    public TaskRecord startTso(Long v) { this.startTso = v; return this; }
    public TaskStatus status() { return status; }
    public TaskRecord status(TaskStatus v) { this.status = v; return this; }

    // —— 非持久化的运行期视图(便于调用方一次拿到规划结果)——
    private transient List<PlannedUnit> planned = new ArrayList<>();

    public List<PlannedUnit> plannedUnits() { return planned; }

    public TaskRecord withPlanned(List<PlannedUnit> v) { this.planned = v == null ? new ArrayList<>() : v; return this; }
}
