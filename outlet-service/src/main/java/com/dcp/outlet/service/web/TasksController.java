package com.dcp.outlet.service.web;

import com.dcp.outlet.adapters.JdbcTaskStore;
import com.dcp.outlet.control.*;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 任务视图与控制(前端"消费任务"页对接的全部接口)。
 *
 * <pre>
 * GET    /api/outlet/tasks                  任务列表(含状态)
 * POST   /api/outlet/tasks                  建任务(通用入口;业务参数由垂直模块/调用方保证)
 * GET    /api/outlet/tasks/{id}             任务详情
 * GET    /api/outlet/tasks/{id}/units       消费单元列表(单元=调度原子)
 * GET    /api/outlet/tasks/{id}/progress    进度(每分区 offset → TSO → 滞后)
 * POST   /api/outlet/tasks/{id}/actions/{pause|resume|stop}
 * </pre>
 */
@RestController
@RequestMapping("/api/outlet/tasks")
public class TasksController {

    private final TaskKernelService kernel;
    private final TaskStore tasks;
    private final UnitStore units;
    private final UnitStatusStore statuses;
    private final ProgressService progress;
    private final com.dcp.outlet.adapters.JdbcDeadLetterSink dlq;

    public TasksController(TaskKernelService kernel, TaskStore tasks, UnitStore units,
                           UnitStatusStore statuses, ProgressService progress,
                           com.dcp.outlet.adapters.JdbcDeadLetterSink dlq) {
        this.kernel = kernel; this.tasks = tasks; this.units = units;
        this.statuses = statuses; this.progress = progress; this.dlq = dlq;
    }

    /** 任务列表:前端列表页用(status / 单元数 / 逻辑版本 / 起止 TSO)。 */
    @GetMapping
    public List<Map<String, Object>> list() {
        List<Map<String, Object>> out = new ArrayList<>();
        if (tasks instanceof JdbcTaskStore jdbc) {
            for (String id : jdbc.allTaskIds()) out.add(view(jdbc.byId(id)));
        }
        return out;
    }

    @GetMapping("/{taskId}")
    public Map<String, Object> get(@PathVariable String taskId) {
        TaskRecord t = tasks.byId(taskId);
        return t == null ? Map.of("error", "not found: " + taskId) : view(t);
    }

    @PostMapping
    public Map<String, Object> create(@RequestBody CreateTaskRequest req) {
        TaskRecord t = new TaskRecord(req.taskId())
                .name(req.name()).consumerType(req.consumerType()).owner(req.owner())
                .logic(req.logicId(), req.logicVersion()).instances(req.instances())
                .contractRef(req.contractRef()).startPolicy(req.startPolicy() == null ? "snapshot" : req.startPolicy())
                .desiredState(req.desiredState() == null ? "running" : req.desiredState())
                .params(req.params());
        req.streams().forEach(t::stream);
        TaskRecord created = kernel.create(t);
        return view(created);
    }

    /** 进度:每分区 offset → TSO → 滞后(前端详情页主视图)。 */
    @GetMapping("/{taskId}/progress")
    public Map<String, Object> progress(@PathVariable String taskId) {
        TaskRecord t = tasks.byId(taskId);
        if (t == null) return Map.of("error", "not found: " + taskId);
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("taskId", taskId);
        out.put("status", t.status().name());
        out.put("startTso", t.startTso());
        out.put("mode", t.startTso() == null ? "SNAPSHOT" : "INCREMENTAL");
        List<Map<String, Object>> unitViews = new ArrayList<>();
        for (PlannedUnit u : units.byTask(taskId)) {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("unitId", u.unitId());
            m.put("stream", u.stream());
            m.put("topic", u.topic());
            m.put("partitions", u.partitions());
            m.put("state", units.stateOf(u.unitId()));
            unitViews.add(m);
        }
        out.put("units", unitViews);
        out.put("unitCount", unitViews.size());
        // 每分区滞后(offset → TSO → 滞后秒/条);queryKafka=false 避免每次翻页都打 Kafka
        List<PartitionProgress> rows = progress.progressOfTask(taskId, units.byTask(taskId),
                statuses.byTask(taskId), t.epoch(), false);
        out.put("progress", rows);
        return out;
    }

    /** 死信列表(前端详情页死信区块)。 */
    @GetMapping("/{taskId}/dlq")
    public List<Map<String, Object>> dlq(@PathVariable String taskId) {
        return dlq.list(taskId);
    }

    /** 标记死信已重放(重投本身 = 回退位点并重启单元,属离线返工流程)。 */
    @PostMapping("/dlq/{dlqId}/replay")
    public Map<String, Object> replay(@PathVariable long dlqId) {
        return Map.of("dlqId", dlqId, "updated", dlq.markReplayed(dlqId));
    }

    @GetMapping("/{taskId}/units")
    public List<PlannedUnit> units(@PathVariable String taskId) { return units.byTask(taskId); }

    @PostMapping("/{taskId}/actions/{action}")
    public Map<String, Object> action(@PathVariable String taskId, @PathVariable String action) {
        TaskRecord t = tasks.byId(taskId);
        if (t == null) return Map.of("error", "not found: " + taskId);
        switch (action) {
            case "pause" -> t.desiredState("paused");
            case "resume" -> t.desiredState("running");
            case "stop" -> t.desiredState("stopped");
            default -> { return Map.of("error", "unknown action: " + action); }
        }
        tasks.update(t);
        return Map.of("taskId", taskId, "desiredState", t.desiredState());
    }

    private Map<String, Object> view(TaskRecord t) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("taskId", t.taskId());
        m.put("name", t.name());
        m.put("consumerType", t.consumerType());
        m.put("streams", t.streams());
        m.put("logic", t.logicId() + "@" + t.logicVersion());
        m.put("instances", t.instances());
        m.put("contractRef", t.contractRef());
        m.put("startPolicy", t.startPolicy());
        m.put("desiredState", t.desiredState());
        m.put("consumerGroup", t.consumerGroup());
        m.put("epoch", t.epoch());
        m.put("startTso", t.startTso());
        m.put("status", t.status().name());
        m.put("params", t.params());
        return m;
    }

    /** 建任务请求(通用参数 + 业务参数;业务参数形状由调用方/垂直模块保证)。 */
    public record CreateTaskRequest(String taskId, String name, String consumerType, String owner,
                                    List<String> streams, String logicId, String logicVersion,
                                    int instances, String contractRef, String startPolicy,
                                    String desiredState, Map<String, Object> params) {
    }
}
