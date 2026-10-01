package com.dcp.outlet.service.web;

import com.dcp.outlet.adapters.JdbcTaskStore;
import com.dcp.outlet.control.*;
import com.dcp.scheduler.model.ExecutorSnapshot;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 任务视图与控制(前端"消费任务"页对接的全部接口)。
 *
 * <pre>
 * GET    /api/outlet/tasks                  任务列表(含状态,状态由调度框架 spec/status 派生)
 * POST   /api/outlet/tasks                  建任务(通用入口;业务参数由垂直模块/调用方保证)
 * GET    /api/outlet/tasks/{id}             任务详情
 * GET    /api/outlet/tasks/{id}/progress    进度(每分区 offset → TSO → 滞后,框架快照派生)
 * POST   /api/outlet/tasks/{id}/actions/{pause|resume|stop}
 * </pre>
 *
 * <p><b>彻底版</b>:调度状态/防双消费/规划代全部由统一调度框架承担,本控制器只做业务视图聚合。
 */
@RestController
@RequestMapping("/api/outlet/tasks")
public class TasksController {

    private final TaskKernelService kernel;
    private final TaskStore tasks;
    private final StreamRegistry streams;
    private final ProgressService progress;
    private final com.dcp.outlet.adapters.JdbcDeadLetterSink dlq;
    private final com.dcp.outlet.service.schedulerbridge.OutletFrameworkRuntime frameworkRuntime;
    private final com.dcp.outlet.control.TaskScheduler scheduler;
    private final int epoch;   // 全局规划代(水位查询维度)

    public TasksController(TaskKernelService kernel, TaskStore tasks, StreamRegistry streams,
                           ProgressService progress,
                           com.dcp.outlet.adapters.JdbcDeadLetterSink dlq,
                           com.dcp.outlet.service.schedulerbridge.OutletFrameworkRuntime frameworkRuntime,
                           com.dcp.outlet.control.TaskScheduler scheduler,
                           @org.springframework.beans.factory.annotation.Value("${outlet.epoch:1}") int epoch) {
        this.kernel = kernel; this.tasks = tasks; this.streams = streams;
        this.progress = progress; this.dlq = dlq;
        this.frameworkRuntime = frameworkRuntime;
        this.scheduler = scheduler;
        this.epoch = epoch;
    }

    /** 任务列表:前端列表页用(状态由框架派生 / 逻辑版本 / 流)。 */
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
                .namespace(req.namespace())
                .logic(req.logicId(), req.logicVersion())
                .contractRef(req.contractRef()).startPolicy(req.startPolicy() == null ? "snapshot" : req.startPolicy())
                .desiredState(req.desiredState() == null ? "running" : req.desiredState())
                .incrementalEngine(req.incrementalEngine() == null ? "unit" : req.incrementalEngine())
                .params(req.params());
        req.streams().forEach(t::stream);
        TaskRecord created = kernel.create(t);
        return view(created);
    }

    /** 进度:每分区 offset → TSO → 滞后(前端详情页主视图;位点/状态由框架快照派生)。 */
    @GetMapping("/{taskId}/progress")
    public Map<String, Object> progress(@PathVariable String taskId) {
        TaskRecord t = tasks.byId(taskId);
        if (t == null) return Map.of("error", "not found: " + taskId);
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("taskId", taskId);
        out.put("status", deriveStatus(t));
        out.put("mode", "snapshot".equals(t.startPolicy()) ? "SNAPSHOT" : "INCREMENTAL");

        Map<String, Map<Integer, ExecutorSnapshot>> snaps = frameworkRuntime.partitionSnapshotsOfTask(t.namespace(), taskId);
        List<PartitionProgress> rows = new ArrayList<>();
        for (String s : t.streams()) {
            var si = streams.get(s).orElse(null);
            if (si == null) continue;
            Map<Integer, ExecutorSnapshot> byPart = snaps.getOrDefault(s, Map.of());
            List<Integer> partIds = new ArrayList<>();
            Map<Integer, Long> committed = new LinkedHashMap<>();
            Map<Integer, String> states = new LinkedHashMap<>();
            for (int p = 0; p < si.partitions(); p++) {
                partIds.add(p);
                ExecutorSnapshot snap = byPart.get(p);
                if (snap != null) {
                    committed.put(p, snap.committedOffset);
                    states.put(p, snap.stopped ? "STOPPED" : "RUNNING");
                }
            }
            rows.addAll(progress.progressOfTask(taskId, si.topic(), partIds, epoch, committed, states, false));
        }
        // 领域类方法风格访问器不可 Jackson 序列化,Web 层转 Map 视图
        out.put("progress", rows.stream().map(TasksController::progressRow).toList());
        out.put("partitionCount", rows.size());
        return out;
    }

    private static Map<String, Object> progressRow(PartitionProgress p) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("unitId", p.unitId()); m.put("topic", p.topic()); m.put("partition", p.partition());
        m.put("state", p.state()); m.put("offset", p.offset()); m.put("tso", p.tso());
        m.put("watermarkTs", p.watermarkTs()); m.put("latestOffset", p.latestOffset());
        m.put("lagRows", p.lagRows()); m.put("lagSeconds", p.lagSeconds()); m.put("lastSeenAt", p.lastSeenAt());
        return m;
    }

    /** 死信列表(前端详情页死信区块)。 */
    @GetMapping("/{taskId}/dlq")
    public List<Map<String, Object>> dlq(@PathVariable String taskId) {
        return dlq.list(taskId);
    }

    /** 标记死信已重放(重投本身 = 回退位点并重启执行器,属离线返工流程)。 */
    @PostMapping("/dlq/{dlqId}/replay")
    public Map<String, Object> replay(@PathVariable long dlqId) {
        return Map.of("dlqId", dlqId, "updated", dlq.markReplayed(dlqId));
    }

    @PostMapping("/{taskId}/actions/{action}")
    public Map<String, Object> action(@PathVariable String taskId, @PathVariable String action) {
        TaskRecord t = tasks.byId(taskId);
        if (t == null) return Map.of("error", "not found: " + taskId);
        switch (action) {
            case "pause" -> {
                t.desiredState("paused");
                frameworkRuntime.setPhase(t.namespace(), taskId, "PAUSED");   // 框架统一执行任务级暂停(追平回收)
            }
            case "resume" -> {
                t.desiredState("running");
                if (frameworkRuntime.hasTask(t.namespace(), taskId)) {
                    frameworkRuntime.setPhase(t.namespace(), taskId, null);       // spec 还在:去 PAUSED → 框架重新分配
                } else {
                    // spec 已删(stop 后 GONE):重走创建流程——latest 重新交接(框架 GONE 复活),
                    // snapshot 重新批打底;状态推进全部由框架统一完成
                    scheduler.onCreated(t);
                }
            }
            case "stop" -> {
                t.desiredState("stopped");
                frameworkRuntime.cancelTask(t.namespace(), taskId);           // 删 spec → 框架推进 DRAINING → GONE
            }
            default -> { return Map.of("error", "unknown action: " + action); }
        }
        tasks.update(t);
        return Map.of("taskId", taskId, "desiredState", t.desiredState());
    }

    /** 任务状态派生:框架 spec.phase / 框架任务状态 / 批失败事件 → 业务状态视图。 */
    private String deriveStatus(TaskRecord t) {
        String phase = frameworkRuntime.phaseOf(t.namespace(), t.taskId());
        String state = frameworkRuntime.stateOf(t.namespace(), t.taskId());
        if ("BATCHING".equals(phase)) {
            return frameworkRuntime.batchFailed(t.namespace(), t.taskId()) ? "BATCH_FAILED" : "BATCH_RUNNING";
        }
        if ("PAUSED".equals(phase)) return "PAUSED";
        if (state == null) {
            return "stopped".equals(t.desiredState()) ? "STOPPED" : "SCHEDULED";
        }
        return switch (state) {
            case "SUBMITTED" -> "SCHEDULED";
            case "RUNNING" -> "RUNNING";
            case "DRAINING" -> "STOPPING";
            case "GONE" -> "STOPPED";
            default -> state;
        };
    }

    private Map<String, Object> view(TaskRecord t) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("taskId", t.taskId());
        m.put("name", t.name());
        m.put("consumerType", t.consumerType());
        m.put("namespace", t.namespace());
        m.put("streams", t.streams());
        m.put("logic", t.logicId() + "@" + t.logicVersion());
        m.put("contractRef", t.contractRef());
        m.put("startPolicy", t.startPolicy());
        m.put("desiredState", t.desiredState());
        m.put("consumerGroup", t.consumerGroup());
        m.put("status", deriveStatus(t));
        m.put("params", t.params());
        return m;
    }

    /** 建任务请求(通用参数 + 业务参数;业务参数形状由调用方/垂直模块保证)。
     *  {@code namespace}: 调度框架 ns(消费底座多 ns 拆分: tag/lake/sub/ingest;缺省 outlet 兼容)。 */
    public record CreateTaskRequest(String taskId, String name, String consumerType, String owner,
                                    List<String> streams, String logicId, String logicVersion,
                                    String contractRef, String startPolicy,
                                    String desiredState, String incrementalEngine,
                                    String namespace, Map<String, Object> params) {
    }
}
