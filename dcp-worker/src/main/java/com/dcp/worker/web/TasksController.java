package com.dcp.worker.web;

import com.dcp.scheduler.model.ExecutorSnapshot;
import com.dcp.scheduler.model.TaskSpec;
import com.dcp.scheduler.model.TaskStatus;
import com.dcp.scheduler.store.EtcdStore;
import com.dcp.scheduler.store.Json;
import com.dcp.scheduler.store.KeySpace;
import com.dcp.worker.WorkerRuntime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 任务提交与状态读入口(方案 §9):控制面(backend/outlet-service/tagengine-service)
 * 不再直连 etcd,统一 REST 到 dcp-worker。
 *
 * <pre>
 *   写路径:
 *     POST   /tasks                              提交/覆盖 TaskSpec(幂等 put)
 *     DELETE /tasks/{ns}/{taskId}                注销 spec(框架推进 DRAINING → GONE)
 *     PUT    /tasks/{ns}/{taskId}/events/{name}  写任务事件(如 batch-failed 告警)
 *     DELETE /tasks/{ns}/{taskId}/events/{name}  删任务事件(如扩容建议消费后清理)
 *     DELETE /tasks/{ns}/{taskId}/status         删状态(一次性任务终态重启: 覆盖 spec 后删 status → SUBMITTED 重新上线)
 *   读路径(控制面探针/巡检;方案 11:etcd 仅 dcp-worker 依赖):
 *     GET    /tasks/{ns}                         该 ns 全部 taskId(spec 键解析)
 *     GET    /tasks/{ns}/events/{name}           全部任务该事件 [{taskId, event}]
 *     GET    /tasks/{ns}/{taskId}/spec           TaskSpec(无 = 404)
 *     GET    /tasks/{ns}/{taskId}/status         TaskStatus(无 = 404)
 *     GET    /tasks/{ns}/{taskId}/events/{name}  事件(无 = 404)
 *     GET    /tasks/{ns}/{taskId}/snaps          分区快照 {partId: ExecutorSnapshot}
 *     GET    /health                             健康检查(骨架状态)
 * </pre>
 */
@RestController
@RequestMapping
public class TasksController {

    private static final Logger log = LoggerFactory.getLogger(TasksController.class);

    private final WorkerRuntime runtime;

    public TasksController(WorkerRuntime runtime) {
        this.runtime = runtime;
    }

    // ── 写路径 ──

    @PostMapping("/tasks")
    public ResponseEntity<Map<String, Object>> submit(@RequestBody TaskSpec spec) {
        if (spec.namespace == null || spec.namespace.isBlank()
                || spec.taskId == null || spec.taskId.isBlank()) {
            return ResponseEntity.badRequest().body(Map.of(
                    "ok", false, "error", "namespace/taskId 必填"));
        }
        EtcdStore store = runtime.store();
        if (store == null) {
            return ResponseEntity.status(503).body(Map.of("ok", false, "error", "骨架未就绪"));
        }
        store.put(KeySpace.spec(spec.namespace, spec.taskId), spec);
        log.info("[tasks] 提交 TaskSpec: ns={} task={} topic={} partitions={}",
                spec.namespace, spec.taskId, spec.topic, spec.partitions);
        return ResponseEntity.ok(Map.of("ok", true));
    }

    @DeleteMapping("/tasks/{ns}/{taskId}")
    public ResponseEntity<Map<String, Object>> cancel(@PathVariable String ns,
                                                      @PathVariable String taskId) {
        EtcdStore store = runtime.store();
        if (store == null) {
            return ResponseEntity.status(503).body(Map.of("ok", false, "error", "骨架未就绪"));
        }
        store.delete(KeySpace.spec(ns, taskId));
        log.info("[tasks] 注销 TaskSpec: ns={} task={}", ns, taskId);
        return ResponseEntity.ok(Map.of("ok", true));
    }

    @PutMapping("/tasks/{ns}/{taskId}/events/{name}")
    public ResponseEntity<Map<String, Object>> putEvent(@PathVariable String ns,
                                                        @PathVariable String taskId,
                                                        @PathVariable String name,
                                                        @RequestBody Map<String, Object> event) {
        EtcdStore store = runtime.store();
        if (store == null) {
            return ResponseEntity.status(503).body(Map.of("ok", false, "error", "骨架未就绪"));
        }
        store.put(KeySpace.taskEvent(ns, taskId, name), event);
        log.info("[tasks] 写任务事件: ns={} task={} event={}", ns, taskId, name);
        return ResponseEntity.ok(Map.of("ok", true));
    }

    @DeleteMapping("/tasks/{ns}/{taskId}/events/{name}")
    public ResponseEntity<Map<String, Object>> deleteEvent(@PathVariable String ns,
                                                           @PathVariable String taskId,
                                                           @PathVariable String name) {
        EtcdStore store = runtime.store();
        if (store == null) {
            return ResponseEntity.status(503).body(Map.of("ok", false, "error", "骨架未就绪"));
        }
        store.delete(KeySpace.taskEvent(ns, taskId, name));
        log.info("[tasks] 删任务事件: ns={} task={} event={}", ns, taskId, name);
        return ResponseEntity.ok(Map.of("ok", true));
    }

    /** 删状态(一次性任务终态重启语义: 终态挂起 → 删 status → 框架按 SUBMITTED 重新上线)。 */
    @DeleteMapping("/tasks/{ns}/{taskId}/status")
    public ResponseEntity<Map<String, Object>> deleteStatus(@PathVariable String ns,
                                                            @PathVariable String taskId) {
        EtcdStore store = runtime.store();
        if (store == null) {
            return ResponseEntity.status(503).body(Map.of("ok", false, "error", "骨架未就绪"));
        }
        store.delete(KeySpace.status(ns, taskId));
        log.info("[tasks] 删任务状态(终态重启): ns={} task={}", ns, taskId);
        return ResponseEntity.ok(Map.of("ok", true));
    }

    // ── 读路径 ──

    /** 该 ns 全部 taskId(按 spec 键解析,按字典序稳定)。 */
    @GetMapping("/tasks/{ns}")
    public ResponseEntity<Map<String, Object>> listTasks(@PathVariable String ns) {
        EtcdStore store = runtime.store();
        if (store == null) {
            return ResponseEntity.status(503).body(Map.of("ok", false, "error", "骨架未就绪"));
        }
        List<String> taskIds = new ArrayList<>();
        for (String key : store.getPrefix(KeySpace.tasksPrefix(ns)).keySet()) {
            if (key.endsWith("/spec")) {
                taskIds.add(KeySpace.taskIdOfSpecKey(key));
            }
        }
        taskIds.sort(String::compareTo);
        return ResponseEntity.ok(Map.of("ok", true, "taskIds", taskIds));
    }

    /** 全部任务的事件(name 同名,滚动覆盖键):backend 扩容建议消费等观察面。 */
    @GetMapping("/tasks/{ns}/events/{name}")
    public ResponseEntity<Map<String, Object>> listEvents(@PathVariable String ns,
                                                          @PathVariable String name) {
        EtcdStore store = runtime.store();
        if (store == null) {
            return ResponseEntity.status(503).body(Map.of("ok", false, "error", "骨架未就绪"));
        }
        List<Map<String, Object>> out = new ArrayList<>();
        for (Map.Entry<String, byte[]> e : store.getPrefix(KeySpace.tasksPrefix(ns)).entrySet()) {
            if (!e.getKey().endsWith("/events/" + name)) {
                continue;
            }
            String taskId = taskIdOfEventKey(e.getKey(), name);
            Map<String, Object> item = new LinkedHashMap<>();
            item.put("taskId", taskId);
            item.put("event", Json.fromBytes(e.getValue(), Map.class));
            out.add(item);
        }
        return ResponseEntity.ok(Map.of("ok", true, "events", out));
    }

    @GetMapping("/tasks/{ns}/{taskId}/spec")
    public ResponseEntity<Map<String, Object>> getSpec(@PathVariable String ns,
                                                       @PathVariable String taskId) {
        EtcdStore store = runtime.store();
        if (store == null) {
            return ResponseEntity.status(503).body(Map.of("ok", false, "error", "骨架未就绪"));
        }
        return store.getObj(KeySpace.spec(ns, taskId), TaskSpec.class)
                .<ResponseEntity<Map<String, Object>>>map(
                        spec -> ResponseEntity.ok(Map.of("ok", true, "spec", spec)))
                .orElseGet(() -> ResponseEntity.status(404).body(Map.of("ok", false, "error", "spec 不存在")));
    }

    @GetMapping("/tasks/{ns}/{taskId}/status")
    public ResponseEntity<Map<String, Object>> getStatus(@PathVariable String ns,
                                                         @PathVariable String taskId) {
        EtcdStore store = runtime.store();
        if (store == null) {
            return ResponseEntity.status(503).body(Map.of("ok", false, "error", "骨架未就绪"));
        }
        return store.getObj(KeySpace.status(ns, taskId), TaskStatus.class)
                .<ResponseEntity<Map<String, Object>>>map(
                        s -> ResponseEntity.ok(Map.of("ok", true, "status", s)))
                .orElseGet(() -> ResponseEntity.status(404).body(Map.of("ok", false, "error", "status 不存在")));
    }

    @GetMapping("/tasks/{ns}/{taskId}/events/{name}")
    public ResponseEntity<Map<String, Object>> getEvent(@PathVariable String ns,
                                                        @PathVariable String taskId,
                                                        @PathVariable String name) {
        EtcdStore store = runtime.store();
        if (store == null) {
            return ResponseEntity.status(503).body(Map.of("ok", false, "error", "骨架未就绪"));
        }
        return store.getObj(KeySpace.taskEvent(ns, taskId, name), Map.class)
                .<ResponseEntity<Map<String, Object>>>map(
                        ev -> ResponseEntity.ok(Map.of("ok", true, "event", ev)))
                .orElseGet(() -> ResponseEntity.status(404).body(Map.of("ok", false, "error", "事件不存在")));
    }

    /** 分区快照(进度页数据源):parts/{p}/snap 键 → {partId: ExecutorSnapshot}。 */
    @GetMapping("/tasks/{ns}/{taskId}/snaps")
    public ResponseEntity<Map<String, Object>> listSnaps(@PathVariable String ns,
                                                         @PathVariable String taskId) {
        EtcdStore store = runtime.store();
        if (store == null) {
            return ResponseEntity.status(503).body(Map.of("ok", false, "error", "骨架未就绪"));
        }
        Map<Integer, ExecutorSnapshot> snaps = new LinkedHashMap<>();
        for (Map.Entry<String, byte[]> e : store.getPrefix(KeySpace.partsPrefix(ns, taskId)).entrySet()) {
            if (!e.getKey().endsWith("/snap")) {
                continue;
            }
            String partKey = e.getKey();
            String partIdStr = partKey.substring(partKey.indexOf("/parts/") + 7,
                    partKey.indexOf("/snap"));
            snaps.put(Integer.parseInt(partIdStr), Json.fromBytes(e.getValue(), ExecutorSnapshot.class));
        }
        return ResponseEntity.ok(Map.of("ok", true, "snaps", snaps));
    }

    @GetMapping("/health")
    public ResponseEntity<Map<String, Object>> health() {
        return ResponseEntity.ok(Map.of(
                "ok", true,
                "skeleton", runtime.isEnabled(),
                "etcd", runtime.store() != null));
    }

    /** 从 .../tasks/{taskId}/events/{name} 反解 taskId。 */
    private static String taskIdOfEventKey(String key, String name) {
        int start = key.indexOf("/tasks/") + 7;
        int end = key.indexOf("/events/" + name);
        return key.substring(start, end);
    }
}
