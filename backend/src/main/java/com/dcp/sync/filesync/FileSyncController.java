package com.dcp.sync.filesync;

import org.springframework.security.access.prepost.PreAuthorize;

import com.dcp.common.ApiResponse;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

/**
 * 文件同步 V3 API(rclone 引擎, 取代 V2 /api/filesync/copy/*):
 *
 * <pre>
 *   GET    /api/filesync/tasks                     列表
 *   POST   /api/filesync/tasks                     创建(任务定义 JSON, 含 syncMode 三态)
 *   GET    /api/filesync/tasks/{id}                详情
 *   PUT    /api/filesync/tasks/{id}                更新(version 乐观锁)
 *   DELETE /api/filesync/tasks/{id}                删除(agent 任务附带 task_remove 指令)
 *   POST   /api/filesync/tasks/{id}/run            手动触发一轮(异步; agent 任务下发 run_now)
 *   POST   /api/filesync/tasks/{id}/stop           停止(仅 agent 任务: 下发 task_stop)
 *   GET    /api/filesync/tasks/{id}/runs           run 分页(传输历史)
 *   GET    /api/filesync/tasks/{id}/runs/{runId}/files  run 文件明细分页
 *   GET    /api/filesync/tasks/{id}/browse         目标桶浏览(rclone lsjson)
 *   POST   /api/filesync/tasks/{id}/reconcile      一致性对账(只报告不修改)
 *   GET    /api/filesync/tasks/{id}/reconciles     对账历史
 * </pre>
 */
@RestController
@RequestMapping("/api/filesync/tasks")
public class FileSyncController {

    private final FileSyncTaskService service;

    public FileSyncController(FileSyncTaskService service) {
        this.service = service;
    }

    @GetMapping
    public ApiResponse<List<Map<String, Object>>> list() {
        return ApiResponse.ok(service.list());
    }

    @PostMapping
    @PreAuthorize("@perm.has('sync:task:write')")
    public ApiResponse<Map<String, Object>> create(@RequestBody Map<String, Object> body) {
        try {
            return ApiResponse.ok(service.create(body));
        } catch (IllegalArgumentException e) {
            return ApiResponse.error(400, e.getMessage());
        }
    }

    @GetMapping("/{id}")
    public ApiResponse<Map<String, Object>> detail(@PathVariable long id) {
        Map<String, Object> d = service.detail(id);
        return d == null ? ApiResponse.error(404, "任务不存在: " + id) : ApiResponse.ok(d);
    }

    @PutMapping("/{id}")
    @PreAuthorize("@perm.has('sync:task:write')")
    public ApiResponse<Map<String, Object>> update(@PathVariable long id, @RequestBody Map<String, Object> body) {
        try {
            return ApiResponse.ok(service.update(id, body));
        } catch (IllegalArgumentException e) {
            return ApiResponse.error(400, e.getMessage());
        }
    }

    @DeleteMapping("/{id}")
    @PreAuthorize("@perm.has('sync:task:write')")
    public ApiResponse<Map<String, Object>> delete(@PathVariable long id) {
        service.delete(id);
        return ApiResponse.ok(Map.of("deleted", true, "id", id));
    }

    @PostMapping("/{id}/run")
    @PreAuthorize("@perm.has('sync:task:run')")
    public ApiResponse<Map<String, Object>> run(@PathVariable long id) {
        Map<String, Object> d = service.detail(id);
        if (d == null) {
            return ApiResponse.error(404, "任务不存在: " + id);
        }
        boolean started = service.runAsync(id);
        if (!started) {
            return ApiResponse.error(400, "任务正在执行中, 请稍后再试");
        }
        boolean agentTask = "agent".equals(String.valueOf(d.get("exec_host")));
        Map<String, Object> out = new java.util.LinkedHashMap<>();   // Map.of 不容 null(平台任务 online=null)
        out.put("started", true);
        out.put("id", id);
        out.put("execHost", d.get("exec_host"));
        out.put("online", agentTask ? Boolean.TRUE.equals(d.get("online")) : null);
        return ApiResponse.ok(out);
    }

    /** 停止: rclone 无常驻会话, 仅 agent 任务可下发 task_stop(否则无可停对象) */
    @PostMapping("/{id}/stop")
    @PreAuthorize("@perm.has('sync:task:run')")
    public ApiResponse<Map<String, Object>> stop(@PathVariable long id) {
        Map<String, Object> d = service.detail(id);
        if (d == null) {
            return ApiResponse.error(404, "任务不存在: " + id);
        }
        if (!"agent".equals(String.valueOf(d.get("exec_host")))) {
            return ApiResponse.error(400, "平台任务为一次性执行(rclone 无常驻会话), 无可停对象");
        }
        boolean stopped = service.stopAgentWatch(id);
        return ApiResponse.ok(Map.of("stopped", stopped, "id", id, "dispatchedToAgent", stopped));
    }

    @GetMapping("/{id}/runs")
    public ApiResponse<Map<String, Object>> runs(@PathVariable long id,
                                                 @RequestParam(defaultValue = "1") int page,
                                                 @RequestParam(defaultValue = "20") int pageSize) {
        return ApiResponse.ok(service.runsPage(id, page, pageSize));
    }

    @GetMapping("/{id}/runs/{runId}/files")
    public ApiResponse<Map<String, Object>> runFiles(@PathVariable long id, @PathVariable long runId,
                                                     @RequestParam(defaultValue = "1") int page,
                                                     @RequestParam(defaultValue = "50") int pageSize) {
        return ApiResponse.ok(service.runFilesPage(runId, page, pageSize));
    }

    @GetMapping("/{id}/browse")
    public ApiResponse<Map<String, Object>> browse(@PathVariable long id,
                                                   @RequestParam(defaultValue = "") String path) {
        try {
            return ApiResponse.ok(service.browse(id, path));
        } catch (IllegalArgumentException e) {
            return ApiResponse.error(400, e.getMessage());
        }
    }

    /** 一致性对账(只报告不修改): 列源/目标清单 → 目标缺失/目标多余/大小不一致 */
    @PostMapping("/{id}/reconcile")
    @PreAuthorize("@perm.has('sync:task:run')")
    public ApiResponse<Map<String, Object>> reconcile(@PathVariable long id,
                                                      @RequestParam(required = false) String trigger) {
        try {
            return ApiResponse.ok(service.reconcile(id, trigger == null ? "manual" : trigger));
        } catch (IllegalArgumentException | IllegalStateException e) {
            return ApiResponse.error(400, e.getMessage());
        }
    }

    /** 对账历史 */
    @GetMapping("/{id}/reconciles")
    public ApiResponse<List<Map<String, Object>>> reconciles(@PathVariable long id,
                                                             @RequestParam(defaultValue = "20") int limit) {
        return ApiResponse.ok(service.reconciles(id, limit));
    }
}
