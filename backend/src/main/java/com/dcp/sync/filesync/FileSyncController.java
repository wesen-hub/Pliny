package com.dcp.sync.filesync;

import org.springframework.security.access.prepost.PreAuthorize;

import com.dcp.common.ApiResponse;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

/**
 * 文件同步 V3 API(mc mirror 原生, 取代 V2 /api/filesync/copy/*):
 *
 * <pre>
 *   GET    /api/filesync/tasks                     列表
 *   POST   /api/filesync/tasks                     创建(任务定义 JSON)
 *   GET    /api/filesync/tasks/{id}                详情
 *   PUT    /api/filesync/tasks/{id}                更新(version 乐观锁)
 *   DELETE /api/filesync/tasks/{id}                删除(agent 任务附带 task_remove 指令)
 *   POST   /api/filesync/tasks/{id}/run            手动触发一轮(异步)
 *   GET    /api/filesync/tasks/{id}/runs           run 分页(传输历史)
 *   GET    /api/filesync/tasks/{id}/runs/{runId}/files  run 文件明细分页
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
        boolean isWatch = Boolean.TRUE.equals(d.get("watch")) || "1".equals(String.valueOf(d.get("watch")));
        if (isWatch && "platform".equals(String.valueOf(d.get("exec_host")))) {
            try {
                boolean started = service.startWatch(id);
                return ApiResponse.ok(Map.of("started", started, "watching", started, "id", id));
            } catch (IllegalStateException e) {
                return ApiResponse.error(400, e.getMessage());
            }
        }
        boolean started = service.runAsync(id);
        if (!started) {
            return ApiResponse.error(400, "任务正在执行中, 请稍后再试");
        }
        boolean agentTask = "agent".equals(String.valueOf(d.get("exec_host")));
        return ApiResponse.ok(Map.of("started", true, "id", id, "execHost", d.get("exec_host"),
                "online", agentTask ? Boolean.TRUE.equals(d.get("online")) : null));
    }

    @PostMapping("/{id}/stop")
    @PreAuthorize("@perm.has('sync:task:run')")
    public ApiResponse<Map<String, Object>> stop(@PathVariable long id) {
        Map<String, Object> d = service.detail(id);
        if (d == null) {
            return ApiResponse.error(404, "任务不存在: " + id);
        }
        boolean stopped;
        if ("agent".equals(String.valueOf(d.get("exec_host")))) {
            stopped = service.stopAgentWatch(id);
            return ApiResponse.ok(Map.of("stopped", stopped, "id", id, "dispatchedToAgent", stopped));
        }
        stopped = service.stopWatch(id);
        return ApiResponse.ok(Map.of("stopped", stopped, "id", id));
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
}
