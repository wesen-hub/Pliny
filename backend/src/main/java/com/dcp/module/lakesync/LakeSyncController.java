package com.dcp.module.lakesync;

import org.springframework.security.access.prepost.PreAuthorize;

import com.dcp.common.ApiResponse;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

/**
 * 湖同步(数据面 → Iceberg)平台调度 API(设计文档 §4b):
 *
 * <pre>
 *   GET    /api/lake-sync/tasks            任务列表(逻辑表粒度)
 *   POST   /api/lake-sync/tasks            新建(逻辑表/调度 interval|cron)
 *   GET    /api/lake-sync/tasks/{id}       详情(含游标)
 *   PUT    /api/lake-sync/tasks/{id}       更新(调度/启停/游标)
 *   DELETE /api/lake-sync/tasks/{id}       删除
 *   POST   /api/lake-sync/tasks/{id}/run   触发一轮(手动; cron 由 15s tick 自动)
 *   GET    /api/lake-sync/runs?taskId=     run 审计列表
 *   GET    /api/lake-sync/runs/{id}        详情(payload/游标前后)
 * </pre>
 */
@RestController
@RequestMapping("/api/lake-sync")
public class LakeSyncController {

    private final LakeSyncService service;

    public LakeSyncController(LakeSyncService service) {
        this.service = service;
    }

    @GetMapping("/tasks")
    public ApiResponse<List<Map<String, Object>>> listTasks() {
        return ApiResponse.ok(service.listTasks());
    }

    @GetMapping("/tasks/{id}")
    public ApiResponse<Map<String, Object>> getTask(@PathVariable long id) {
        try {
            return ApiResponse.ok(service.getTask(id));
        } catch (IllegalArgumentException e) {
            return ApiResponse.error(400, e.getMessage());
        }
    }

    @PostMapping("/tasks")
    @PreAuthorize("@perm.has('sync:task:write')")
    public ApiResponse<Map<String, Object>> createTask(@RequestBody Map<String, Object> body) {
        try {
            return ApiResponse.ok(service.createTask(body));
        } catch (IllegalArgumentException e) {
            return ApiResponse.error(400, e.getMessage());
        }
    }

    @PutMapping("/tasks/{id}")
    @PreAuthorize("@perm.has('sync:task:write')")
    public ApiResponse<Map<String, Object>> updateTask(@PathVariable long id, @RequestBody Map<String, Object> body) {
        try {
            return ApiResponse.ok(service.updateTask(id, body));
        } catch (IllegalArgumentException e) {
            return ApiResponse.error(400, e.getMessage());
        }
    }

    @DeleteMapping("/tasks/{id}")
    @PreAuthorize("@perm.has('sync:task:write')")
    public ApiResponse<Map<String, Object>> deleteTask(@PathVariable long id) {
        try {
            service.deleteTask(id);
            return ApiResponse.ok(Map.of("deleted", true, "id", id));
        } catch (IllegalArgumentException e) {
            return ApiResponse.error(400, e.getMessage());
        }
    }

    @PostMapping("/tasks/{id}/run")
    @PreAuthorize("@perm.has('sync:task:run')")
    public ApiResponse<Map<String, Object>> run(@PathVariable long id,
                                                @RequestParam(required = false) String trigger) {
        try {
            return ApiResponse.ok(service.startRun(id, trigger == null ? "manual" : trigger));
        } catch (IllegalArgumentException e) {
            return ApiResponse.error(400, e.getMessage());
        }
    }

    @GetMapping("/runs")
    public ApiResponse<List<Map<String, Object>>> listRuns(@RequestParam(required = false) Long taskId) {
        List<Map<String, Object>> rows = new java.util.ArrayList<>();
        if (taskId != null) {
            rows = service.listRuns(taskId);
        }
        return ApiResponse.ok(rows);
    }

    @GetMapping("/runs/{id}")
    public ApiResponse<Map<String, Object>> getRun(@PathVariable long id) {
        try {
            return ApiResponse.ok(service.getRun(id));
        } catch (IllegalArgumentException e) {
            return ApiResponse.error(400, e.getMessage());
        }
    }
}
