package com.dcp.module.tagengine.online;

import com.dcp.common.ApiResponse;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

/**
 * 流作业平台化管理 API(P1.1):
 *
 * <pre>
 *   POST /api/tagengine/stream/{taskId}/start    启动流作业(上传 jar + flink run, 记 jobId/状态)
 *   POST /api/tagengine/stream/{taskId}/stop     停止(flink cancel)
 *   GET  /api/tagengine/stream/{taskId}/status   状态(任务行 + Flink 实时)
 * </pre>
 */
@RestController
@RequestMapping("/api/tagengine/stream")
public class StreamJobController {

    private final StreamJobService service;

    public StreamJobController(StreamJobService service) {
        this.service = service;
    }

    @PostMapping("/{taskId}/start")
    public ApiResponse<Map<String, Object>> start(@PathVariable long taskId) {
        try {
            return ApiResponse.ok(service.start(taskId));
        } catch (IllegalArgumentException e) {
            return ApiResponse.error(400, e.getMessage());
        }
    }

    @PostMapping("/{taskId}/stop")
    public ApiResponse<Map<String, Object>> stop(@PathVariable long taskId) {
        try {
            return ApiResponse.ok(service.stop(taskId));
        } catch (IllegalArgumentException e) {
            return ApiResponse.error(400, e.getMessage());
        }
    }

    @GetMapping("/{taskId}/status")
    public ApiResponse<Map<String, Object>> status(@PathVariable long taskId) {
        try {
            return ApiResponse.ok(service.status(taskId));
        } catch (IllegalArgumentException e) {
            return ApiResponse.error(400, e.getMessage());
        }
    }
}
