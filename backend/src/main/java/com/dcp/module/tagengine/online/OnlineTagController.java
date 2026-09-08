package com.dcp.module.tagengine.online;

import com.dcp.common.ApiResponse;
import org.springframework.web.bind.annotation.*;
import org.springframework.security.access.prepost.PreAuthorize;

import java.util.Map;

/**
 * 在线打标执行 API(P1; 设计 §4 在线态):
 *
 * <pre>
 *   GET  /api/tagengine/online/{taskId}            任务在线信息(需 published)
 *   POST /api/tagengine/online/{taskId}/event      事件求值 + Redis 置位
 *        body: {"entityKey字段":值, 业务字段..., "ttlSeconds":可选}
 *   GET  /api/tagengine/online/entity/{entityKey}  读在线层实体标签(Redis)
 * </pre>
 *
 * dev 事件源: 手动 POST(演示); 生产接 Kafka dcp.in(与 ingest 同源, 见设计 §4)。
 */
@RestController
@RequestMapping("/api/tagengine/online")
public class OnlineTagController {

    private final OnlineTagService service;

    public OnlineTagController(OnlineTagService service) {
        this.service = service;
    }

    @GetMapping("/{taskId}")
    public ApiResponse<Map<String, Object>> info(@PathVariable long taskId) {
        try {
            return ApiResponse.ok(service.info(taskId));
        } catch (IllegalArgumentException e) {
            return ApiResponse.error(400, e.getMessage());
        }
    }

    @PostMapping("/{taskId}/event")
    @PreAuthorize("@perm.has('tag:task:run')")
    public ApiResponse<Map<String, Object>> event(@PathVariable long taskId, @RequestBody Map<String, Object> body) {
        try {
            Object ttl = body.remove("ttlSeconds");
            Long ttlSeconds = ttl instanceof Number n ? n.longValue() : null;
            return ApiResponse.ok(service.handleEvent(taskId, body, ttlSeconds));
        } catch (IllegalArgumentException e) {
            return ApiResponse.error(400, e.getMessage());
        }
    }

    @GetMapping("/entity/{entityKey}")
    public ApiResponse<Map<String, Object>> entity(@PathVariable String entityKey) {
        try {
            return ApiResponse.ok(service.readOnline(entityKey));
        } catch (IllegalArgumentException e) {
            return ApiResponse.error(400, e.getMessage());
        }
    }
}
