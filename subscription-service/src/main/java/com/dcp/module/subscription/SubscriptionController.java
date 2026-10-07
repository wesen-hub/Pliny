package com.dcp.module.subscription;

import org.springframework.security.access.prepost.PreAuthorize;

import com.dcp.common.ApiResponse;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

/**
 * 订阅中心 API: 订阅 = 标签条件, 命中资产 = 标签子集(与资产目录打通)。
 */
@RestController
@RequestMapping("/api/subscriptions")
public class SubscriptionController {

    private final SubscriptionService subscriptionService;

    public SubscriptionController(SubscriptionService subscriptionService) {
        this.subscriptionService = subscriptionService;
    }

    @GetMapping
    public ApiResponse<List<Map<String, Object>>> list() {
        return ApiResponse.ok(subscriptionService.list());
    }

    @GetMapping("/{id}")
    public ApiResponse<Map<String, Object>> detail(@PathVariable long id) {
        Map<String, Object> detail = subscriptionService.detail(id);
        if (detail == null) {
            return ApiResponse.error(404, "订阅不存在: " + id);
        }
        return ApiResponse.ok(detail);
    }

    @GetMapping("/{id}/assets")
    public ApiResponse<List<Map<String, Object>>> matchedAssets(@PathVariable long id) {
        Map<String, Object> detail = subscriptionService.detail(id);
        if (detail == null) {
            return ApiResponse.error(404, "订阅不存在: " + id);
        }
        return ApiResponse.ok(subscriptionService.matchedAssets(detail));
    }

    @PostMapping
    @PreAuthorize("@perm.has('tag:sub:manage')")
    public ApiResponse<Map<String, Object>> create(@RequestBody Map<String, Object> body) {
        long id = subscriptionService.create(
                String.valueOf(body.get("name")),
                body.get("expr") instanceof Map<?, ?> m ? (Map<String, Object>) m : Map.of(),
                (String) body.get("mode"),
                (String) body.get("target"),
                (String) body.get("createBy"));
        return ApiResponse.ok(Map.of("id", id));
    }

    @PutMapping("/{id}")
    @PreAuthorize("@perm.has('tag:sub:manage')")
    public ApiResponse<Void> update(@PathVariable long id, @RequestBody Map<String, Object> body) {
        subscriptionService.update(id, (String) body.get("name"),
                body.get("expr") instanceof Map<?, ?> m ? (Map<String, Object>) m : null,
                (String) body.get("mode"), (String) body.get("target"));
        return ApiResponse.ok(null);
    }

    /** 状态切换: 运行中/暂停 */
    @PutMapping("/{id}/status")
    @PreAuthorize("@perm.has('tag:sub:manage')")
    public ApiResponse<Void> setStatus(@PathVariable long id, @RequestBody Map<String, Object> body) {
        subscriptionService.setStatus(id, String.valueOf(body.get("status")));
        return ApiResponse.ok(null);
    }

    @DeleteMapping("/{id}")
    @PreAuthorize("@perm.has('tag:sub:manage')")
    public ApiResponse<Void> delete(@PathVariable long id) {
        subscriptionService.delete(id);
        return ApiResponse.ok(null);
    }
}
