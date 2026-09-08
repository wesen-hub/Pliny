package com.dcp.module.tagengine;

import com.dcp.common.ApiResponse;
import org.springframework.web.bind.annotation.*;
import org.springframework.security.access.prepost.PreAuthorize;

import java.util.List;
import java.util.Map;

/**
 * 打标引擎 V2 —— 规则库 API(规则管理 kind=rule / 规则流管理 kind=flow):
 *
 * <pre>
 *   GET    /api/tagengine/rules?kind=rule|flow   列表(含 usedBy / deps)
 *   POST   /api/tagengine/rules                  新建(校验定义/依赖/引用/环)
 *   GET    /api/tagengine/rules/{id}             详情
 *   PUT    /api/tagengine/rules/{id}             编辑(升版本, kind 不可改)
 *   DELETE /api/tagengine/rules/{id}             删除(被规则流引用拦截)
 *   POST   /api/tagengine/rules/{id}/copy        复制为副本
 * </pre>
 */
@RestController
@RequestMapping("/api/tagengine/rules")
public class TagRuleController {

    private final TagEngineService service;

    public TagRuleController(TagEngineService service) {
        this.service = service;
    }

    @GetMapping
    public ApiResponse<List<Map<String, Object>>> list(@RequestParam(required = false) String kind) {
        return ApiResponse.ok(service.listRules(kind));
    }

    @GetMapping("/{id}")
    public ApiResponse<Map<String, Object>> get(@PathVariable long id) {
        try {
            return ApiResponse.ok(service.getRule(id));
        } catch (IllegalArgumentException e) {
            return ApiResponse.error(400, e.getMessage());
        }
    }

    @PostMapping
    @PreAuthorize("@perm.has('tag:rule:write')")
    public ApiResponse<Map<String, Object>> create(@RequestBody Map<String, Object> body) {
        try {
            return ApiResponse.ok(service.createRule(body));
        } catch (IllegalArgumentException e) {
            return ApiResponse.error(400, e.getMessage());
        }
    }

    @PutMapping("/{id}")
    @PreAuthorize("@perm.has('tag:rule:write')")
    public ApiResponse<Map<String, Object>> update(@PathVariable long id, @RequestBody Map<String, Object> body) {
        try {
            return ApiResponse.ok(service.updateRule(id, body));
        } catch (IllegalArgumentException e) {
            return ApiResponse.error(400, e.getMessage());
        }
    }

    @PostMapping("/{id}/copy")
    @PreAuthorize("@perm.has('tag:rule:write')")
    public ApiResponse<Map<String, Object>> copy(@PathVariable long id) {
        try {
            return ApiResponse.ok(service.copyRule(id));
        } catch (IllegalArgumentException e) {
            return ApiResponse.error(400, e.getMessage());
        }
    }

    @DeleteMapping("/{id}")
    @PreAuthorize("@perm.has('tag:rule:write')")
    public ApiResponse<Map<String, Object>> delete(@PathVariable long id) {
        try {
            service.deleteRule(id);
            return ApiResponse.ok(Map.of("deleted", true, "id", id));
        } catch (IllegalArgumentException e) {
            return ApiResponse.error(400, e.getMessage());
        }
    }
}
