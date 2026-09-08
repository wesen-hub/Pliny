package com.dcp.module.tagengine;

import com.dcp.common.ApiResponse;
import org.springframework.web.bind.annotation.*;
import org.springframework.security.access.prepost.PreAuthorize;

import java.util.List;
import java.util.Map;

/**
 * 打标引擎 V2 —— 标签注册表 API(注册表 = tag_dict 树节点 + 声明):
 *
 * <pre>
 *   GET    /api/tagengine/registry       列表(含 tagPath/domain)
 *   POST   /api/tagengine/registry       注册(tagDictId 指向 tag_dict 节点; timing 自动推导 compute/store; sliding/instant 必填 ttl)
 *   PUT    /api/tagengine/registry/{id}  更新声明
 *   DELETE /api/tagengine/registry/{id}  取消注册
 * </pre>
 */
@RestController
@RequestMapping("/api/tagengine/registry")
public class TagRegistryController {

    private final TagRegistryService service;
    private final com.dcp.module.tagengine.perm.DataSecurityService security;

    public TagRegistryController(TagRegistryService service, com.dcp.module.tagengine.perm.DataSecurityService security) {
        this.service = service;
        this.security = security;
    }

    @GetMapping
    public ApiResponse<List<Map<String, Object>>> list() {
        java.util.List<Map<String, Object>> filtered = new java.util.ArrayList<>();
        for (Map<String, Object> r : service.list()) {
            if (security.tagLevel(String.valueOf(r.get("tagPath"))) > 0) filtered.add(r);
        }
        return ApiResponse.ok(filtered);
    }

    @PostMapping
    @PreAuthorize("@perm.has('tag:registry:write')")
    public ApiResponse<Map<String, Object>> create(@RequestBody Map<String, Object> body) {
        try {
            return ApiResponse.ok(service.create(body));
        } catch (IllegalArgumentException e) {
            return ApiResponse.error(400, e.getMessage());
        }
    }

    @PutMapping("/{id}")
    @PreAuthorize("@perm.has('tag:registry:write')")
    public ApiResponse<Map<String, Object>> update(@PathVariable long id, @RequestBody Map<String, Object> body) {
        try {
            return ApiResponse.ok(service.update(id, body));
        } catch (IllegalArgumentException e) {
            return ApiResponse.error(400, e.getMessage());
        }
    }

    @DeleteMapping("/{id}")
    @PreAuthorize("@perm.has('tag:registry:write')")
    public ApiResponse<Map<String, Object>> delete(@PathVariable long id) {
        try {
            service.delete(id);
            return ApiResponse.ok(Map.of("deleted", true, "id", id));
        } catch (IllegalArgumentException e) {
            return ApiResponse.error(400, e.getMessage());
        }
    }
}
