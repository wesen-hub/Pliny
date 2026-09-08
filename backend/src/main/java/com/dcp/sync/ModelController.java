package com.dcp.sync;

import org.springframework.security.access.prepost.PreAuthorize;

import com.dcp.common.ApiResponse;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

/**
 * 模型管理 API(建模即发布: 建 topic/分区 + 预建物理表)。
 */
@RestController
@RequestMapping("/api/models")
public class ModelController {

    private final ModelService modelService;

    public ModelController(ModelService modelService) {
        this.modelService = modelService;
    }

    @GetMapping
    public ApiResponse<List<Map<String, Object>>> list() {
        return ApiResponse.ok(modelService.list());
    }

    @GetMapping("/{id}")
    public ApiResponse<Map<String, Object>> get(@PathVariable long id) {
        Map<String, Object> m = modelService.get(id);
        if (m == null) {
            return ApiResponse.error(404, "模型不存在: " + id);
        }
        return ApiResponse.ok(m);
    }

    @PostMapping
    @PreAuthorize("@perm.has('job:write')")
    public ApiResponse<Map<String, Object>> create(@RequestBody Map<String, Object> body) {
        try {
            @SuppressWarnings("unchecked")
            List<String> keys = body.get("keyColumns") instanceof List<?> l
                    ? l.stream().map(String::valueOf).toList() : List.of();
            String fieldsJson = body.get("fields") == null ? null : toJson(body.get("fields"));
            long id = modelService.create(
                    (String) body.get("name"),
                    (String) body.get("layer"),
                    (String) body.get("domain"),
                    String.valueOf(body.get("tableName")),
                    body.get("partitionCount") == null ? 8 : ((Number) body.get("partitionCount")).intValue(),
                    body.get("tableCount") == null ? 50 : ((Number) body.get("tableCount")).intValue(),
                    keys, fieldsJson, (String) body.get("description"));
            return ApiResponse.ok(Map.of("id", id, "tableName", String.valueOf(body.get("tableName"))));
        } catch (IllegalArgumentException e) {
            return ApiResponse.error(400, e.getMessage());
        }
    }

    @DeleteMapping("/{id}")
    @PreAuthorize("@perm.has('job:write')")
    public ApiResponse<Void> delete(@PathVariable long id) {
        modelService.delete(id);
        return ApiResponse.ok(null);
    }

    private String toJson(Object o) {
        try {
            return new com.fasterxml.jackson.databind.ObjectMapper().writeValueAsString(o);
        } catch (Exception e) {
            return null;
        }
    }
}
