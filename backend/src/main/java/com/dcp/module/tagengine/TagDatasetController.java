package com.dcp.module.tagengine;

import com.dcp.common.ApiResponse;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

/**
 * 打标引擎 V2 —— 数据集元数据 API(数据集 = 逻辑表):
 *
 * <pre>
 *   GET /api/tagengine/datasets                   逻辑表列表(元库)
 *   GET /api/tagengine/datasets/{name}/fields     真实字段元数据(name/rawType/kind/主键) —— 生产为湖 schema, 现代理权威/分片物理表
 * </pre>
 */
@RestController
@RequestMapping("/api/tagengine/datasets")
public class TagDatasetController {

    private final TagDatasetService service;

    public TagDatasetController(TagDatasetService service) {
        this.service = service;
    }

    @GetMapping
    public ApiResponse<List<Map<String, Object>>> list() {
        return ApiResponse.ok(service.listDatasets());
    }

    @GetMapping("/{name}/fields")
    public ApiResponse<Map<String, Object>> fields(@PathVariable String name) {
        try {
            return ApiResponse.ok(service.fieldsOf(name));
        } catch (IllegalArgumentException e) {
            return ApiResponse.error(400, e.getMessage());
        }
    }
}
