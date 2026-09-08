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
    private final com.dcp.module.tagengine.perm.DataSecurityService security;

    public TagDatasetController(TagDatasetService service, com.dcp.module.tagengine.perm.DataSecurityService security) {
        this.service = service;
        this.security = security;
    }

    @GetMapping
    public ApiResponse<List<Map<String, Object>>> list() {
        // 数据权限 F1: 表 L0 不出现在数据集目录(向导/下拉)
        java.util.List<Map<String, Object>> out = new java.util.ArrayList<>();
        for (Map<String, Object> d : service.listDatasets()) {
            if (security.tableLevel(String.valueOf(d.get("table_name") == null ? d.get("name") : d.get("table_name"))) > 0) out.add(d);
        }
        return ApiResponse.ok(out);
    }

    @GetMapping("/{name}/fields")
    public ApiResponse<Map<String, Object>> fields(@PathVariable String name) {
        try {
            int tbl = security.tableLevel(name);
            if (tbl <= 0) {
                return ApiResponse.error(403, "数据集不可见(表级 L0): " + name);
            }
            Map<String, Object> m = service.fieldsOf(name);
            // 列级 L0 剔除(结构也不可见); L1 起结构可见
            java.util.List<Map<String, Object>> cols = (java.util.List<Map<String, Object>>) m.get("fields");
            if (cols != null) {
                java.util.List<Map<String, Object>> visible = new java.util.ArrayList<>();
                for (Map<String, Object> c : cols) {
                    if (security.columnLevel(name, String.valueOf(c.get("name"))) > 0) visible.add(c);
                }
                m.put("fields", visible);
            }
            return ApiResponse.ok(m);
        } catch (IllegalArgumentException e) {
            return ApiResponse.error(400, e.getMessage());
        }
    }
}
