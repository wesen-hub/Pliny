package com.dcp.module.lineage;

import com.dcp.common.ApiResponse;
import com.dcp.common.BizException;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

/**
 * 血缘 API(2026-09-09 方案 §6, P0):
 *   POST /api/lineage/rebuild   全量重建血缘图(解析真实存量)
 *   GET  /api/lineage/graph     全图(前端图谱接真)
 *   GET  /api/lineage/upstream?table=&depth=      溯源(多路径)
 *   GET  /api/lineage/downstream?table=&depth=    影响
 *   GET  /api/lineage/field/{table}/{col}/trace   字段链路(最终来源+备选)
 *   GET  /api/lineage/impact?table=&col=          影响评估摘要
 */
@RestController
@RequestMapping("/api/lineage")
@RequiredArgsConstructor
public class LineageController {

    private final LineageService service;

    @PostMapping("/rebuild")
    public ApiResponse<Map<String, Object>> rebuild() {
        return ApiResponse.ok(service.rebuild());
    }

    @GetMapping("/graph")
    public ApiResponse<Map<String, Object>> graph() {
        return ApiResponse.ok(service.graph());
    }

    @GetMapping("/upstream")
    public ApiResponse<List<Map<String, Object>>> upstream(@RequestParam String table,
                                                           @RequestParam(required = false) Integer depth) {
        return ApiResponse.ok(service.upstream(table, depth));
    }

    @GetMapping("/downstream")
    public ApiResponse<List<Map<String, Object>>> downstream(@RequestParam String table,
                                                             @RequestParam(required = false) Integer depth) {
        return ApiResponse.ok(service.downstream(table, depth));
    }

    @GetMapping("/field/{table}/{col}/trace")
    public ApiResponse<Map<String, Object>> trace(@PathVariable String table, @PathVariable String col) {
        return ApiResponse.ok(service.fieldTrace(table, col));
    }

    @GetMapping("/impact")
    public ApiResponse<Map<String, Object>> impact(@RequestParam String table, @RequestParam(required = false) String col) {
        try {
            List<Map<String, Object>> paths = service.downstream(table, 3);
            Map<String, Object> out = new java.util.LinkedHashMap<>();
            out.put("table", table);
            out.put("col", col);
            out.put("paths", paths);
            out.put("pathCount", paths.size());
            // 去重端点统计(应用/表/字段)
            java.util.Set<String> apps = new java.util.HashSet<>(), tabs = new java.util.HashSet<>();
            for (Map<String, Object> p : paths) {
                Object ns = p.get("nodes");
                if (ns instanceof List<?> list) {
                    for (Object o : list) {
                        if (o instanceof Map<?, ?> m) {
                            String type = String.valueOf(m.get("type"));
                            String id = String.valueOf(m.get("id"));
                            if ("App".equals(type)) apps.add(id);
                            if ("Table".equals(type)) tabs.add(id);
                        }
                    }
                }
            }
            out.put("appCount", apps.size());
            out.put("tableCount", tabs.size());
            return ApiResponse.ok(out);
        } catch (BizException e) {
            return ApiResponse.error(e.getCode(), e.getMessage());
        }
    }
}
