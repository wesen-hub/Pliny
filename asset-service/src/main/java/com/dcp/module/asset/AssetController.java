package com.dcp.module.asset;

import com.dcp.common.ApiResponse;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

/**
 * 数据资产目录 API(真实数据): 逻辑表 + 来源 + 水位 + 目标分表 Schema/预览 + 字段血缘。
 */
@RestController
@RequestMapping("/api/assets")
public class AssetController {

    private final AssetService assetService;

    public AssetController(AssetService assetService) {
        this.assetService = assetService;
    }

    @GetMapping
    public ApiResponse<List<Map<String, Object>>> list() {
        return ApiResponse.ok(assetService.list());
    }

    @GetMapping("/{table}")
    public ApiResponse<Map<String, Object>> detail(@PathVariable String table) {
        Map<String, Object> detail = assetService.detail(table);
        if (detail == null) {
            return ApiResponse.error(404, "资产不存在: " + table);
        }
        return ApiResponse.ok(detail);
    }

    @GetMapping("/{table}/preview")
    public ApiResponse<Map<String, Object>> preview(@PathVariable String table,
                                                    @RequestParam(defaultValue = "10") int limit) {
        return ApiResponse.ok(assetService.preview(table, limit));
    }

    @GetMapping("/{table}/lineage")
    public ApiResponse<List<Map<String, Object>>> lineage(@PathVariable String table) {
        return ApiResponse.ok(assetService.lineage(table));
    }
}
