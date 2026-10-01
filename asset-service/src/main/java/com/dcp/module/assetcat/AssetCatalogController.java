package com.dcp.module.assetcat;

import com.dcp.common.ApiResponse;
import com.dcp.security.SecurityUtils;
import lombok.RequiredArgsConstructor;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

/**
 * 资产目录 API(2026-09-09 P0: 人工业务打标 + 按标签查资产 + 标签树):
 *   POST /api/asset-catalog/tag     {table, path}      打标(幂等)
 *   POST /api/asset-catalog/untag   {table, path}      去标
 *   GET  /api/asset-catalog/tags?table=xxx            某资产标签
 *   GET  /api/asset-catalog/assets?tag=业务域/电商      按标签(含子路径)查资产
 *   GET  /api/asset-catalog/tag-tree                   业务标签树(层级+计数)
 * 自动标签(层/来源/类型)由血缘图推导, 不入库。
 */
@RestController
@RequestMapping("/api/asset-catalog")
@RequiredArgsConstructor
public class AssetCatalogController {

    private final AssetCatalogService service;

    @PostMapping("/tag")
    @PreAuthorize("@perm.isAdmin()")
    public ApiResponse<Map<String, Object>> tag(@RequestBody Map<String, Object> body) {
        String table = String.valueOf(body.getOrDefault("table", ""));
        String path = String.valueOf(body.getOrDefault("path", ""));
        service.tag(table, path, SecurityUtils.currentUsername());
        return ApiResponse.ok(Map.of("tagged", true, "table", table.trim(), "path", path.trim()));
    }

    @PostMapping("/untag")
    @PreAuthorize("@perm.isAdmin()")
    public ApiResponse<Map<String, Object>> untag(@RequestBody Map<String, Object> body) {
        String table = String.valueOf(body.getOrDefault("table", ""));
        String path = String.valueOf(body.getOrDefault("path", ""));
        service.untag(table, path, SecurityUtils.currentUsername());
        return ApiResponse.ok(Map.of("untagged", true, "table", table.trim(), "path", path.trim()));
    }

    @GetMapping("/tags")
    public ApiResponse<List<String>> tags(@RequestParam String table) {
        return ApiResponse.ok(service.tagsOf(table));
    }

    @GetMapping("/assets")
    public ApiResponse<List<String>> assets(@RequestParam String tag) {
        return ApiResponse.ok(service.assetsByTag(tag));
    }

    @GetMapping("/tag-tree")
    public ApiResponse<List<Map<String, Object>>> tagTree() {
        return ApiResponse.ok(service.tagTree());
    }
}
