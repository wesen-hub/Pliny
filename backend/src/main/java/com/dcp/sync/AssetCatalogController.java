package com.dcp.sync;

import com.dcp.common.ApiResponse;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

/**
 * 数据资产编目 API(可维护): 多层级目录树 + 节点/挂载/资产元数据维护。
 */
@RestController
@RequestMapping("/api/asset-catalog")
public class AssetCatalogController {

    private final AssetCatalogService catalog;

    public AssetCatalogController(AssetCatalogService catalog) {
        this.catalog = catalog;
    }

    /** 目录树(某维度) */
    @GetMapping("/tree")
    public ApiResponse<Map<String, Object>> tree(@RequestParam(defaultValue = "biz") String dim) {
        return ApiResponse.ok(catalog.tree(dim));
    }

    /** 资产列表(含可维护元数据) */
    @GetMapping("/assets")
    public ApiResponse<List<Map<String, Object>>> assets() {
        return ApiResponse.ok(catalog.assetsWithMeta());
    }

    /** 建节点 */
    @PostMapping("/nodes")
    public ApiResponse<Map<String, Object>> createNode(@RequestBody Map<String, Object> body) {
        long id = catalog.createNode(
                String.valueOf(body.get("dim")),
                body.get("parentId") == null ? 0 : ((Number) body.get("parentId")).longValue(),
                String.valueOf(body.get("name")),
                body.get("sort") == null ? 0 : ((Number) body.get("sort")).intValue());
        return ApiResponse.ok(Map.of("id", id));
    }

    /** 重命名节点 */
    @PutMapping("/nodes/{id}")
    public ApiResponse<Void> renameNode(@PathVariable long id, @RequestBody Map<String, Object> body) {
        catalog.renameNode(id, String.valueOf(body.get("name")));
        return ApiResponse.ok(null);
    }

    /** 删除节点(连带子节点与挂载) */
    @DeleteMapping("/nodes/{id}")
    public ApiResponse<Void> deleteNode(@PathVariable long id) {
        catalog.deleteNode(id);
        return ApiResponse.ok(null);
    }

    /** 挂载资产到节点 */
    @PostMapping("/bind")
    public ApiResponse<Void> bind(@RequestBody Map<String, Object> body) {
        catalog.bind(((Number) body.get("nodeId")).longValue(), String.valueOf(body.get("tableName")));
        return ApiResponse.ok(null);
    }

    /** 取消挂载 */
    @DeleteMapping("/bind")
    public ApiResponse<Void> unbind(@RequestParam long nodeId, @RequestParam String tableName) {
        catalog.unbind(nodeId, tableName);
        return ApiResponse.ok(null);
    }

    /** 编辑资产元数据 */
    @PutMapping("/assets/{table}")
    public ApiResponse<Void> saveMeta(@PathVariable String table, @RequestBody Map<String, Object> body) {
        catalog.saveMeta(table, body);
        return ApiResponse.ok(null);
    }
}
