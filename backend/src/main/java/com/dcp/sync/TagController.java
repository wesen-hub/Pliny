package com.dcp.sync;

import com.dcp.common.ApiResponse;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

/**
 * 标签体系 API: 多级标签树 + 维护 + 标签下资产。
 */
@RestController
@RequestMapping("/api/tags")
public class TagController {

    private final TagService tagService;

    public TagController(TagService tagService) {
        this.tagService = tagService;
    }

    /** 标签树(含 path 与挂载资产数) */
    @GetMapping("/tree")
    public ApiResponse<List<Map<String, Object>>> tree() {
        return ApiResponse.ok(tagService.tree());
    }

    /** 某标签(含子孙)下的资产列表 */
    @GetMapping("/assets")
    public ApiResponse<List<Map<String, Object>>> assetsOfTag(@RequestParam String path) {
        return ApiResponse.ok(tagService.assetsOfTag(path));
    }

    /** 新建标签 */
    @PostMapping
    public ApiResponse<Map<String, Object>> create(@RequestBody Map<String, Object> body) {
        long id = tagService.create(
                String.valueOf(body.get("name")),
                body.get("parentId") == null ? 0 : ((Number) body.get("parentId")).longValue(),
                (String) body.get("category"),
                (String) body.get("granularity"),
                (String) body.get("source"));
        return ApiResponse.ok(Map.of("id", id));
    }

    /** 更新标签 */
    @PutMapping("/{id}")
    public ApiResponse<Void> update(@PathVariable long id, @RequestBody Map<String, Object> body) {
        tagService.update(id, (String) body.get("name"), (String) body.get("category"),
                (String) body.get("granularity"), (String) body.get("source"), (String) body.get("status"));
        return ApiResponse.ok(null);
    }

    /** 删除标签(连带子标签) */
    @DeleteMapping("/{id}")
    public ApiResponse<Void> delete(@PathVariable long id) {
        tagService.delete(id);
        return ApiResponse.ok(null);
    }

    /** 移动标签(调整目录层级; 新父不能是自己的子孙) */
    @PutMapping("/{id}/move")
    public ApiResponse<Void> move(@PathVariable long id, @RequestBody Map<String, Object> body) {
        tagService.move(id, body.get("parentId") == null ? 0 : ((Number) body.get("parentId")).longValue());
        return ApiResponse.ok(null);
    }

    /**
     * 动态子集: 按标签组合筛选资产(目录查询子集)。
     * GET /api/tags/subset?tags=业务域/交易,密级/内部&op=AND&exclude=来源系统/MongoDB
     */
    @GetMapping("/subset")
    public ApiResponse<List<Map<String, Object>>> subset(
            @RequestParam(required = false) List<String> tags,
            @RequestParam(defaultValue = "AND") String op,
            @RequestParam(required = false) List<String> exclude) {
        return ApiResponse.ok(tagService.subset(tags, op, exclude));
    }
}
