package com.dcp.module.tagengine;

import com.dcp.common.ApiResponse;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

/**
 * 标签认领/定位查询 API(P0.5 收口 ⑥; 设计 §13 "标签由首次发布任务认领"):
 *
 * <pre>
 *   GET /api/tagengine/claims            全部认领: 标签 → 认领任务(首个 published 产出的任务)+ 湖定位 + 类型/值域
 *   GET /api/tagengine/claims/{tagPath}  单标签查询引导(湖表 + spark-sql 示例)
 * </pre>
 *
 * 认领语义: 发布时同标签跨任务冲突拦截(见 TagEngineService.checkPublishTags), 因此"认领者"=
 * 所有 published 任务快照中产出该标签的任务(首个, 取 ver 最小); 冲突被拦保证每标签至多一个认领任务。
 * 实体×标签当前值数据 = 认领任务的 tag_fact 湖表(Iceberg time travel 留史), 平台查询 API 给定位+示例,
 * 湖读由 Spark 面消费(执行器/外部查询); 订阅/圈选消费端为 P1。
 */
@RestController
@RequestMapping("/api/tagengine/claims")
public class TagClaimController {

    private final TagEngineService engine;

    public TagClaimController(TagEngineService engine) {
        this.engine = engine;
    }

    @GetMapping
    public ApiResponse<List<Map<String, Object>>> list() {
        return ApiResponse.ok(engine.listClaims());
    }

    @GetMapping("/{tagPath}")
    public ApiResponse<Map<String, Object>> getByPath(@PathVariable String tagPath,
                                                      @RequestParam(required = false) String path) {
        String real = (path != null && !path.isBlank()) ? path : tagPath;
        try {
            return ApiResponse.ok(engine.getClaim(real));
        } catch (IllegalArgumentException e) {
            return ApiResponse.error(400, e.getMessage());
        }
    }
}
