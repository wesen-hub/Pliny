package com.dcp.module.tagengine;

import com.dcp.common.ApiResponse;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

/**
 * 标签服务 API(P1; 查询面 = 投影表 tag_fact_view, 由执行器每轮同步 —— 湖 tag_fact 为权威+历史):
 *
 * <pre>
 *   GET /api/tagengine/labels/summary                视图总览(每 tag 实体数/最新 run 时间)
 *   GET /api/tagengine/labels/circle                 圈选: ?tagPath=业务域/交易/订单/金额档[&value=大额][&limit=100] → 实体集
 *   GET /api/tagengine/labels/entity/{entityKey}     实体全标签(当前值)
 * </pre>
 *
 * 订阅/圈选消费端可基于此 API 或直接查 tag_fact_view; 实体×标签当前值由认领任务产出(见 claims)。
 */
@RestController
@RequestMapping("/api/tagengine/labels")
public class TagLabelController {

    private final TagEngineService engine;

    public TagLabelController(TagEngineService engine) {
        this.engine = engine;
    }

    @GetMapping("/summary")
    public ApiResponse<List<Map<String, Object>>> summary() {
        return ApiResponse.ok(engine.labelSummary());
    }

    @GetMapping("/circle")
    public ApiResponse<Map<String, Object>> circle(@RequestParam String tagPath,
                                                   @RequestParam(required = false) String value,
                                                   @RequestParam(defaultValue = "100") int limit) {
        try {
            return ApiResponse.ok(engine.labelCircle(tagPath, value, Math.min(Math.max(limit, 1), 1000)));
        } catch (IllegalArgumentException e) {
            return ApiResponse.error(400, e.getMessage());
        }
    }

    @GetMapping("/entity/{entityKey}")
    public ApiResponse<Map<String, Object>> entity(@PathVariable String entityKey) {
        try {
            return ApiResponse.ok(engine.labelEntity(entityKey));
        } catch (IllegalArgumentException e) {
            return ApiResponse.error(400, e.getMessage());
        }
    }
}
