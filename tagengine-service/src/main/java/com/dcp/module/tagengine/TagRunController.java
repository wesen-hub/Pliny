package com.dcp.module.tagengine;

import com.dcp.common.ApiResponse;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

/**
 * 打标引擎 V2 —— run 审计/执行回写 API:
 *
 * <pre>
 *   GET  /api/tagengine/runs?taskId=&limit=    运行记录列表
 *   GET  /api/tagengine/runs/{id}              详情(含 task-json payload 预览)
 *   POST /api/tagengine/runs/{id}/finish       执行器回写(状态/exit/summary/错误; 命中矩阵回写待 E2E 接线)
 * </pre>
 */
@RestController
@RequestMapping("/api/tagengine/runs")
public class TagRunController {

    private final TagEngineService service;

    public TagRunController(TagEngineService service) {
        this.service = service;
    }

    @GetMapping
    public ApiResponse<List<Map<String, Object>>> list(@RequestParam(required = false) Long taskId,
                                                       @RequestParam(defaultValue = "50") int limit) {
        return ApiResponse.ok(service.listRuns(taskId, limit));
    }

    @GetMapping("/{id}")
    public ApiResponse<Map<String, Object>> get(@PathVariable long id) {
        try {
            return ApiResponse.ok(service.getRun(id));
        } catch (IllegalArgumentException e) {
            return ApiResponse.error(400, e.getMessage());
        }
    }

    @PostMapping("/{id}/finish")
    public ApiResponse<Map<String, Object>> finish(@PathVariable long id, @RequestBody Map<String, Object> body) {
        try {
            return ApiResponse.ok(service.finishRun(id, body));
        } catch (IllegalArgumentException e) {
            return ApiResponse.error(400, e.getMessage());
        }
    }
}
