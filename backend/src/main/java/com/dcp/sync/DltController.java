package com.dcp.sync;

import com.dcp.common.ApiResponse;

import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

/**
 * 同步死信队列 API(死信监控页):
 *
 * <pre>
 *   GET  /api/sync/dlt                分页查询(logicalTable/status/keyword/page/pageSize)
 *   GET  /api/sync/dlt/stats          统计卡片(待处理/今日/近7天/重放数/Top表)
 *   GET  /api/sync/dlt/{id}           详情(含 payload 全文)
 *   POST /api/sync/dlt/{id}/replay    重放单条(原样重发原 topic)
 *   POST /api/sync/dlt/replay-batch   批量重放 {ids:[]}
 *   POST /api/sync/dlt/{id}/ignore    忽略
 * </pre>
 */
@RestController
@RequestMapping("/api/sync/dlt")
public class DltController {

    private final DltAdminService dltService;

    public DltController(DltAdminService dltService) {
        this.dltService = dltService;
    }

    @GetMapping
    public ApiResponse<Map<String, Object>> page(
            @RequestParam(required = false) String logicalTable,
            @RequestParam(required = false) String status,
            @RequestParam(required = false) String keyword,
            @RequestParam(required = false) String sourceTable,
            @RequestParam(defaultValue = "1") int page,
            @RequestParam(defaultValue = "20") int pageSize) {
        return ApiResponse.ok(dltService.page(logicalTable, status, keyword, sourceTable, page, pageSize));
    }

    @GetMapping("/stats")
    public ApiResponse<Map<String, Object>> stats() {
        return ApiResponse.ok(dltService.stats());
    }

    @GetMapping("/{id}")
    public ApiResponse<Map<String, Object>> detail(@PathVariable Long id) {
        return ApiResponse.ok(dltService.detail(id));
    }

    @PostMapping("/{id}/replay")
    public ApiResponse<Map<String, Object>> replay(@PathVariable Long id) {
        return ApiResponse.ok(dltService.replay(id));
    }

    @PostMapping("/replay-batch")
    public ApiResponse<Map<String, Object>> replayBatch(@RequestBody Map<String, Object> body) {
        @SuppressWarnings("unchecked")
        List<Object> ids = (List<Object>) body.getOrDefault("ids", List.of());
        int n = dltService.replayBatch(ids.stream().map(x -> ((Number) x).longValue()).toList());
        return ApiResponse.ok(Map.of("replayed", n));
    }

    @PostMapping("/{id}/ignore")
    public ApiResponse<Map<String, Object>> ignore(@PathVariable Long id) {
        dltService.ignore(id);
        return ApiResponse.ok(Map.of("ignored", true, "id", id));
    }
}
