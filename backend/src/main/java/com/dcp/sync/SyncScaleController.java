package com.dcp.sync;

import com.dcp.common.ApiResponse;

import org.springframework.web.bind.annotation.*;

import java.util.Map;

/**
 * 扩容 API(V3 §6):
 *
 * <pre>
 *   GET  /api/sync/scale/lag/{table}              单逻辑表 lag(各分区 + 总量)
 *   POST /api/sync/scale/expand/{table}?partitions=16  加分区扩容(建分区 + 重算桶映射 + 热更新)
 * </pre>
 */
@RestController
@RequestMapping("/api/sync/scale")
public class SyncScaleController {

    private final ScaleService scaleService;

    public SyncScaleController(ScaleService scaleService) {
        this.scaleService = scaleService;
    }

    @GetMapping("/lag/{table}")
    public ApiResponse<Map<String, Object>> lag(@PathVariable String table) {
        return ApiResponse.ok(scaleService.lagOf(table));
    }

    @PostMapping("/expand/{table}")
    public ApiResponse<Map<String, Object>> expand(@PathVariable String table,
                                      @RequestParam int partitions) {
        return ApiResponse.ok(scaleService.expandPartitions(table, partitions));
    }
}
