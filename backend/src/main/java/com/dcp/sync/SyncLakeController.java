package com.dcp.sync;

import org.springframework.security.access.prepost.PreAuthorize;

import com.dcp.common.ApiResponse;

import org.springframework.web.bind.annotation.*;

import java.util.Map;

/**
 * 权威副本(Iceberg)追平 API(P5):
 *
 * <pre>
 *   GET  /api/sync/lake/status/{table}   权威副本实时状态(行数/最新 seq, 实时派生)
 *   POST /api/sync/lake/flush/{table}    手动触发追平(生产由定时任务每 30s 自动执行)
 * </pre>
 */
@RestController
@RequestMapping("/api/sync/lake")
public class SyncLakeController {

    private final AuthorityMerger authorityMerger;

    public SyncLakeController(AuthorityMerger authorityMerger) {
        this.authorityMerger = authorityMerger;
    }

    @GetMapping("/status/{table}")
    public ApiResponse<Map<String, Object>> status(@PathVariable String table) {
        return ApiResponse.ok(authorityMerger.statusOf(table));
    }

    @PostMapping("/flush/{table}")
    @PreAuthorize("@perm.has('sync:task:run')")
    public ApiResponse<Map<String, Object>> flush(@PathVariable String table) {
        int merged = authorityMerger.flush(table);
        Map<String, Object> out = authorityMerger.statusOf(table);
        out.put("merged", merged);
        return ApiResponse.ok(out);
    }
}
