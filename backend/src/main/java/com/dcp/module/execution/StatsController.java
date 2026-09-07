package com.dcp.module.execution;

import com.dcp.common.ApiResponse;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** 统计报表（F-603，直接 SQL 聚合） */
@RestController
@RequestMapping("/api/stats")
@RequiredArgsConstructor
public class StatsController {

    private final StatsMapper statsMapper;

    @GetMapping("/overview")
    public ApiResponse<Map<String, Object>> overview() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("todayExecutions", statsMapper.todayExecutions());
        m.put("successRate24h", statsMapper.total24h() == 0 ? 0
                : Math.round(statsMapper.success24h() * 10000.0 / statsMapper.total24h()) / 100.0);
        m.put("runningCount", statsMapper.runningCount());
        m.put("jobCount", statsMapper.jobCount());
        m.put("datasourceCount", statsMapper.datasourceCount());
        return ApiResponse.ok(m);
    }

    @GetMapping("/trend")
    public ApiResponse<List<Map<String, Object>>> trend(@RequestParam(defaultValue = "7") int days) {
        return ApiResponse.ok(statsMapper.trend(Math.min(Math.max(days, 1), 90)));
    }

    @GetMapping("/task-rank")
    public ApiResponse<List<Map<String, Object>>> taskRank(@RequestParam(defaultValue = "10") int limit) {
        return ApiResponse.ok(statsMapper.taskRank(Math.min(Math.max(limit, 1), 50)));
    }
}
