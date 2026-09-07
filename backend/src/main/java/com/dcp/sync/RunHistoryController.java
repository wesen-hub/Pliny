package com.dcp.sync;

import com.dcp.common.ApiResponse;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 任务执行历史 API(任务维度: 每次 启动/停止/自动完成)。
 */
@RestController
@RequestMapping("/api/sync/history")
public class RunHistoryController {

    private final JdbcTemplate metaJdbc;

    public RunHistoryController(JdbcTemplate metaJdbc) {
        this.metaJdbc = metaJdbc;
    }

    @GetMapping
    public ApiResponse<List<Map<String, Object>>> list(
            @RequestParam(required = false) String logicalTable,
            @RequestParam(defaultValue = "50") int limit) {
        List<Map<String, Object>> out = new ArrayList<>();
        try {
            StringBuilder sql = new StringBuilder(
                    "SELECT logical_table, sync_mode, job_id, action, start_time, end_time, status, message "
                            + "FROM cdc_run_history ");
            List<Object> args = new ArrayList<>();
            if (logicalTable != null && !logicalTable.isBlank()) {
                sql.append("WHERE logical_table = ? ");
                args.add(logicalTable);
            }
            sql.append("ORDER BY id DESC LIMIT ").append(Math.min(Math.max(limit, 1), 200));
            metaJdbc.query(sql.toString(), rs -> {
                Map<String, Object> m = new LinkedHashMap<>();
                m.put("logicalTable", rs.getString("logical_table"));
                m.put("syncMode", rs.getString("sync_mode"));
                m.put("jobId", rs.getString("job_id"));
                m.put("action", rs.getString("action"));
                m.put("startTime", rs.getTimestamp("start_time"));
                m.put("endTime", rs.getTimestamp("end_time"));
                m.put("status", rs.getString("status"));
                m.put("message", rs.getString("message"));
                out.add(m);
            }, args.toArray());
        } catch (Exception e) {
            return ApiResponse.error(500, "历史查询失败: " + e.getMessage());
        }
        return ApiResponse.ok(out);
    }
}
