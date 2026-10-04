package com.dcp.sync;

import com.dcp.common.ApiResponse;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 同步总览聚合 API(前端 sync/Overview 页):
 *
 * <pre>
 *   GET /api/sync/overview →
 *     cards:   运行中采集任务 / 待处理死信 / 逻辑表数 / 今日新增死信(全部实时元库口径)
 *     tasks:   逻辑表 × 采集任务(名称/源库表/主题/运行状态), 无任务行仅逻辑表配置
 *     notices: 死信待处理告警 / 采集任务未运行待办 / 运行中提示
 * </pre>
 * 聚合口径与死信页、采集任务页一致(同库同表), 不引入新数据源。
 */
@RestController
@RequestMapping("/api/sync/overview")
public class SyncOverviewController {

    private final JdbcTemplate metaJdbc;

    public SyncOverviewController(JdbcTemplate metaJdbc) {
        this.metaJdbc = metaJdbc;
    }

    @GetMapping
    public ApiResponse<Map<String, Object>> overview() {
        Long running = metaJdbc.queryForObject(
                "SELECT COUNT(*) FROM cdc_st_task WHERE running = 1", Long.class);
        Long pendingDlt = metaJdbc.queryForObject(
                "SELECT COUNT(*) FROM dlt_message WHERE status = 'PENDING'", Long.class);
        Long todayDlt = metaJdbc.queryForObject(
                "SELECT COUNT(*) FROM dlt_message WHERE DATE(last_fail_time) = CURDATE()", Long.class);
        Long tables = metaJdbc.queryForObject(
                "SELECT COUNT(*) FROM logical_table", Long.class);

        List<Map<String, Object>> cards = new ArrayList<>();
        cards.add(card("运行中采集任务", running == null ? 0L : running, ""));
        cards.add(card("待处理死信", pendingDlt == null ? 0L : pendingDlt, ""));
        cards.add(card("逻辑表数", tables == null ? 0L : tables, ""));
        cards.add(card("今日新增死信", todayDlt == null ? 0L : todayDlt, ""));

        // 任务列表: 采集任务行 LEFT JOIN 逻辑表(逻辑表已配置但未注册任务时也可见, 状态=未配置任务)
        List<Map<String, Object>> tasks = metaJdbc.queryForList(
                "SELECT t.task_id, t.logical_table, t.source_db, t.source_table, t.topic, t.running, "
                        + "t.update_time, l.table_count, l.table_prefix "
                        + "FROM cdc_st_task t LEFT JOIN logical_table l ON l.table_name = t.logical_table "
                        + "ORDER BY t.running DESC, t.task_id DESC");
        List<Map<String, Object>> taskRows = new ArrayList<>();
        for (Map<String, Object> t : tasks) {
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("id", t.get("task_id"));
            row.put("name", t.get("logical_table"));
            row.put("source", t.get("source_db") + "." + t.get("source_table"));
            row.put("topic", t.get("topic"));
            row.put("running", Integer.valueOf(1).equals(t.get("running")));
            row.put("updateTime", t.get("update_time"));
            taskRows.add(row);
        }

        // 待办与告警(基于同一批实时指标, 不造静态文案)
        List<Map<String, Object>> notices = new ArrayList<>();
        long dlt = pendingDlt == null ? 0L : pendingDlt;
        if (dlt > 0) {
            Map<String, Object> n = new LinkedHashMap<>();
            n.put("level", "danger");
            n.put("text", "待处理死信 " + dlt + " 条(消费失败留证, 请到死信监控页排查/重放)");
            notices.add(n);
        }
        long run = running == null ? 0L : running;
        long tbl = tables == null ? 0L : tables;
        if (run == 0 && tbl > 0) {
            Map<String, Object> n = new LinkedHashMap<>();
            n.put("level", "warning");
            n.put("text", "已配置 " + tbl + " 张逻辑表, 当前无运行中的采集任务");
            notices.add(n);
        }
        if (run > 0) {
            Map<String, Object> n = new LinkedHashMap<>();
            n.put("level", "success");
            n.put("text", "采集任务运行中 " + run + " 个");
            notices.add(n);
        }
        if (notices.isEmpty()) {
            Map<String, Object> n = new LinkedHashMap<>();
            n.put("level", "info");
            n.put("text", "暂无待办: 未配置逻辑表");
            notices.add(n);
        }

        Map<String, Object> out = new LinkedHashMap<>();
        out.put("cards", cards);
        out.put("tasks", taskRows);
        out.put("notices", notices);
        return ApiResponse.ok(out);
    }

    private static Map<String, Object> card(String label, long value, String unit) {
        Map<String, Object> c = new LinkedHashMap<>();
        c.put("label", label);
        c.put("value", value);
        c.put("unit", unit);
        return c;
    }
}
