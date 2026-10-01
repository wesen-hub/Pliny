package com.dcp.filesync.core;

import com.dcp.filesync.util.Strategy;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * run 执行态落库(执行器侧; 方案 N4 DltService 先例: 执行态写迁入业务 jar, 查询/归档留 backend):
 *   - 重派幂等: 每次 start 先清该 runId 旧明细行(判死重派后 rclone 比较式幂等重跑, 明细不残留);
 *   - insertRunFile: 逐文件事件(success/deleted/error);
 *   - finishRun: 收尾 run 表(status/summary/error/finished_at) + task 表(run_status/last_run_summary)
 *     + 全量轮成功后 baseline_done 推进(full_then_incr 下一轮转增量)。
 *
 * <p>run 行本体由 backend 提交 TaskSpec 前 insertRun 创建(executor='platform'), 执行器只收尾。
 */
public class RunRecorder {

    private static final Logger log = LoggerFactory.getLogger(RunRecorder.class);
    private static final ObjectMapper OM = new ObjectMapper();

    private final DataSource ds;

    public RunRecorder(DataSource ds) {
        this.ds = ds;
    }

    /** 判死重派幂等: 清空该 runId 旧明细(rclone 比较式幂等重跑, 明细从头累计) */
    public void resetRunFiles(long runId) {
        exec("DELETE FROM file_sync_run_file WHERE run_id = ?", runId);
    }

    public void insertRunFile(long runId, String relPath, String status, long size, String error) {
        exec("INSERT INTO file_sync_run_file (run_id, rel_path, status, size, error_msg) VALUES (?, ?, ?, ?, ?)",
                runId, relPath, status, size, error);
    }

    /** 收尾 run + task(run_status/last_run_summary); 全量轮成功顺带标记基线就绪 */
    public void finishRun(long runId, String status, Map<String, Object> summary,
                          String error, long taskId, boolean fullRoundOk) {
        String summaryJson;
        try {
            summaryJson = OM.writeValueAsString(summary);
        } catch (Exception e) {
            summaryJson = "{}";
        }
        exec("UPDATE file_sync_run SET status = ?, summary = ?, error_msg = ?, finished_at = NOW() WHERE id = ?",
                status, summaryJson, Strategy.truncate(error, 1900), runId);
        exec("UPDATE file_sync_task SET run_status = ?, last_run_summary = ?, last_run_time = NOW() WHERE id = ?",
                status, summaryJson, taskId);
        if ("COMPLETED".equals(status) && fullRoundOk) {
            int n = update("UPDATE file_sync_task SET baseline_done = 1 WHERE id = ?", taskId);
            log.info("[filesync] run#{} 全量轮成功, 基线就绪 task={} (baseline_done 更新 {} 行)", runId, taskId, n);
        }
    }

    private void exec(String sql, Object... args) {
        try (Connection c = ds.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            for (int i = 0; i < args.length; i++) {
                if (args[i] instanceof Long l) ps.setLong(i + 1, l);
                else if (args[i] instanceof Integer in) ps.setInt(i + 1, in);
                else ps.setObject(i + 1, args[i]);
            }
            ps.executeUpdate();
        } catch (SQLException e) {
            log.warn("[filesync] 落库失败: {} ({})", e.getMessage(), sql.substring(0, Math.min(40, sql.length())));
        }
    }

    private int update(String sql, Object... args) {
        try (Connection c = ds.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            for (int i = 0; i < args.length; i++) ps.setObject(i + 1, args[i]);
            return ps.executeUpdate();
        } catch (SQLException e) {
            log.warn("[filesync] 落库失败: {}", e.getMessage());
            return 0;
        }
    }
}
