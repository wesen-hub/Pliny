package com.dcp.outlet.adapters;

import com.dcp.outlet.control.HandoverFacts;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;

/**
 * 批阶段终态物化存储(表 {@code batch_job})——补偿对账(CP2a/CP2b)判据载体。
 *
 * <p>语义(与 consumer_task 纯业务登记区分):批执行是耗时段, 控制面重启丢回调/批执行体崩溃
 * 都必须能从元库恢复事实, 故批执行体在运行期心跳续租、终态含 t0(交接输入)与 handover_at
 * (交接完成标记)全部物化。对账 tick 以本表行判活/判终态/判未交接。
 *
 * <p>全部方法静默失败:物化失败不阻断批执行(元库抖动不应打断批), 由对账 tick 以目标态判据兜底。
 */
public class BatchJobStore implements HandoverFacts {

    private final DataSource ds;

    public BatchJobStore(DataSource ds) { this.ds = ds; }

    /** 批开始:物化 RUNNING 行(重做批 = 覆盖重置:清 t0/handover, 重计心跳)。 */
    public void markStarted(String taskId) {
        String sql = "INSERT INTO batch_job(task_id, state, t0, handover_at, heartbeat_at) "
                + "VALUES(?, 'RUNNING', NULL, NULL, NOW()) "
                + "ON DUPLICATE KEY UPDATE state='RUNNING', t0=NULL, handover_at=NULL, heartbeat_at=NOW()";
        try (Connection c = ds.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, taskId);
            ps.executeUpdate();
        } catch (SQLException ignored) { /* 静默:不阻断批, 对账 tick 兜底 */ }
    }

    /** 批执行体心跳续租(每 30s);静默失败——心跳线程不因此打断批。 */
    public void heartbeat(String taskId) {
        String sql = "UPDATE batch_job SET heartbeat_at=NOW() WHERE task_id=?";
        try (Connection c = ds.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, taskId);
            ps.executeUpdate();
        } catch (SQLException ignored) { /* 静默 */ }
    }

    /** 批结束:物化终态(成功带 t0;失败无 t0, 走 batch-failed 事件键人工重做)。 */
    public void markDone(String taskId, boolean ok, long t0) {
        String sql = ok
                ? "UPDATE batch_job SET state='SUCCEEDED', t0=?, heartbeat_at=NOW() WHERE task_id=?"
                : "UPDATE batch_job SET state='FAILED', heartbeat_at=NOW() WHERE task_id=?";
        try (Connection c = ds.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            if (ok) {
                ps.setLong(1, t0);
                ps.setString(2, taskId);
            } else {
                ps.setString(1, taskId);
            }
            ps.executeUpdate();
        } catch (SQLException ignored) { /* 静默:不阻断回调 */ }
    }

    /** 交接完成标记(CP2b 判据收敛依据:补发 handover 成功后回写, 否则判据永不收敛)。 */
    public void markHandover(String taskId) {
        String sql = "UPDATE batch_job SET handover_at=NOW() WHERE task_id=?";
        try (Connection c = ds.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, taskId);
            ps.executeUpdate();
        } catch (SQLException ignored) { /* 静默 */ }
    }

    /** handover-pending 判据(CP2b 修复路径):批 SUCCEEDED ∧ 未交接 → t0;否则 empty。
     *  静默失败返回 empty(保守方向:不误补交接;查询失败下轮再判)。 */
    @Override
    public Optional<Long> pendingT0(String taskId) {
        String sql = "SELECT t0 FROM batch_job "
                + "WHERE task_id=? AND state='SUCCEEDED' AND handover_at IS NULL AND t0 IS NOT NULL";
        try (Connection c = ds.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, taskId);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? Optional.of(rs.getLong(1)) : Optional.empty();
            }
        } catch (SQLException e) {
            return Optional.empty();
        }
    }
}
