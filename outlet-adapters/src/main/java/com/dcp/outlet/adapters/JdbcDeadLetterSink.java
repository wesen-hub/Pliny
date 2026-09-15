package com.dcp.outlet.adapters;

import com.dcp.outlet.worker.DecodedMessage;
import com.dcp.outlet.worker.DeadLetterSink;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * 死信 JDBC 实现(表 {@code consumer_dlq}):重试耗尽后落库并可由页面查询 / 标记重放。
 *
 * <p>原则:**绝不静默丢弃** —— 落死信同时由调用方告警。
 */
public class JdbcDeadLetterSink implements DeadLetterSink {

    private final DataSource ds;

    public JdbcDeadLetterSink(DataSource ds) { this.ds = ds; }

    @Override
    public void record(String taskId, String unitId, int partition, long offset, long tso,
                       DecodedMessage message, Throwable error) {
        String sql = "INSERT INTO consumer_dlq(task_id,unit_id,topic,partition_,offset_,tso,stream,op,error_msg) "
                + "VALUES(?,?,?,?,?,?,?,?,?)";
        try (Connection c = ds.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, taskId);
            ps.setString(2, unitId);
            ps.setString(3, unitId == null ? "" : unitId.replaceAll("^u-", ""));   // topic 由调用方补:见下面 setTopic
            ps.setInt(4, partition);
            ps.setLong(5, offset);
            if (tso <= 0) ps.setNull(6, java.sql.Types.BIGINT); else ps.setLong(6, tso);
            ps.setString(7, message == null ? null : message.stream());
            ps.setString(8, message == null || message.op() == null ? null : message.op().name());
            ps.setString(9, error == null ? null : String.valueOf(error.getMessage()));
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException("record dead letter failed", e);
        }
    }

    /** 查询某任务的死信(前端详情页用)。 */
    public java.util.List<java.util.Map<String, Object>> list(String taskId) {
        java.util.List<java.util.Map<String, Object>> out = new java.util.ArrayList<>();
        String sql = "SELECT dlq_id,task_id,unit_id,topic,partition_,offset_,tso,stream,op,error_msg,retries,"
                + "created_at,replayed_at FROM consumer_dlq WHERE task_id=? ORDER BY dlq_id DESC LIMIT 200";
        try (Connection c = ds.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, taskId);
            try (java.sql.ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    java.util.Map<String, Object> m = new java.util.LinkedHashMap<>();
                    m.put("dlqId", rs.getLong("dlq_id"));
                    m.put("unitId", rs.getString("unit_id"));
                    m.put("topic", rs.getString("topic"));
                    m.put("partition", rs.getInt("partition_"));
                    m.put("offset", rs.getLong("offset_"));
                    m.put("tso", rs.getLong("tso"));
                    m.put("stream", rs.getString("stream"));
                    m.put("op", rs.getString("op"));
                    m.put("error", rs.getString("error_msg"));
                    m.put("retries", rs.getInt("retries"));
                    m.put("createdAt", String.valueOf(rs.getTimestamp("created_at")));
                    m.put("replayedAt", String.valueOf(rs.getTimestamp("replayed_at")));
                    out.add(m);
                }
            }
        } catch (SQLException e) {
            throw new IllegalStateException("list dead letters failed", e);
        }
        return out;
    }

    /** 标记已重放(真正的重投由"把该分区位点回退到该 offset 并重启单元"完成,见详细设计返工流程)。 */
    public int markReplayed(long dlqId) {
        try (Connection c = ds.getConnection();
             PreparedStatement ps = c.prepareStatement(
                     "UPDATE consumer_dlq SET replayed_at=NOW() WHERE dlq_id=? AND replayed_at IS NULL")) {
            ps.setLong(1, dlqId);
            return ps.executeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException("mark replayed failed", e);
        }
    }

    /** 便捷重载:显式给 topic(推荐,便于排查)。 */
    public void record(String taskId, String unitId, String topic, int partition, long offset, long tso,
                       DecodedMessage message, Throwable error, int retries) {
        String sql = "INSERT INTO consumer_dlq(task_id,unit_id,topic,partition_,offset_,tso,stream,op,error_msg,retries) "
                + "VALUES(?,?,?,?,?,?,?,?,?,?)";
        try (Connection c = ds.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, taskId);
            ps.setString(2, unitId);
            ps.setString(3, topic);
            ps.setInt(4, partition);
            ps.setLong(5, offset);
            if (tso <= 0) ps.setNull(6, java.sql.Types.BIGINT); else ps.setLong(6, tso);
            ps.setString(7, message == null ? null : message.stream());
            ps.setString(8, message == null || message.op() == null ? null : message.op().name());
            ps.setString(9, error == null ? null : String.valueOf(error.getMessage()));
            ps.setInt(10, retries);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException("record dead letter failed", e);
        }
    }
}
