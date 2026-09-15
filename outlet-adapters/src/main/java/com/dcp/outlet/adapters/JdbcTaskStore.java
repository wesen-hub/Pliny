package com.dcp.outlet.adapters;

import com.dcp.outlet.control.FailureSpec;
import com.dcp.outlet.control.TaskRecord;
import com.dcp.outlet.control.TaskStatus;
import com.dcp.outlet.control.TaskStore;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;

/** 任务存储 JDBC 实现(表 {@code consumer_task});{@code uk_group} 保证一任务一消费组。 */
public class JdbcTaskStore implements TaskStore {

    private final DataSource ds;

    public JdbcTaskStore(DataSource ds) { this.ds = ds; }

    @Override
    public boolean exists(String taskId) {
        try (Connection c = ds.getConnection();
             PreparedStatement ps = c.prepareStatement("SELECT 1 FROM consumer_task WHERE task_id=?")) {
            ps.setString(1, taskId);
            try (ResultSet rs = ps.executeQuery()) { return rs.next(); }
        } catch (SQLException e) { throw new IllegalStateException("task exists failed", e); }
    }

    @Override
    public boolean groupExists(String consumerGroup) {
        try (Connection c = ds.getConnection();
             PreparedStatement ps = c.prepareStatement("SELECT 1 FROM consumer_task WHERE consumer_group=?")) {
            ps.setString(1, consumerGroup);
            try (ResultSet rs = ps.executeQuery()) { return rs.next(); }
        } catch (SQLException e) { throw new IllegalStateException("group exists failed", e); }
    }

    @Override
    public void insert(TaskRecord t) {
        String sql = "INSERT INTO consumer_task(task_id,name,consumer_type,owner,streams,logic_id,logic_version,"
                + "instances,contract_ref,start_policy,desired_state,consumer_group,epoch,start_tso,params,status) "
                + "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
        try (Connection c = ds.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, t.taskId());
            ps.setString(2, t.name());
            ps.setString(3, t.consumerType() == null ? "custom" : t.consumerType());
            ps.setString(4, t.owner());
            ps.setString(5, Json.write(t.streams()));
            ps.setString(6, t.logicId());
            ps.setString(7, t.logicVersion());
            ps.setInt(8, t.instances());
            ps.setString(9, t.contractRef());
            ps.setString(10, t.startPolicy());
            ps.setString(11, t.desiredState());
            ps.setString(12, t.consumerGroup());
            ps.setInt(13, t.epoch());
            if (t.startTso() == null) ps.setNull(14, Types.BIGINT); else ps.setLong(14, t.startTso());
            ps.setString(15, Json.write(t.params()));
            ps.setString(16, t.status().name());
            ps.executeUpdate();
        } catch (SQLException e) { throw new IllegalStateException("insert task failed", e); }
    }

    @Override
    public TaskRecord byId(String taskId) {
        String sql = "SELECT * FROM consumer_task WHERE task_id=?";
        try (Connection c = ds.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, taskId);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) return null;
                TaskRecord t = new TaskRecord(rs.getString("task_id"))
                        .name(rs.getString("name"))
                        .consumerType(rs.getString("consumer_type"))
                        .owner(rs.getString("owner"))
                        .logic(rs.getString("logic_id"), rs.getString("logic_version"))
                        .instances(rs.getInt("instances"))
                        .contractRef(rs.getString("contract_ref"))
                        .startPolicy(rs.getString("start_policy"))
                        .desiredState(rs.getString("desired_state"))
                        .consumerGroup(rs.getString("consumer_group"))
                        .epoch(rs.getInt("epoch"))
                        .status(TaskStatus.valueOf(rs.getString("status")));
                long tso = rs.getLong("start_tso");
                if (!rs.wasNull()) t.startTso(tso);
                Json.readStringArray(rs.getString("streams")).forEach(t::stream);
                return t;
            }
        } catch (SQLException e) { throw new IllegalStateException("task byId failed", e); }
    }

    @Override
    public void update(TaskRecord t) {
        String sql = "UPDATE consumer_task SET status=?, start_tso=?, logic_version=?, instances=?, desired_state=? "
                + "WHERE task_id=?";
        try (Connection c = ds.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, t.status().name());
            if (t.startTso() == null) ps.setNull(2, Types.BIGINT); else ps.setLong(2, t.startTso());
            ps.setString(3, t.logicVersion());
            ps.setInt(4, t.instances());
            ps.setString(5, t.desiredState());
            ps.setString(6, t.taskId());
            ps.executeUpdate();
        } catch (SQLException e) { throw new IllegalStateException("update task failed", e); }
    }

    public List<String> allTaskIds() {
        try (Connection c = ds.getConnection();
             PreparedStatement ps = c.prepareStatement("SELECT task_id FROM consumer_task ORDER BY create_time DESC");
             ResultSet rs = ps.executeQuery()) {
            java.util.List<String> out = new java.util.ArrayList<>();
            while (rs.next()) out.add(rs.getString(1));
            return out;
        } catch (SQLException e) { throw new IllegalStateException("list tasks failed", e); }
    }

    static FailureSpec failureOf(String dlqPolicy, String onFailure) {
        return new FailureSpec(5, 2000L,
                dlqPolicy == null ? "dlq" : dlqPolicy, onFailure == null ? "stop" : onFailure);
    }
}
