package com.dcp.outlet.adapters;

import com.dcp.outlet.control.FailureSpec;
import com.dcp.outlet.control.TaskRecord;
import com.dcp.outlet.control.TaskStore;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 任务存储 JDBC 实现(表 {@code consumer_task});{@code uk_group} 保证一任务一消费组。
 *
 * <p><b>纯业务登记</b>(彻底版):调度状态/批起点/规划代/并发全部由统一调度框架承担,
 * 本表只存取业务事实,不再读写任何调度状态列。
 */
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
        String sql = "INSERT INTO consumer_task(task_id,name,consumer_type,namespace,owner,streams,logic_id,logic_version,"
                + "contract_ref,start_policy,desired_state,consumer_group,params,incremental_engine) "
                + "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
        try (Connection c = ds.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, t.taskId());
            ps.setString(2, t.name());
            ps.setString(3, t.consumerType() == null ? "custom" : t.consumerType());
            ps.setString(4, t.namespace());
            ps.setString(5, t.owner());
            ps.setString(6, Json.write(t.streams()));
            ps.setString(7, t.logicId());
            ps.setString(8, t.logicVersion());
            ps.setString(9, t.contractRef());
            ps.setString(10, t.startPolicy());
            ps.setString(11, t.desiredState());
            ps.setString(12, t.consumerGroup());
            ps.setString(13, Json.write(t.params()));
            ps.setString(14, t.incrementalEngine() == null ? "unit" : t.incrementalEngine());
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
                        .namespace(rs.getString("namespace"))
                        .owner(rs.getString("owner"))
                        .logic(rs.getString("logic_id"), rs.getString("logic_version"))
                        .contractRef(rs.getString("contract_ref"))
                        .startPolicy(rs.getString("start_policy"))
                        .desiredState(rs.getString("desired_state"))
                        .consumerGroup(rs.getString("consumer_group"))
                        .incrementalEngine(rs.getString("incremental_engine"));
                Json.readStringArray(rs.getString("streams")).forEach(t::stream);
                Object p = Json.readValue(rs.getString("params"));
                if (p instanceof Map<?, ?> pm) t.params().putAll((Map<String, Object>) pm);
                return t;
            }
        } catch (SQLException e) { throw new IllegalStateException("task byId failed", e); }
    }

    @Override
    public void update(TaskRecord t) {
        String sql = "UPDATE consumer_task SET logic_version=?, desired_state=? WHERE task_id=?";
        try (Connection c = ds.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, t.logicVersion());
            ps.setString(2, t.desiredState());
            ps.setString(3, t.taskId());
            ps.executeUpdate();
        } catch (SQLException e) { throw new IllegalStateException("update task failed", e); }
    }

    public List<String> allTaskIds() {
        try (Connection c = ds.getConnection();
             PreparedStatement ps = c.prepareStatement("SELECT task_id FROM consumer_task ORDER BY create_time DESC");
             ResultSet rs = ps.executeQuery()) {
            List<String> out = new ArrayList<>();
            while (rs.next()) out.add(rs.getString(1));
            return out;
        } catch (SQLException e) { throw new IllegalStateException("list tasks failed", e); }
    }

    static FailureSpec failureOf(String dlqPolicy, String onFailure) {
        return new FailureSpec(5, 2000L,
                dlqPolicy == null ? "dlq" : dlqPolicy, onFailure == null ? "stop" : onFailure);
    }
}
