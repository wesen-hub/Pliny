package com.dcp.outlet.adapters;

import com.dcp.outlet.control.UnitStatusStore;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * 单元运行状态来源(表 {@code consumer_unit}):供进度查询与巡检器使用。
 *
 * <p>{@code last_seen_at} 由计算面心跳上报,{@code dispatched_at} 由下发时写入。
 */
public class JdbcUnitStatusStore implements UnitStatusStore {

    private final DataSource ds;

    public JdbcUnitStatusStore(DataSource ds) { this.ds = ds; }

    private static final String COLS = "u.unit_id, u.task_id, u.state, u.last_offset, u.last_tso, "
            + "IFNULL(UNIX_TIMESTAMP(u.last_seen_at)*1000,0) AS seen, "
            + "IFNULL(UNIX_TIMESTAMP(u.dispatched_at)*1000,0) AS disp";

    @Override
    public List<UnitStatus> byTask(String taskId) {
        List<UnitStatus> out = new ArrayList<>();
        String sql = "SELECT " + COLS + " FROM consumer_unit u WHERE u.task_id=?";
        try (Connection c = ds.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, taskId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) out.add(map(rs));
            }
        } catch (SQLException e) { throw new IllegalStateException("unit status byTask failed", e); }
        return out;
    }

    @Override
    public List<UnitStatus> all() {
        List<UnitStatus> out = new ArrayList<>();
        String sql = "SELECT " + COLS + " FROM consumer_unit u";
        try (Connection c = ds.getConnection(); PreparedStatement ps = c.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) out.add(map(rs));
        } catch (SQLException e) { throw new IllegalStateException("unit status all failed", e); }
        return out;
    }

    private UnitStatus map(ResultSet rs) throws SQLException {
        return new UnitStatus(rs.getString("unit_id"), rs.getString("task_id"), rs.getString("state"),
                rs.getLong("last_offset"), rs.getLong("last_tso"), rs.getLong("seen"), rs.getLong("disp"));
    }
}
