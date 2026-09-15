package com.dcp.outlet.adapters;

import com.dcp.outlet.control.HeartbeatStore;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/** 心跳落库(表 {@code consumer_unit}):最近心跳时间 + 最近位点/TSO。 */
public class JdbcHeartbeatStore implements HeartbeatStore {

    private final DataSource ds;

    public JdbcHeartbeatStore(DataSource ds) { this.ds = ds; }

    @Override
    public void record(String unitId, int partition, long offset, long tso, long at) {
        String sql = "UPDATE consumer_unit SET last_seen_at=NOW(), state='RUNNING', "
                + "last_offset=GREATEST(IFNULL(last_offset,0), ?), "
                + "last_tso=GREATEST(IFNULL(last_tso,0), ?) WHERE unit_id=?";
        try (Connection c = ds.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setLong(1, offset);
            ps.setLong(2, tso);
            ps.setString(3, unitId);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException("record heartbeat failed", e);
        }
    }
}
