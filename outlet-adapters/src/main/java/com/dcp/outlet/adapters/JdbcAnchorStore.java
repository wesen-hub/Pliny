package com.dcp.outlet.adapters;

import com.dcp.outlet.control.Anchor;
import com.dcp.outlet.control.AnchorStore;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * 锚点表 JDBC 实现(表 {@code chg_anchor})。
 *
 * <p>栅栏查询由索引 {@code idx_fence_tso(topic, partition_, epoch, fence, commit_ts)} 支撑:
 * {@code WHERE ... AND fence=1 AND commit_ts <= ? ORDER BY commit_ts DESC LIMIT 1}。
 */
public class JdbcAnchorStore implements AnchorStore {

    private final DataSource ds;

    public JdbcAnchorStore(DataSource ds) { this.ds = ds; }

    @Override
    public void save(Anchor a) {
        String sql = "INSERT INTO chg_anchor(topic,partition_,epoch,offset_,commit_ts,wall_time,fence) "
                + "VALUES(?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE "
                + "commit_ts=VALUES(commit_ts), wall_time=VALUES(wall_time), fence=VALUES(fence)";
        try (Connection c = ds.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, a.topic());
            ps.setInt(2, a.partition());
            ps.setInt(3, a.epoch());
            ps.setLong(4, a.offset());
            ps.setLong(5, a.commitTs());
            ps.setLong(6, a.wallTime());
            ps.setBoolean(7, a.fence());
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException("save anchor failed: " + a, e);
        }
    }

    @Override
    public Anchor lastFenceAtOrBefore(String topic, int partition, int epoch, long tso) {
        return lookup(topic, partition, epoch, tso, "AND fence=1");
    }

    @Override
    public Anchor lastAtOrBefore(String topic, int partition, int epoch, long tso) {
        return lookup(topic, partition, epoch, tso, "");
    }

    private Anchor lookup(String topic, int partition, int epoch, long tso, String extraCond) {
        String sql = "SELECT offset_,commit_ts,wall_time,fence FROM chg_anchor "
                + "WHERE topic=? AND partition_=? AND epoch=? AND commit_ts<=? " + extraCond + " "
                + "ORDER BY commit_ts DESC LIMIT 1";
        try (Connection c = ds.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, topic);
            ps.setInt(2, partition);
            ps.setInt(3, epoch);
            ps.setLong(4, tso);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) return null;
                return new Anchor(topic, partition, epoch, rs.getLong("offset_"),
                        rs.getLong("commit_ts"), rs.getLong("wall_time"), rs.getBoolean("fence"));
            }
        } catch (SQLException e) {
            throw new IllegalStateException("anchor lookup failed", e);
        }
    }

    @Override
    public int prune(long before) {
        try (Connection c = ds.getConnection();
             PreparedStatement ps = c.prepareStatement("DELETE FROM chg_anchor WHERE wall_time < ?")) {
            ps.setLong(1, before);
            return ps.executeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException("prune anchors failed", e);
        }
    }

    @Override
    public List<Anchor> list(String topic, int partition, int epoch) {
        List<Anchor> out = new ArrayList<>();
        String sql = "SELECT offset_,commit_ts,wall_time,fence FROM chg_anchor "
                + "WHERE topic=? AND partition_=? AND epoch=? ORDER BY offset_";
        try (Connection c = ds.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, topic);
            ps.setInt(2, partition);
            ps.setInt(3, epoch);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    out.add(new Anchor(topic, partition, epoch, rs.getLong("offset_"),
                            rs.getLong("commit_ts"), rs.getLong("wall_time"), rs.getBoolean("fence")));
                }
            }
        } catch (SQLException e) {
            throw new IllegalStateException("list anchors failed", e);
        }
        return out;
    }
}
