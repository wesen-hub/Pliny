package com.dcp.outlet.adapters;

import com.dcp.outlet.control.StreamInfo;
import com.dcp.outlet.control.StreamRegistry;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/** 流登记 JDBC 实现(表 {@code chg_stream})。 */
public class JdbcStreamRegistry implements StreamRegistry {

    private final DataSource ds;

    public JdbcStreamRegistry(DataSource ds) { this.ds = ds; }

    @Override
    public Optional<StreamInfo> get(String stream) {
        try (Connection c = ds.getConnection();
             PreparedStatement ps = c.prepareStatement("SELECT topic,partitions FROM chg_stream WHERE stream=?")) {
            ps.setString(1, stream);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) return Optional.empty();
                return Optional.of(new StreamInfo(stream, rs.getString(1), rs.getInt(2)));
            }
        } catch (SQLException e) {
            throw new IllegalStateException("stream lookup failed", e);
        }
    }

    @Override
    public List<StreamInfo> list() {
        List<StreamInfo> out = new ArrayList<>();
        try (Connection c = ds.getConnection();
             PreparedStatement ps = c.prepareStatement("SELECT stream,topic,partitions FROM chg_stream ORDER BY stream");
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) out.add(new StreamInfo(rs.getString(1), rs.getString(2), rs.getInt(3)));
        } catch (SQLException e) {
            throw new IllegalStateException("stream list failed", e);
        }
        return out;
    }

    @Override
    public void register(StreamInfo info) {
        String sql = "INSERT INTO chg_stream(stream,topic,partitions) VALUES(?,?,?) "
                + "ON DUPLICATE KEY UPDATE topic=VALUES(topic), partitions=VALUES(partitions)";
        try (Connection c = ds.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, info.stream());
            ps.setString(2, info.topic());
            ps.setInt(3, info.partitions());
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException("stream register failed", e);
        }
    }
}
