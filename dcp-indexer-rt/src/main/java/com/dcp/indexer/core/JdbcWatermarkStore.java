package com.dcp.indexer.core;

import com.dcp.indexer.core.Watermark;
import com.dcp.indexer.core.WatermarkStore;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/** 水位表 JDBC 实现(表 {@code chg_watermark_latest});写入用 GREATEST 保证**单调不回退**。 */
public class JdbcWatermarkStore implements WatermarkStore {

    private final DataSource ds;

    public JdbcWatermarkStore(DataSource ds) { this.ds = ds; }

    @Override
    public void upsert(Watermark w) {
        String sql = "INSERT INTO chg_watermark_latest(topic,partition_,epoch,watermark_ts,offset_,updated_at) "
                + "VALUES(?,?,?,?,?,NOW()) "
                + "ON DUPLICATE KEY UPDATE "
                + "  offset_ = IF(VALUES(watermark_ts) > watermark_ts, VALUES(offset_), offset_), "
                + "  watermark_ts = GREATEST(watermark_ts, VALUES(watermark_ts))";
        try (Connection c = ds.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, w.topic());
            ps.setInt(2, w.partition());
            ps.setInt(3, w.epoch());
            ps.setLong(4, w.watermarkTs());
            ps.setLong(5, w.offset());
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException("upsert watermark failed", e);
        }
    }

    @Override
    public Watermark latest(String topic, int partition, int epoch) {
        String sql = "SELECT watermark_ts,offset_,UNIX_TIMESTAMP(updated_at)*1000 FROM chg_watermark_latest "
                + "WHERE topic=? AND partition_=? AND epoch=?";
        try (Connection c = ds.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, topic);
            ps.setInt(2, partition);
            ps.setInt(3, epoch);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) return null;
                return new Watermark(topic, partition, epoch, rs.getLong(1), rs.getLong(2), rs.getLong(3));
            }
        } catch (SQLException e) {
            throw new IllegalStateException("watermark lookup failed", e);
        }
    }
}
