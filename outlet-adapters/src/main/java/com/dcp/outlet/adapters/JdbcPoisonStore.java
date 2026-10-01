package com.dcp.outlet.adapters;

import com.dcp.outlet.control.Poison;
import com.dcp.outlet.control.PoisonStore;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * 索引器分区污染死信 JDBC 实现(表 {@code chg_anchor_poison})。
 *
 * <p>C1:解析失败后该分区不再产栅栏(查询退化 fallback);记录跨进程重启成立,
 * 人工介入确认后 DELETE 该行恢复采样 —— 与 consumer_dlq 同为"绝不静默丢弃"的死信语义。
 */
public class JdbcPoisonStore implements PoisonStore {

    private final DataSource ds;

    public JdbcPoisonStore(DataSource ds) { this.ds = ds; }

    @Override
    public void mark(String topic, int partition, int epoch) {
        String sql = "INSERT IGNORE INTO chg_anchor_poison(topic,partition_,epoch) VALUES(?,?,?)";
        try (Connection c = ds.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, topic);
            ps.setInt(2, partition);
            ps.setInt(3, epoch);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException("mark poison failed: " + topic + "#" + partition + "@" + epoch, e);
        }
    }

    @Override
    public boolean poisoned(String topic, int partition, int epoch) {
        String sql = "SELECT 1 FROM chg_anchor_poison WHERE topic=? AND partition_=? AND epoch=?";
        try (Connection c = ds.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, topic);
            ps.setInt(2, partition);
            ps.setInt(3, epoch);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }
        } catch (SQLException e) {
            throw new IllegalStateException("query poison failed", e);
        }
    }

    @Override
    public List<Poison> list() {
        List<Poison> out = new ArrayList<>();
        String sql = "SELECT topic,partition_,epoch FROM chg_anchor_poison";
        try (Connection c = ds.getConnection();
             PreparedStatement ps = c.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                out.add(new Poison(rs.getString("topic"), rs.getInt("partition_"), rs.getInt("epoch")));
            }
        } catch (SQLException e) {
            throw new IllegalStateException("list poisons failed", e);
        }
        return out;
    }
}
