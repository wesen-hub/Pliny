package com.dcp.outlet.adapters;

import com.dcp.outlet.control.PlannedUnit;
import com.dcp.outlet.control.UnitStore;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * 单元存储 JDBC 实现(表 {@code consumer_unit} + {@code consumer_unit_partition})。
 *
 * <p>{@code consumer_unit_partition} 的主键 {@code (topic, partition_, epoch)}
 * 由数据库保证**同分区同代只能被一个单元认领** —— 防双消费的最后一道闸。
 */
public class JdbcUnitStore implements UnitStore {

    private final DataSource ds;

    public JdbcUnitStore(DataSource ds) { this.ds = ds; }

    @Override
    public void insert(String taskId, List<PlannedUnit> units) {
        String unitSql = "INSERT INTO consumer_unit(unit_id,task_id,topic,stream,epoch,state,partitions) "
                + "VALUES(?,?,?,?,?, 'PENDING', ?) ON DUPLICATE KEY UPDATE partitions=VALUES(partitions)";
        String partSql = "INSERT INTO consumer_unit_partition(unit_id,task_id,topic,partition_,epoch) "
                + "VALUES(?,?,?,?,?)";
        try (Connection c = ds.getConnection()) {
            c.setAutoCommit(false);
            try (PreparedStatement u = c.prepareStatement(unitSql);
                 PreparedStatement p = c.prepareStatement(partSql)) {
                for (PlannedUnit pu : units) {
                    int epoch = epochOf(pu.unitId());
                    u.setString(1, pu.unitId());
                    u.setString(2, taskId);
                    u.setString(3, pu.topic());
                    u.setString(4, pu.stream());
                    u.setInt(5, epoch);
                    u.setString(6, pu.partitions().stream().map(String::valueOf).collect(Collectors.joining(",")));
                    u.addBatch();
                    for (int part : pu.partitions()) {
                        p.setString(1, pu.unitId());
                        p.setString(2, taskId);
                        p.setString(3, pu.topic());
                        p.setInt(4, part);
                        p.setInt(5, epoch);
                        p.addBatch();
                    }
                }
                u.executeBatch();
                p.executeBatch();
                c.commit();
            } catch (SQLException e) {
                c.rollback();
                throw e;
            }
        } catch (SQLException e) {
            throw new IllegalStateException("insert units failed(可能触发同分区同代唯一约束 → 防双消费)", e);
        }
    }

    @Override
    public List<PlannedUnit> byTask(String taskId) {
        List<PlannedUnit> out = new ArrayList<>();
        String sql = "SELECT unit_id,topic,stream,partitions FROM consumer_unit WHERE task_id=? ORDER BY unit_id";
        try (Connection c = ds.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, taskId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    List<Integer> parts = new ArrayList<>();
                    for (String s : rs.getString("partitions").split(",")) {
                        if (!s.isBlank()) parts.add(Integer.parseInt(s.trim()));
                    }
                    out.add(new PlannedUnit(rs.getString("unit_id"), rs.getString("stream"),
                            rs.getString("topic"), parts));
                }
            }
        } catch (SQLException e) { throw new IllegalStateException("list units failed", e); }
        return out;
    }

    @Override
    public void markDispatched(String unitId) {
        try (Connection c = ds.getConnection();
             PreparedStatement ps = c.prepareStatement(
                     "UPDATE consumer_unit SET state='DISPATCHED' WHERE unit_id=?")) {
            ps.setString(1, unitId);
            ps.executeUpdate();
        } catch (SQLException e) { throw new IllegalStateException("markDispatched failed", e); }
    }

    @Override
    public String stateOf(String unitId) {
        try (Connection c = ds.getConnection();
             PreparedStatement ps = c.prepareStatement("SELECT state FROM consumer_unit WHERE unit_id=?")) {
            ps.setString(1, unitId);
            try (ResultSet rs = ps.executeQuery()) { return rs.next() ? rs.getString(1) : null; }
        } catch (SQLException e) { throw new IllegalStateException("stateOf failed", e); }
    }

    private static int epochOf(String unitId) {
        int i = unitId.lastIndexOf('-');
        try { return Integer.parseInt(unitId.substring(i + 1)); } catch (Exception e) { return 1; }
    }
}
