package com.dcp.outlet.adapters;

import com.dcp.outlet.control.LogicCatalog;
import com.dcp.outlet.control.LogicInfo;
import com.dcp.outlet.control.LogicPublishException;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * 控制面生产实现:从 {@code consumer_logic} 表判断逻辑是否已注册 / 登记 / 启停 / 审计。
 *
 * <p>只判"**声明过**",不判"jar 在不在、哈希对不对" —— 后者是计算面加载时的事
 * (加载失败由计算面按单元上报,控制面只负责别让任务建在空气上 / 已禁用的版本上)。
 */
public class JdbcLogicCatalog implements LogicCatalog {

    private static final String COLS =
            "logic_id, version, description, artifact_uri, artifact_hash, registered_at, "
                    + "status, published_by, jar_size, preflight";

    private final DataSource ds;

    public JdbcLogicCatalog(DataSource ds) { this.ds = ds; }

    @Override
    public boolean registered(String logicId, String version) {
        if (logicId == null || version == null) return false;
        String sql = "SELECT 1 FROM consumer_logic WHERE logic_id=? AND version=? LIMIT 1";
        try (Connection c = ds.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, logicId);
            ps.setString(2, version);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }
        } catch (SQLException e) {
            throw new IllegalStateException("逻辑目录查询失败:" + logicId + "@" + version, e);
        }
    }

    @Override
    public Optional<LogicInfo> find(String logicId, String version) {
        if (logicId == null || version == null) return Optional.empty();
        String sql = "SELECT " + COLS + " FROM consumer_logic WHERE logic_id=? AND version=?";
        try (Connection c = ds.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, logicId);
            ps.setString(2, version);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? Optional.of(row(rs)) : Optional.empty();
            }
        } catch (SQLException e) {
            throw new IllegalStateException("逻辑目录查询失败:" + logicId + "@" + version, e);
        }
    }

    @Override
    public List<LogicInfo> versionsOf(String logicId) {
        String sql = "SELECT " + COLS + " FROM consumer_logic WHERE logic_id=? ORDER BY registered_at DESC, version DESC";
        try (Connection c = ds.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, logicId);
            try (ResultSet rs = ps.executeQuery()) {
                List<LogicInfo> out = new ArrayList<>();
                while (rs.next()) out.add(row(rs));
                return out;
            }
        } catch (SQLException e) {
            throw new IllegalStateException("逻辑版本列表查询失败:" + logicId, e);
        }
    }

    @Override
    public List<LogicInfo> all() {
        String sql = "SELECT " + COLS + " FROM consumer_logic ORDER BY logic_id, registered_at DESC, version DESC";
        try (Connection c = ds.getConnection();
             PreparedStatement ps = c.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {
            List<LogicInfo> out = new ArrayList<>();
            while (rs.next()) out.add(row(rs));
            return out;
        } catch (SQLException e) {
            throw new IllegalStateException("逻辑列表查询失败", e);
        }
    }

    @Override
    public void register(LogicInfo info) {
        // 先判存在(友好提示),再以主键约束兜底(并发窗口)
        if (registered(info.logicId(), info.version())) {
            throw new LogicPublishException("版本已存在,禁止覆盖:" + info.logicId() + "@" + info.version()
                    + "(发错内容请发新版本)");
        }
        String sql = "INSERT INTO consumer_logic(logic_id, version, description, artifact_uri, artifact_hash, "
                + "registered_at, status, published_by, jar_size, preflight) VALUES(?,?,?,?,?,?,?,?,?,?)";
        try (Connection c = ds.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, info.logicId());
            ps.setString(2, info.version());
            ps.setString(3, info.description());
            ps.setString(4, info.artifactUri());
            ps.setString(5, info.artifactHash());
            ps.setTimestamp(6, new Timestamp(System.currentTimeMillis()));
            ps.setString(7, info.status() == null ? LogicInfo.STATUS_ACTIVE : info.status());
            ps.setString(8, info.publishedBy());
            if (info.jarSize() == null) ps.setNull(9, Types.BIGINT); else ps.setLong(9, info.jarSize());
            ps.setString(10, info.preflight());
            ps.executeUpdate();
        } catch (SQLException e) {
            if (isDuplicate(e)) {
                throw new LogicPublishException("版本已存在,禁止覆盖:" + info.logicId() + "@" + info.version(), e);
            }
            throw new IllegalStateException("逻辑登记失败:" + info, e);
        }
    }

    @Override
    public void setStatus(String logicId, String version, String status) {
        String sql = "UPDATE consumer_logic SET status=? WHERE logic_id=? AND version=?";
        try (Connection c = ds.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, status);
            ps.setString(2, logicId);
            ps.setString(3, version);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException("逻辑状态更新失败:" + logicId + "@" + version, e);
        }
    }

    @Override
    public void audit(String logicId, String version, String action, String operator, String detail) {
        String sql = "INSERT INTO logic_publish_audit(logic_id, version, action, operator, detail) VALUES(?,?,?,?,?)";
        try (Connection c = ds.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, logicId);
            ps.setString(2, version);
            ps.setString(3, action);
            ps.setString(4, operator);
            ps.setString(5, detail);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException("逻辑审计写入失败:" + logicId + "@" + version, e);
        }
    }

    private static LogicInfo row(ResultSet rs) throws SQLException {
        return LogicInfo.builder(rs.getString("logic_id"), rs.getString("version"))
                .description(rs.getString("description"))
                .artifactUri(rs.getString("artifact_uri"))
                .artifactHash(rs.getString("artifact_hash"))
                .registeredAt(ts(rs.getTimestamp("registered_at")))
                .status(rs.getString("status"))
                .publishedBy(rs.getString("published_by"))
                .jarSize(readLong(rs, "jar_size"))
                .preflight(rs.getString("preflight"))
                .build();
    }

    private static Long readLong(ResultSet rs, String col) throws SQLException {
        long v = rs.getLong(col);
        return rs.wasNull() ? null : v;
    }

    private static String ts(Timestamp t) {
        return t == null ? null : t.toLocalDateTime().toString();
    }

    private static boolean isDuplicate(SQLException e) {
        // MySQL 唯一键冲突 SQLState = 23000
        return e.getSQLState() != null && e.getSQLState().startsWith("23");
    }
}
