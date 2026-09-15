package com.dcp.outlet.adapters;

import com.dcp.outlet.control.LogicCatalog;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * 控制面生产实现:从 {@code consumer_logic} 表判断逻辑是否已注册(建任务时校验)。
 *
 * <p>只判"**声明过**",不判"jar 在不在、哈希对不对" —— 后者是计算面加载时的事
 * (加载失败由计算面按单元上报,控制面只负责别让任务建在空气上)。
 */
public class JdbcLogicCatalog implements LogicCatalog {

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
}
