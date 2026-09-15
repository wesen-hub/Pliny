package com.dcp.outlet.adapters;

import com.dcp.outlet.control.ContractInfo;
import com.dcp.outlet.control.ContractLookup;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedHashSet;
import java.util.Optional;
import java.util.Set;

/**
 * 控制面生产实现:从**出口模块的 `contract` 表**读契约摘要(建任务时校验用)。
 *
 * <p>只在**建任务**时查一次(不是热路径):契约不存在 / 未生效 / 未覆盖流 → 任务直接建不出来
 * (与 {@code JdbcContractViewSource} 同一个表、同一口径,避免"建任务时放行、下发时才发现没契约")。
 */
public class JdbcContractLookup implements ContractLookup {

    private final DataSource ds;

    public JdbcContractLookup(DataSource ds) { this.ds = ds; }

    @Override
    public Optional<ContractInfo> find(String contractRef) {
        if (contractRef == null || contractRef.isBlank()) return Optional.empty();
        String sql = "SELECT contract_id, streams, status FROM contract WHERE contract_id=?";
        try (Connection c = ds.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, contractRef);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) return Optional.empty();
                Set<String> streams = new LinkedHashSet<>(Json.readStringArray(rs.getString("streams")));
                boolean active = "active".equalsIgnoreCase(rs.getString("status"));
                return Optional.of(new ContractInfo(rs.getString("contract_id"), active, streams));
            }
        } catch (SQLException e) {
            throw new IllegalStateException("契约查询失败:" + contractRef, e);
        }
    }
}
