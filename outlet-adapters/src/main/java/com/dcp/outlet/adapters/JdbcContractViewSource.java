package com.dcp.outlet.adapters;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * 控制面侧:从 {@code contract} 表产出**契约视图**(供下发给计算面)。
 *
 * <p><b>重要</b>:视图里的 {@code allowedColumns} 必须是**「契约 ∩ 主体」**的结果 ——
 * 主体侧收窄由 {@link SubjectColumnPolicy} 端口完成(生产实现直接复用出口权限引擎;
 * 骨架实现返回全部可见)。
 * 若把"仅契约白名单"当作最终授权下发,就等于**放大权限** —— 这是安全红线。
 */
public class JdbcContractViewSource {

    /** 主体侧列级权限端口(接出口权限引擎 / DataSecurityService)。 */
    public interface SubjectColumnPolicy {
        /** true = 该列对当前主体隐藏,必须剔除 */
        boolean hidden(String consumerId, String stream, String column);

        static SubjectColumnPolicy permissive() { return (c, s, col) -> false; }
    }

    /** 视图(下发单元)。 */
    public static final class View {
        public final String contractId;
        public final String consumerId;
        public final String stream;
        public final Set<String> allowedColumns;
        public final Map<String, String> maskRules;
        public final int maxLevel;
        public final String rowCondition;
        public final boolean active;

        View(String contractId, String consumerId, String stream, Set<String> allowedColumns,
             Map<String, String> maskRules, int maxLevel, String rowCondition, boolean active) {
            this.contractId = contractId; this.consumerId = consumerId; this.stream = stream;
            this.allowedColumns = allowedColumns; this.maskRules = maskRules; this.maxLevel = maxLevel;
            this.rowCondition = rowCondition; this.active = active;
        }
    }

    private final DataSource ds;
    private final SubjectColumnPolicy subjectPolicy;

    public JdbcContractViewSource(DataSource ds, SubjectColumnPolicy subjectPolicy) {
        this.ds = ds; this.subjectPolicy = subjectPolicy;
    }

    public Optional<View> viewOf(String contractId, String stream) {
        String sql = "SELECT contract_id, consumer_id, streams, column_whitelist, row_condition, max_level, "
                + "mask_rules, status FROM contract WHERE contract_id=?";
        try (Connection c = ds.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, contractId);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) return Optional.empty();
                String consumerId = rs.getString("consumer_id");
                boolean active = "active".equalsIgnoreCase(rs.getString("status"));
                // 契约是否覆盖该流(streams 是 JSON 数组)
                if (!Json.readStringArray(rs.getString("streams")).contains(stream)) {
                    return Optional.of(new View(contractId, consumerId, stream, Set.of(), Map.of(),
                            rs.getInt("max_level"), rs.getString("row_condition"), false));
                }
                // 列白名单:column_whitelist 是 {stream: [cols]} → 用 MySQL JSON 函数取该流的数组
                Set<String> cols = new LinkedHashSet<>();
                String wlSql = "SELECT JSON_UNQUOTE(JSON_EXTRACT(column_whitelist, ?)) AS cols "
                        + "FROM contract WHERE contract_id=?";
                try (PreparedStatement wl = c.prepareStatement(wlSql)) {
                    wl.setString(1, "$.\"" + stream + "\"");
                    wl.setString(2, contractId);
                    try (ResultSet w = wl.executeQuery()) {
                        if (w.next()) cols.addAll(Json.readStringArray(w.getString("cols")));
                    }
                }
                // 主体侧收窄(契约 ∩ 主体)—— 缺了这步就是放大权限
                Set<String> visible = new LinkedHashSet<>();
                for (String col : cols) {
                    if (!subjectPolicy.hidden(consumerId, stream, col)) visible.add(col);
                }
                Map<String, String> masks = new LinkedHashMap<>(Json.readStringMap(rs.getString("mask_rules")));
                masks.keySet().retainAll(visible);          // 被剔除的列不再下发脱敏规则
                return Optional.of(new View(contractId, consumerId, stream, visible, masks,
                        rs.getInt("max_level"), rs.getString("row_condition"),
                        active));
            }
        } catch (SQLException e) {
            throw new IllegalStateException("contract view lookup failed", e);
        }
    }
}
