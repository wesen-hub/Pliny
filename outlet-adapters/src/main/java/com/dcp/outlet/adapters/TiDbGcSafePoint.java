package com.dcp.outlet.adapters;

import com.dcp.outlet.control.T0Selector;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.function.Supplier;

/**
 * R2 适配器:TiDB GC safe point({@code mysql.tidb.tikv_gc_safe_point})。
 *
 * <p>该值以**时间字符串**存储(如 {@code 20260914-07:15:13.164 +0000},UTC),
 * 而 T0 三规则统一按 TSO 比较,故转成 TSO:物理毫秒 {@code << 18}(逻辑位=0)。
 * 比 chg-status.sh 的"截断到秒"更精确;毫秒级边界误差由快照读的 fail-safe(读失败 → 重做批)兜底。
 *
 * <p>取不到 / 解析失败 → 抛异常 → 批阶段失败。**绝不静默跳过 R2** —— 那等于允许读已被 GC 回收的版本。
 */
public class TiDbGcSafePoint implements T0Selector.GcSafePoint {

    private static final DateTimeFormatter FMT =
            DateTimeFormatter.ofPattern("yyyyMMdd-HH:mm:ss[.SSS]");

    private final Supplier<String> rawValue;

    public TiDbGcSafePoint(DataSource ds) {
        this.rawValue = () -> queryRaw(ds);
    }

    /** 测试注入原始值(绕过 JDBC)。 */
    TiDbGcSafePoint(Supplier<String> rawValue) {
        this.rawValue = rawValue;
    }

    @Override
    public long gcSafePointTs() {
        String raw = rawValue.get();
        if (raw == null || raw.isBlank()) {
            throw new IllegalStateException("TiDB GC safe point 为空,拒绝定 T0(R2)");
        }
        return toTso(raw);
    }

    private static String queryRaw(DataSource ds) {
        String sql = "SELECT variable_value FROM mysql.tidb WHERE variable_name='tikv_gc_safe_point'";
        try (Connection c = ds.getConnection();
             PreparedStatement ps = c.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {
            if (!rs.next()) throw new IllegalStateException("TiDB 无 tikv_gc_safe_point");
            return rs.getString(1);
        } catch (SQLException e) {
            throw new IllegalStateException("查询 TiDB GC safe point 失败", e);
        }
    }

    /** 纯转换(便于离线自检):时间字符串 → TSO。形如 {@code 20260914-07:15:13.164 +0000}。 */
    static long toTso(String raw) {
        String s = raw.trim();
        int sp = s.indexOf(' ');
        String body = sp >= 0 ? s.substring(0, sp) : s;   // 去掉 " +0000" 偏移段
        LocalDateTime ldt = LocalDateTime.parse(body, FMT);
        long ms = ldt.toInstant(ZoneOffset.UTC).toEpochMilli();
        return ms << 18;
    }
}
