package com.dcp.engine;

import com.dcp.common.BizException;
import com.dcp.common.ErrorCode;
import com.dcp.module.datasource.DsType;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * 增量两段式规划器（设计文档 §4.2 K3）
 * ① 探测 MAX(inc) 锁定窗口上界 m（只扫 > L0 的数据）
 * ② 生成窗口查询 SQL (L0, m]，首跑 L0=null 只有上界
 * 轻量引擎与 SeaTunnel 引擎共用
 */
public final class IncrementalPlanner {

    private IncrementalPlanner() {
    }

    @Data
    @AllArgsConstructor
    public static class PlanResult {
        /** 实际执行的抽取 SQL（增量已带窗口过滤） */
        private String selectSql;
        /** 窗口上界 m（成功后用于推进位点） */
        private String windowMax;
        /** 无新数据 */
        private boolean noData;
        /** 是否已有下界 */
        private boolean hasL0;
    }

    public static PlanResult plan(JobRunContext ctx, DsType st, String baseSql) throws SQLException {
        String incField = ctx.getJob().getIncrementalField();
        boolean incremental = "incremental".equalsIgnoreCase(ctx.getJob().getSyncMode());
        if (!incremental) {
            return new PlanResult(baseSql, null, false, false);
        }
        if (incField == null || incField.isBlank()) {
            throw new BizException(ErrorCode.JOB_INC_FIELD_MISSING, "增量模式必须配置增量字段");
        }
        try (Connection sourceConn = openConn(ctx.getSourceConn())) {
            String l0 = ctx.getSyncState() == null ? null : ctx.getSyncState().getLastSyncValue();
            boolean hasL0 = l0 != null && !l0.isBlank();
            String probeSql = probeSql(st, baseSql, incField, hasL0);
            List<Object> probeParams = new ArrayList<>();
            if (hasL0) {
                probeParams.add(bindValue(l0, ctx.getSyncState().getValueType()));
            }
            Object m = queryScalar(sourceConn, probeSql, probeParams);
            String windowMax = stringify(m);
            if (m == null) {
                ctx.getInfoLog().accept("增量探测: MAX(" + incField + ") 为空, 无新数据, 本次跳过引擎执行");
                return new PlanResult(null, null, true, hasL0);
            }
            ctx.getInfoLog().accept("增量探测: L0=" + l0 + ", 窗口上界 m=" + windowMax);
            // SeaTunnel Jdbc source 不支持参数化 query, 窗口过滤需渲染为字面量 SQL
            String selectSql = renderWindowSql(st, baseSql, incField, l0, windowMax);
            return new PlanResult(selectSql, windowMax, false, hasL0);
        }
    }

    /** 窗口 SQL（字面量渲染, 供 SeaTunnel 直接执行）: (L0, m] 或全量首跑 (0, m] */
    static String renderWindowSql(DsType st, String sql, String incField, String l0, String m) {
        String s = sql.trim().endsWith(";") ? sql.trim().substring(0, sql.trim().length() - 1) : sql.trim();
        String f = st.quote(incField);
        String inner = "SELECT * FROM (" + s + ") _t";
        if (l0 != null && !l0.isBlank()) {
            return inner + " WHERE " + f + " > " + literal(l0) + " AND " + f + " <= " + literal(m);
        }
        return inner + " WHERE " + f + " <= " + literal(m);
    }

    /** 增量字段字面量: 数字原样, 其余加引号（防注入: 单引号转义） */
    static String literal(String v) {
        if (v == null) return "NULL";
        if (v.matches("-?\\d+(\\.\\d+)?")) return v;
        return "'" + v.replace("'", "''") + "'";
    }

    static String probeSql(DsType st, String baseSql, String incField, boolean withLower) {
        String f = st.quote(incField);
        String where = withLower ? " WHERE " + f + " > ?" : "";
        if (baseSql.toLowerCase().startsWith("select * from")) {
            String table = baseSql.substring("select * from".length()).trim();
            return "SELECT MAX(" + f + ") FROM " + table + where;
        }
        return "SELECT MAX(" + f + ") FROM (" + baseSql + ") _t" + where;
    }

    static Object bindValue(String value, String valueType) {
        if ("DATETIME".equalsIgnoreCase(valueType)) {
            return Timestamp.valueOf(value);
        }
        if ("STRING".equalsIgnoreCase(valueType)) {
            return value;
        }
        return Long.parseLong(value);
    }

    static String stringify(Object m) {
        if (m == null) return null;
        if (m instanceof Timestamp ts) {
            return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(ts);
        }
        if (m instanceof java.util.Date d) {
            return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(d);
        }
        return String.valueOf(m);
    }

    private static Object queryScalar(Connection conn, String sql, List<Object> params) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            int idx = 1;
            for (Object p : params) {
                ps.setObject(idx++, p);
            }
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    return rs.getObject(1);
                }
                return null;
            }
        }
    }

    static Connection openConn(java.util.Map<String, String> conn) throws SQLException {
        Properties props = new Properties();
        props.setProperty("user", conn.get("username"));
        props.setProperty("password", conn.get("password"));
        props.setProperty("connectTimeout", "10000");
        props.setProperty("socketTimeout", "120000");
        return DriverManager.getConnection(conn.get("url"), props);
    }
}
