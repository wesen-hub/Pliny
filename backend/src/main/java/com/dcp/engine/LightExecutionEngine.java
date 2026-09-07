package com.dcp.engine;

import com.dcp.common.BizException;
import com.dcp.common.ErrorCode;
import com.dcp.module.datasource.DsType;
import com.googlecode.aviator.AviatorEvaluatorInstance;
import com.googlecode.aviator.Expression;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.sql.*;
import java.util.*;

/**
 * 轻量执行引擎（设计文档 §4.1 ~ §4.4 的本地化实现）
 * - 全量/增量两段式执行：先探测 MAX 锁定窗口 (L0, m]，成功后推进位点
 * - 转换链：direct 重命名 → dictionary 字典 → expression Aviator，按 sort_no 串联
 * - 目标端 REPLACE INTO 批量幂等写入
 * 生产环境可替换为 SeaTunnel Zeta（ExecutionEngine 接口预留）
 */
@Slf4j
@Component
@ConditionalOnProperty(name = "dcp.engine.type", havingValue = "light", matchIfMissing = true)
public class LightExecutionEngine implements SyncEngine {

    @Value("${dcp.engine.batch-size:1000}")
    private int batchSize;

    @Value("${dcp.engine.fetch-size:1000}")
    private int fetchSize;

    /** 执行一次任务，返回源/目标行数与增量窗口上界 */
    @Override
    public EngineResult execute(JobRunContext ctx) throws Exception {
        DsType st = DsType.of(ctx.getSourceConn().get("type"));
        DsType tt = DsType.of(ctx.getTargetConn().get("type"));
        if (st == DsType.MONGODB) {
            throw new BizException(ErrorCode.JOB_CONFIG_INCOMPLETE, "轻量引擎暂不支持 MongoDB 源, 请使用 SeaTunnel 引擎");
        }
        String syncMode = ctx.getJob().getSyncMode();
        if ("cdc".equalsIgnoreCase(syncMode)) {
            throw new BizException(ErrorCode.JOB_CONFIG_INCOMPLETE, "CDC 模式需要 SeaTunnel Zeta 引擎(checkpoint), 轻量版暂不支持");
        }

        String selectSql;
        List<Object> selectParams = new ArrayList<>();
        String windowMax = null;
        boolean noData = false;

        try (Connection sourceConn = IncrementalPlanner.openConn(ctx.getSourceConn())) {
            String baseSql = buildBaseSql(ctx, st);
            IncrementalPlanner.PlanResult plan = IncrementalPlanner.plan(ctx, st, baseSql);
            selectSql = plan.getSelectSql();
            windowMax = plan.getWindowMax();
            noData = plan.isNoData();
            if (noData) {
                return new EngineResult(0, 0, null, true);
            }
            // 窗口参数: [L0?, m]
            String l0 = ctx.getSyncState() == null ? null : ctx.getSyncState().getLastSyncValue();
            boolean hasL0 = l0 != null && !l0.isBlank();
            if (hasL0) {
                selectParams.add(IncrementalPlanner.bindValue(l0, ctx.getSyncState().getValueType()));
            }
            if (plan.getWindowMax() != null) {
                selectParams.add(IncrementalPlanner.bindValue(plan.getWindowMax(),
                        ctx.getSyncState() == null ? "LONG" : ctx.getSyncState().getValueType()));
            }

            // 执行抽取 + 转换 + 写入
            long sourceCount = 0;
            long targetCount = 0;
            List<String> targetCols = ctx.getMappings().stream()
                    .sorted(Comparator.comparing(m -> m.getSortNo() == null ? 0 : m.getSortNo()))
                    .map(com.dcp.module.job.entity.FieldMapping::getTargetField).toList();
            String upsertSql = tt.upsertSql(ctx.getJob().getTargetTableName(), targetCols);

            Map<String, Map<String, String>> dictMaps = ctx.getDictMaps() == null ? Map.of() : ctx.getDictMaps();
            AviatorEvaluatorInstance aviator = ExpressionUtil.newSafeEvaluator(dictMaps);
            Map<String, Expression> compiledExprs = new LinkedHashMap<>();
            for (var m : ctx.getMappings()) {
                if ("expression".equals(m.getTransformType()) && m.getTransformRule() != null && !m.getTransformRule().isBlank()) {
                    compiledExprs.put(m.getTargetField(), aviator.compile(m.getTransformRule()));
                }
            }

            try (Connection targetConn = openConn(ctx.getTargetConn());
                 PreparedStatement ps = targetConn.prepareStatement(upsertSql);
                 PreparedStatement selectPs = sourceConn.prepareStatement(selectSql)) {
                int pIdx = 1;
                for (Object p : selectParams) {
                    selectPs.setObject(pIdx++, p);
                }
                selectPs.setFetchSize(fetchSize);
                selectPs.setQueryTimeout((int) Math.min(ctx.getTimeoutMillis() / 1000, 600));
                try (ResultSet rs = selectPs.executeQuery()) {
                    ResultSetMetaData md = rs.getMetaData();
                    int colCount = md.getColumnCount();
                    String[] labels = new String[colCount];
                    for (int i = 0; i < colCount; i++) {
                        labels[i] = md.getColumnLabel(i + 1);
                    }
                    long deadline = System.currentTimeMillis() + ctx.getTimeoutMillis();
                    int batch = 0;
                    while (rs.next()) {
                        checkStop(ctx, deadline);
                        Map<String, Object> row = new LinkedHashMap<>();
                        for (int i = 0; i < colCount; i++) {
                            row.put(labels[i], rs.getObject(i + 1));
                        }
                        Map<String, Object> out = transformRow(ctx, row, dictMaps, aviator, compiledExprs);
                        int idx = 1;
                        for (String col : targetCols) {
                            Object v = out.get(col);
                            ps.setObject(idx++, v);
                        }
                        ps.addBatch();
                        sourceCount++;
                        if (++batch >= batchSize) {
                            targetCount += executeBatch(ps);
                            batch = 0;
                        }
                    }
                    if (batch > 0) {
                        targetCount += executeBatch(ps);
                    }
                }
            }
            ctx.getInfoLog().accept("执行完成: 读取 " + sourceCount + " 行, 写入 " + targetCount + " 行");
            return new EngineResult(sourceCount, targetCount, windowMax, false);
        }
    }

    private int executeBatch(PreparedStatement ps) throws SQLException {
        int[] r = ps.executeBatch();
        int n = 0;
        for (int x : r) {
            // SUCCESS_NO_INFO(-2) 等负值表示行数未知, 按 1 计
            n += x >= 0 ? x : 1;
        }
        return n;
    }

    private String buildBaseSql(JobRunContext ctx, DsType st) {
        String content = ctx.getExtract().getContent().trim();
        if ("table".equalsIgnoreCase(ctx.getExtract().getSourceType())) {
            return "SELECT * FROM " + st.quote(content);
        }
        if (!content.toLowerCase().startsWith("select")) {
            throw new BizException(ErrorCode.SQL_VALIDATE_FAIL, "抽取 SQL 必须以 SELECT 开头");
        }
        return content;
    }

    private String probeSql(DsType st, String baseSql, String incField, boolean withLower) {
        return IncrementalPlanner.probeSql(st, baseSql, incField, withLower);
    }

    private Object bindValue(String value, String valueType) {
        return IncrementalPlanner.bindValue(value, valueType);
    }

    /** 结果值字符串化（位点存储格式） */
    private String stringify(Object m) {
        return IncrementalPlanner.stringify(m);
    }

    private Object queryScalar(Connection conn, String sql, List<Object> params) throws SQLException {
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

    /** 转换链: direct -> dictionary -> expression（按 sort_no 顺序，后者可引用前者输出） */
    private Map<String, Object> transformRow(JobRunContext ctx, Map<String, Object> row,
                                             Map<String, Map<String, String>> dictMaps,
                                             AviatorEvaluatorInstance aviator,
                                             Map<String, Expression> compiledExprs) {
        Map<String, Object> out = new LinkedHashMap<>();
        Map<String, Object> env = new HashMap<>();
        row.forEach((k, v) -> env.put(k, ExpressionUtil.normalizeForAviator(v)));

        for (var m : ctx.getMappings().stream()
                .sorted(Comparator.comparing(x -> x.getSortNo() == null ? 0 : x.getSortNo())).toList()) {
            String type = m.getTransformType() == null ? "direct" : m.getTransformType();
            Object value;
            switch (type) {
                case "dictionary" -> {
                    Map<String, String> dict = dictMaps.get(m.getTransformRule());
                    Object raw = row.get(m.getSourceField());
                    value = raw;
                    if (dict != null && raw != null) {
                        String mapped = dict.get(String.valueOf(raw));
                        value = mapped != null ? mapped : raw;
                    }
                }
                case "expression" -> {
                    Expression expr = compiledExprs.get(m.getTargetField());
                    Object v = null;
                    if (expr != null) {
                        v = expr.execute(env);
                        // 自定义函数(如 date.format) 顶层结果可能未解包, 统一取值
                        if (v instanceof com.googlecode.aviator.runtime.type.AviatorObject ao) {
                            v = ao.getValue(env);
                        }
                    } else {
                        v = row.get(m.getSourceField());
                    }
                    value = ExpressionUtil.normalizeForAviator(v);
                }
                default -> value = row.get(m.getSourceField());
            }
            out.put(m.getTargetField(), value);
            // 表达式可引用前置转换输出（设计文档: 表达式可引用字典转换后的结果）
            env.put(m.getTargetField(), ExpressionUtil.normalizeForAviator(value));
        }
        return out;
    }

    private Connection openConn(Map<String, String> conn) throws SQLException {
        return IncrementalPlanner.openConn(conn);
    }

    private void checkStop(JobRunContext ctx, long deadline) throws InterruptedException {
        if (ctx.getStopFlag() != null && ctx.getStopFlag().get()) {
            throw new InterruptedException("任务被手动停止");
        }
        if (System.currentTimeMillis() > deadline) {
            throw new BizException(ErrorCode.ENGINE_TIMEOUT, "执行超时(>" + ctx.getJob().getTimeoutMinutes() + " 分钟)");
        }
    }
}
