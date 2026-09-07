package com.dcp.tagjob.run;

import com.dcp.tagjob.plan.AssetPlan;
import com.dcp.tagjob.plan.BuildPlan;
import com.dcp.tagjob.util.Log;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.kie.api.runtime.KieSession;
import org.kie.api.runtime.rule.AgendaFilter;
import org.kie.api.runtime.rule.FactHandle;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * 逐分区求值(设计文档 §12 细化; 语义对齐 §11 已拍决策):
 *
 * <p>每分区: 一个 StatefulKieSession(共享本 executor 已重建的 KieBase)。每行:
 * <pre>
 *  1. 平铺规则(A 组, bindings 中 kind=rule 的数组序): 逐资产 insert Fact → 按资产包 AgendaFilter fire → delete;
 *     若动作触发 halt()/停止动作 → 置 stopPlain, 跳过剩余平铺规则(规则流仍照跑)。
 *  2. 规则流(B 组, 数组序): Java 控制流逐步骤递归:
 *     步骤引用规则 → fire 该资产包(决策表排他组已由 activation-group+salience 保证);
 *     步骤引用流 → 递归执行其步骤(嵌套);
 *     该步骤命中(≥1 条动作, §3: 被引流命中=链内至少执行一条动作)与否 →
 *     按 hit/miss(next|stop|end|jump[+N]) 控制; stop/end 只结束当前流(§11①), 继续下一个绑定流;
 *     嵌套流终止只作用于嵌套流自身, 返回命中给外层步骤分支。
 *  3. 每行处理完: delete Fact + TagActions ThreadLocal 清空。
 * </pre>
 * halt() 实现: TagActions 置行级 halt 标志 + 尽力 KieSession.halt(); 每次 fire 后消费标志
 * (平铺阶段据此跳过剩余平铺资产; 流步骤内只中止该资产组求值, 流控制照常)。
 */
public final class PartitionEvaluator implements MapPartitionsFunction<Row, Row> {
    private static final long serialVersionUID = 1L;
    private static final int MAX_FLOW_DEPTH = 64;   // 后端环检测已拦; 防御 task-json 手工构造

    private final Broadcast<BuildPlan> planBc;
    private final MatrixAccumulator matrix;   // driver 注册; 分区结束合并命中聚合
    private final long taskId;
    private final Timestamp runTs;
    private final boolean emitRows;

    public PartitionEvaluator(Broadcast<BuildPlan> planBc, MatrixAccumulator matrix,
                              long taskId, Timestamp runTs, boolean emitRows) {
        this.planBc = planBc;
        this.matrix = matrix;
        this.taskId = taskId;
        this.runTs = runTs;
        this.emitRows = emitRows;
    }

    @Override
    public Iterator<Row> call(Iterator<Row> input) {
        BuildPlan plan = planBc.getValue();
        ExecutorRuntimeCache.ExecutorRuntime rt = ExecutorRuntimeCache.get(plan);
        Map<String, Long> partAgg = new HashMap<>();
        List<Row> out = new ArrayList<>(emitRows ? 1024 : 4);
        long rows = 0;
        try {
            if (!input.hasNext()) return out.iterator();
            KieSession session = rt.base.newKieSession();   // 每分区一个 StatefulKieSession(§12)
            try {
                while (input.hasNext()) {
                    Row row = input.next();
                    rows++;
                    partAgg.merge("rows", 1L, Long::sum);
                    Object fact = newFact(rt, plan, row);
                    String entity = stringValue(row, plan.entityField);
                    if (entity == null) partAgg.merge("nullEntity", 1L, Long::sum);

                    RowSink sink = new RowSink(entity, plan.assets.size());
                    TagActions.bindSink(sink);
                    try {
                        evalRow(session, rt, plan, fact, sink);
                    } finally {
                        TagActions.clearThread();
                    }
                    if (entity != null && !sink.tags().isEmpty()) {
                        for (RowSink.TagEntry e : sink.tags()) {
                            if (emitRows) {
                                out.add(RowFactory.create(taskId, entity, e.tag, e.value, runTs));
                            }
                        }
                    }
                    mergeSink(partAgg, plan, sink);
                }
                Log.info("[executor] 分区处理完成 rows=" + rows + " 标签行=" + out.size());
            } finally {
                session.dispose();
            }
        } finally {
            // 无 TCCL 依赖: Fact/规则均由 KieModule 类加载器编译解析(KieBaseFactory 内联 Java 源)
        }
        matrix.add(partAgg);   // 分区聚合合并(ClosureCleaner 注册的 AccumulatorV2 自动归并到 driver)
        return out.iterator();
    }

    /* ---------- 行求值 ---------- */

    private void evalRow(KieSession session, ExecutorRuntimeCache.ExecutorRuntime rt, BuildPlan plan,
                         Object fact, RowSink sink) {
        // ① 平铺规则(A 组, 数组序); halt()/停止动作 → 跳过剩余平铺规则(规则流仍照跑)
        for (int flat : plan.flatOrder) {
            fireAsset(session, rt, plan, fact, sink, flat);
            if (sink.consumeHalt()) {
                Log.info("[executor] halt()/停止动作 → 跳过剩余平铺规则(规则流照跑)");
                break;
            }
        }
        // ② 规则流(B 组, 数组序; 终止只作用于当前流 §11①)
        for (int flow : plan.flowOrder) {
            evalFlow(session, rt, plan, fact, sink, flow, 0);
        }
    }

    /** 执行单个规则资产: insert → fire(仅放行该资产包) → delete */
    private void fireAsset(KieSession session, ExecutorRuntimeCache.ExecutorRuntime rt, BuildPlan plan,
                           Object fact, RowSink sink, int assetIdx) {
        AssetPlan a = plan.assets.get(assetIdx);
        if (a.drl == null || a.drl.isEmpty()) return;
        sink.setFocused(assetIdx);
        FactHandle handle = session.insert(fact);
        try {
            TagActions.bindSession(session);
            session.fireAllRules(match -> a.pkgName.equals(match.getRule().getPackageName()));
        } finally {
            TagActions.bindSession(null);
            sink.setFocused(-1);
            session.delete(handle);
        }
    }

    /** 规则流 Java 控制流; 返回本流本轮是否至少执行一条动作(命中) */
    private boolean evalFlow(KieSession session, ExecutorRuntimeCache.ExecutorRuntime rt, BuildPlan plan,
                             Object fact, RowSink sink, int flowIdx, int depth) {
        boolean hit = evalFlowInner(session, rt, plan, fact, sink, flowIdx, depth);
        if (hit) sink.markHit(flowIdx);   // 矩阵: 流命中行(嵌套流同样标记)
        return hit;
    }

    private boolean evalFlowInner(KieSession session, ExecutorRuntimeCache.ExecutorRuntime rt, BuildPlan plan,
                                  Object fact, RowSink sink, int flowIdx, int depth) {
        if (depth > MAX_FLOW_DEPTH) {
            throw new IllegalStateException("规则流嵌套超限(" + MAX_FLOW_DEPTH + ")——task-json 编排成环?");
        }
        AssetPlan flow = plan.assets.get(flowIdx);
        long startActions = sink.totalActions();
        int i = 0;
        while (i < flow.steps.size()) {
            AssetPlan.FlowStep st = flow.steps.get(i);
            AssetPlan target = plan.assets.get(st.assetIdx);
            boolean stepHit;
            if ("rule".equals(target.kind)) {
                long before = sink.totalActions();
                fireAsset(session, rt, plan, fact, sink, st.assetIdx);
                sink.consumeHalt();   // 步骤内 halt 只中止该资产组; 流控制继续(§11①)
                stepHit = sink.totalActions() > before;
            } else if ("flow".equals(target.kind)) {
                stepHit = evalFlow(session, rt, plan, fact, sink, st.assetIdx, depth + 1);
            } else {
                throw new IllegalStateException("流程步骤引用未知资产 kind=" + target.kind + " id=" + target.id);
            }
            if (stepHit) {
                switch (st.hit == null ? "next" : st.hit) {
                    case "next" -> i++;
                    case "stop" -> {
                        return sink.totalActions() > startActions;   // 终止当前流(嵌套同理只终止自己)
                    }
                    case "jump" -> i += Math.max(1, st.hitJump);     // jump[+N]: 相对前跳 N 步(缺省/0 → 1)
                    default -> throw new IllegalStateException("非法 hit 分支: " + st.hit);
                }
            } else {
                switch (st.miss == null ? "next" : st.miss) {
                    case "next" -> i++;
                    case "end" -> {
                        return sink.totalActions() > startActions;   // 结束当前流
                    }
                    case "jump" -> i += Math.max(1, st.missJump);
                    default -> throw new IllegalStateException("非法 miss 分支: " + st.miss);
                }
            }
        }
        return sink.totalActions() > startActions;
    }

    /* ---------- 行 → Fact ---------- */

    private Object newFact(ExecutorRuntimeCache.ExecutorRuntime rt, BuildPlan plan, Row row) {
        try {
            Object fact = rt.factClass.getDeclaredConstructor().newInstance();
            for (BuildPlan.FactField f : plan.factFields) {
                Object v = valueOf(row, f);
                if (v != null) rt.setters.get(f.name).invoke(fact, v);
            }
            return fact;
        } catch (Exception e) {
            throw new IllegalStateException("行 → Fact 装配失败: " + e, e);
        }
    }

    /** Row 取值并按 schema 分类转换(与 PlanBuilder.analyzeSchema 的 kind 对应; 类型不符 → null 而非误标) */
    private Object valueOf(Row row, BuildPlan.FactField f) {
        Object v;
        try {
            v = row.getAs(f.name);
        } catch (IllegalArgumentException | NoSuchElementException e) {
            return null;   // 行缺列(schema 演进) → null
        }
        if (v == null) return null;
        return switch (f.kind) {
            case 'L' -> (v instanceof Number n) ? n.longValue() : null;
            case 'D' -> (v instanceof Number n) ? n.doubleValue() : null;
            case 'B' -> (v instanceof Boolean b) ? b : null;
            case 'S' -> String.valueOf(v);   // 含 date/timestamp 序列化字符串
            default -> null;
        };
    }

    private String stringValue(Row row, String field) {
        Object v;
        try {
            v = row.getAs(field);
        } catch (Exception e) {
            return null;
        }
        return v == null ? null : String.valueOf(v);
    }

    /** 行级命中聚合 → 分区 Map: hit:{idx}=命中行数(≥1 动作), tag:{idx}=标签行数 */
    private void mergeSink(Map<String, Long> partAgg, BuildPlan plan, RowSink sink) {
        for (int idx = 0; idx < plan.assets.size(); idx++) {
            if (sink.rowHit().get(idx)) partAgg.merge("hit:" + idx, 1L, Long::sum);
            long t = sink.tagsOf(idx);
            if (t > 0) partAgg.merge("tag:" + idx, t, Long::sum);
        }
    }
}
