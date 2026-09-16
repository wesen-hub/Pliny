package com.dcp.tagstream;

import com.dcp.tag.kie.AssetPlan;
import com.dcp.tag.kie.BuildPlan;
import com.dcp.tag.kie.DrlGenerator;
import com.dcp.tag.kie.FactSourceGen;
import com.dcp.tag.kie.KieBaseFactory;
import com.dcp.tag.kie.TagActions;
import com.dcp.tagstream.config.StreamConfig;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.kie.api.KieBase;
import org.kie.api.runtime.KieSession;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 流求值算子(P1.2 ②①: 批端执行计划同构 + 窗口派生):
 * <pre>
 * 拓扑: source → keyBy(entity) → process(本类)
 *   · 事件阶段(每事件): 窗口累计器更新(derived 非空时) → 动态 Fact(事件字段 + 派生字段当前窗口累计值,
 *     缺省 0 避免 null 数值比较) → 按执行计划求值(平铺规则逐资产包 fire + 规则流 Java 控制流
 *     hit/miss next|stop|end|jump, 与 PartitionEvaluator 同语义) → 命中输出 Hit(双写 sink)
 *   · 窗口阶段: 滚动窗口(processing time, 对齐窗口边界); 事件到达重建过期窗口并注册边界 timer;
 *     onTimer 到点重置累计器(下事件开新窗) —— 事件级近似(窗口内累计) + 窗口边界精确重置
 * </pre>
 * 事件信封: {op, seq, fields:{...}}(与 ingest dcp.in 一致), 取 fields 映射; entityKey 取字段值。
 */
public class StreamEvaluator extends KeyedProcessFunction<String, String, StreamEvaluator.Hit> {

    private static final ObjectMapper OM = new ObjectMapper();
    private static final long serialVersionUID = 1L;
    private static final int MAX_FLOW_DEPTH = 64;

    private final StreamConfig cfg;
    private transient Bundle bundle;
    private transient ValueState<WinAcc[]> winState;

    public StreamEvaluator(StreamConfig cfg) {
        this.cfg = cfg;
    }

    @Override
    public void open(Configuration parameters) {
        bundle = buildBundle(cfg);
        if (!cfg.derived.isEmpty()) {
            winState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("tagstream-win", TypeInformation.of(WinAcc[].class)));
        }
    }

    @Override
    public void processElement(String json, Context ctx, Collector<Hit> out) throws Exception {
        JsonNode ev;
        try {
            ev = OM.readTree(json);
        } catch (Exception e) {
            return;   // 非 JSON 事件丢弃
        }
        JsonNode fields = ev.has("fields") ? ev.path("fields") : ev;
        if (fields == null || !fields.isObject()) return;
        String entity = fields.path(cfg.entityKey).asText(null);
        if (entity == null || entity.isEmpty()) return;

        long now = System.currentTimeMillis();
        Map<String, Double> winVals = new LinkedHashMap<>();
        if (!cfg.derived.isEmpty()) {
            WinAcc[] accs = winState.value();
            if (accs == null) accs = new WinAcc[cfg.derived.size()];
            for (int i = 0; i < cfg.derived.size(); i++) {
                StreamConfig.DerivedRef d = cfg.derived.get(i);
                WinAcc acc = accs[i];
                if (acc == null || acc.windowEnd <= now) {
                    long start = (now / (d.windowSeconds * 1000L)) * (d.windowSeconds * 1000L);
                    acc = new WinAcc(start, start + d.windowSeconds * 1000L);
                    ctx.timerService().registerProcessingTimeTimer(acc.windowEnd);
                    accs[i] = acc;
                }
                JsonNode fv = fields.get(d.field);
                if (fv != null && fv.isNumber()) acc.add(fv.asDouble());
                else if (fv != null && fv.isTextual()) {
                    try {
                        acc.add(Double.parseDouble(fv.asText()));
                    } catch (NumberFormatException ignore) {
                    }
                }
                winVals.put(d.name, acc.value(d.func));
            }
            winState.update(accs);
        }
        try {
            Object fact = bundle.newFact(fields, winVals);
            List<String[]> tags = evalFact(fact);
            if (!tags.isEmpty()) {
                Map<String, String> tm = new LinkedHashMap<>();
                for (String[] t : tags) tm.put(t[0], t[1] == null ? "" : t[1]);
                out.collect(new Hit(entity, tm, now));
            }
        } catch (Exception e) {
            System.err.println("[tagstream] 事件求值失败: " + e.getMessage());
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Hit> out) throws Exception {
        if (winState == null) return;
        WinAcc[] accs = winState.value();
        if (accs == null) return;
        for (int i = 0; i < accs.length; i++) {
            if (accs[i] != null && accs[i].windowEnd <= timestamp) accs[i] = null;   // 窗口重置(下事件开新窗)
        }
        winState.update(accs);
    }

    /* ---------- 求值(批端执行计划同构) ---------- */

    /** 按计划求值一个 Fact; 返回命中的标签列表 */
    private List<String[]> evalFact(Object fact) {
        TagActions.Collector collector = new TagActions.Collector();
        KieSession session = bundle.base.newKieSession();
        try {
            TagActions.bindSink(collector);
            // ① 平铺规则(数组序); halt/停止动作 → 跳过剩余平铺(规则流照跑)
            for (int flat : bundle.flatOrder) {
                fireAsset(session, fact, flat);
                if (collector.halted()) {
                    collector.resetHalt();
                    break;
                }
            }
            // ② 规则流(数组序; 终止只作用于当前流)
            for (int flow : bundle.flowOrder) {
                evalFlow(session, fact, flow, 0, collector);
            }
        } catch (Exception e) {
            System.err.println("[tagstream] 计划求值异常: " + e.getMessage());
            return List.of();
        } finally {
            TagActions.clearThread();
            session.dispose();
        }
        return collector.tags();
    }

    /** 执行单个规则资产: insert → fire(仅放行该资产包) → delete */
    private void fireAsset(KieSession session, Object fact, int assetIdx) {
        AssetPlan a = bundle.assets.get(assetIdx);
        if (a.drl == null || a.drl.isEmpty()) return;
        try {
            TagActions.bindSession(session);
            org.kie.api.runtime.rule.FactHandle h = session.insert(fact);
            session.fireAllRules(match -> a.pkgName.equals(match.getRule().getPackageName()));
            session.delete(h);
        } finally {
            TagActions.bindSession(null);
        }
    }

    /** 规则流 Java 控制流(hit/miss: next|stop|end|jump); 返回本流是否命中(至少一枚标签) */
    private boolean evalFlow(KieSession session, Object fact, int flowIdx, int depth,
                             TagActions.Collector collector) {
        if (depth > MAX_FLOW_DEPTH) {
            throw new IllegalStateException("规则流嵌套超限(" + MAX_FLOW_DEPTH + ")——job-json 编排成环?");
        }
        AssetPlan flow = bundle.assets.get(flowIdx);
        int startTags = collector.tags().size();
        int i = 0;
        while (i < flow.steps.size()) {
            AssetPlan.FlowStep st = flow.steps.get(i);
            AssetPlan target = bundle.assets.get(st.assetIdx);
            boolean stepHit;
            if ("rule".equals(target.kind)) {
                int before = collector.tags().size();
                fireAsset(session, fact, st.assetIdx);
                collector.resetHalt();   // 步骤内 halt 只中止该资产组; 流控制继续(§11①)
                stepHit = collector.tags().size() > before;
            } else if ("flow".equals(target.kind)) {
                stepHit = evalFlow(session, fact, st.assetIdx, depth + 1, collector);
            } else {
                throw new IllegalStateException("流程步骤引用未知资产 kind=" + target.kind + " id=" + target.id);
            }
            if (stepHit) {
                switch (st.hit == null ? "next" : st.hit) {
                    case "next" -> i++;
                    case "stop" -> {
                        return collector.tags().size() > startTags;   // 终止当前流(嵌套同理)
                    }
                    case "jump" -> i += Math.max(1, st.hitJump);
                    default -> throw new IllegalStateException("非法 hit 分支: " + st.hit);
                }
            } else {
                switch (st.miss == null ? "next" : st.miss) {
                    case "next" -> i++;
                    case "end" -> {
                        return collector.tags().size() > startTags;   // 结束当前流
                    }
                    case "jump" -> i += Math.max(1, st.missJump);
                    default -> throw new IllegalStateException("非法 miss 分支: " + st.miss);
                }
            }
        }
        return collector.tags().size() > startTags;
    }

    /* ---------- 计划构建(open 一次; 对齐 batch PlanAssembler) ---------- */

    static Bundle buildBundle(StreamConfig cfg) {
        Bundle b = new Bundle();
        // Fact 字段: datasetFields(事件字段, S) + derived(窗口聚合, D); entityKey 若在 dsFields 中保持
        Map<String, Character> kinds = new LinkedHashMap<>();
        List<BuildPlan.FactField> factFields = new ArrayList<>();
        for (StreamConfig.FieldDef f : cfg.datasetFields) {
            if (f == null || f.name == null || f.name.isEmpty() || kinds.containsKey(f.name)) continue;
            char k = (f.kind == null || f.kind.isBlank()) ? 'S' : f.kind.charAt(0);
            if (!"LDBS".contains(String.valueOf(k))) k = 'S';
            kinds.put(f.name, k);
            factFields.add(new BuildPlan.FactField(f.name, k));
        }
        for (StreamConfig.DerivedRef d : cfg.derived) {
            if (kinds.containsKey(d.name)) continue;
            kinds.put(d.name, 'D');
            factFields.add(new BuildPlan.FactField(d.name, 'D'));
        }
        String pkg = "dcp.tagstream.fact.t" + cfg.taskId;
        String fqcn = pkg + ".Fact";
        String javaSource = FactSourceGen.genSource(pkg, factFields);

        // 资产表 + 绑定序(rule→平铺组, flow→规则流组; 空 bindings 兼容旧平铺 job-json)
        List<Map<Long, Integer>> idxHolder = new ArrayList<>();
        Map<Long, Integer> planIdx = new HashMap<>();
        List<AssetPlan> assets = new ArrayList<>();
        for (StreamConfig.RuleRef r : cfg.rules) {
            Integer ex = planIdx.get(r.id);
            if (ex != null) {
                if ("flow".equals(r.kind)) {
                    // 同名(rule 与 flow 不共存): 忽略
                }
                continue;
            }
            AssetPlan ap = new AssetPlan();
            ap.id = r.id;
            ap.kind = r.kind == null ? "rule" : r.kind;
            ap.name = r.name;
            ap.ver = r.ver;
            if ("rule".equals(ap.kind)) {
                ap.pkgName = DrlGenerator.assetPackage(r.id, r.ver);
                List<String> errs = new ArrayList<>();
                String drl = DrlGenerator.genRuleDrl(toSnapshot(r), fqcn, kinds, errs);
                if (drl == null || drl.isBlank()) {
                    throw new IllegalArgumentException("[流规则 " + ap.name + " #" + r.id + "] DRL 生成失败: "
                            + String.join("; ", errs));
                }
                ap.drl = drl;
            }
            planIdx.put(r.id, assets.size());
            assets.add(ap);
        }
        List<AssetPlan> assetsLocal = assets;
        List<Integer> flat = new ArrayList<>();
        List<Integer> flow = new ArrayList<>();
        List<StreamConfig.BindingRef> binds = cfg.bindings;
        if (binds == null || binds.isEmpty()) {
            for (AssetPlan a : assetsLocal) {
                if ("rule".equals(a.kind)) flat.add(planIdx.get(a.id));
            }
        } else {
            for (StreamConfig.BindingRef br : binds) {
                Integer idx = planIdx.get(br.id);
                if (idx == null) throw new IllegalArgumentException("绑定资产 id=" + br.id + " 不在 job-json 资产表中");
                if ("flow".equals(br.kind)) flow.add(idx);
                else flat.add(idx);
            }
        }
        // flow 资产解析 steps(引用资产必须在表中; 环由发布闭包/后端拦截, 深度上限防御)
        for (AssetPlan cur : assetsLocal) {
            if (!"flow".equals(cur.kind)) continue;
            StreamConfig.RuleRef rr = findRule(cfg, cur.id);
            JsonNode steps = rr == null || rr.def == null ? null : rr.def.path("steps");
            if (steps == null || !steps.isArray() || steps.isEmpty()) {
                throw new IllegalArgumentException("[规则流 " + cur.name + " #" + cur.id + "] 无任何步骤");
            }
            for (int i = 0; i < steps.size(); i++) {
                JsonNode st = steps.get(i);
                long refId = st.path("refId").asLong(0);
                Integer refIdx = planIdx.get(refId);
                if (refIdx == null) throw new IllegalArgumentException(
                        "[规则流 " + cur.name + "] 步骤" + i + " 引用资产 id=" + refId + " 不在资产表(发布快照缺失)");
                AssetPlan target = assetsLocal.get(refIdx);
                AssetPlan.FlowStep fs = new AssetPlan.FlowStep();
                fs.assetIdx = refIdx;
                fs.refId = refId;
                fs.refName = target.name;
                String hit = st.path("hit").asText("next");
                String miss = st.path("miss").asText("next");
                if (!List.of("next", "stop", "jump").contains(hit)) throw new IllegalArgumentException("hit 非法: " + hit);
                if (!List.of("next", "end", "jump").contains(miss)) throw new IllegalArgumentException("miss 非法: " + miss);
                fs.hit = hit;
                fs.hitJump = st.path("hitJump").asInt(0);
                fs.miss = miss;
                fs.missJump = st.path("missJump").asInt(0);
                cur.steps.add(fs);
            }
        }
        if (assetsLocal.isEmpty()) throw new IllegalArgumentException("流任务无资产");
        b.assets = assetsLocal;
        b.flatOrder = flat.stream().mapToInt(Integer::intValue).toArray();
        b.flowOrder = flow.stream().mapToInt(Integer::intValue).toArray();
        b.derived = cfg.derived == null ? List.of() : cfg.derived;

        List<String> errs = new ArrayList<>();
        KieBaseFactory.KieBuildResult res = KieBaseFactory.buildBase(assetsLocal, javaSource, fqcn, errs);
        if (res == null) {
            throw new IllegalArgumentException("流 KieBase 编译失败: " + String.join("; ", errs));
        }
        b.base = res.kieBase();
        try {
            b.factClass = Class.forName(fqcn, true, res.moduleClassLoader());
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException("Fact 类加载失败: " + fqcn, e);
        }
        b.factFields = factFields;
        for (BuildPlan.FactField f : factFields) {
            String mn = "set" + Character.toUpperCase(f.name.charAt(0)) + f.name.substring(1);
            try {
                Class<?> pt = switch (f.kind) {
                    case 'L' -> Long.class;
                    case 'D' -> Double.class;
                    case 'B' -> Boolean.class;
                    default -> String.class;
                };
                b.setters.put(f.name, b.factClass.getMethod(mn, pt));
                b.kindByName.put(f.name, f.kind);
            } catch (NoSuchMethodException ignore) {
            }
        }
        return b;
    }

    private static StreamConfig.RuleRef findRule(StreamConfig cfg, long id) {
        for (StreamConfig.RuleRef r : cfg.rules) if (r.id == id) return r;
        return null;
    }

    private static StreamConfig.RuleRef toSnapshot(StreamConfig.RuleRef r) {
        return r;   // RuleRef 结构已含 def, DrlGenerator 消费同一形状
    }

    /** 每事件装配 Fact: 事件字段(S) + 派生字段(D, 缺省 0.0 避免 null 数值比较) */
    /* ---------- 内部模型 ---------- */

    static class Bundle {
        KieBase base;
        Class<?> factClass;
        List<AssetPlan> assets = new ArrayList<>();
        int[] flatOrder = new int[0];
        int[] flowOrder = new int[0];
        List<StreamConfig.DerivedRef> derived = List.of();
        List<BuildPlan.FactField> factFields = new ArrayList<>();
        Map<String, java.lang.reflect.Method> setters = new HashMap<>();
        Map<String, Character> kindByName = new HashMap<>();

        /** 事件字段(按类型 L/D/B/S)+ 派生值(Double) 装配 Fact; 派生缺省 0.0 避免 null 数值比较 */
        Object newFact(JsonNode fields, Map<String, Double> winVals) throws Exception {
            Object fact = factClass.getDeclaredConstructor().newInstance();
            for (BuildPlan.FactField f : factFields) {
                java.lang.reflect.Method m = setters.get(f.name);
                if (m == null) continue;
                if (f.kind == 'D') {
                    Double wv = winVals == null ? null : winVals.get(f.name);
                    if (wv != null) {
                        m.invoke(fact, wv);
                        continue;
                    }
                    // 事件字段数值(如 amount): 未命中窗口派生时从事件取值; 缺省 0.0 避免 null 比较
                    JsonNode dv = fields.get(f.name);
                    m.invoke(fact, dv == null || dv.isNull() ? 0.0
                            : (dv.isNumber() ? dv.asDouble() : parseDoubleOrNull(dv.asText())));
                    continue;
                }
                JsonNode v = fields.get(f.name);
                if (v == null || v.isNull()) continue;
                switch (f.kind) {
                    case 'L' -> m.invoke(fact, v.isNumber() ? v.asLong() : parseLongOrNull(v.asText()));
                    case 'B' -> m.invoke(fact, v.isBoolean() ? v.asBoolean()
                            : "true".equalsIgnoreCase(v.asText()));
                    default -> m.invoke(fact, v.asText());
                }
            }
            return fact;
        }

        private static Long parseLongOrNull(String s) {
            try {
                return s == null || s.isBlank() ? null : Long.parseLong(s.trim());
            } catch (NumberFormatException e) {
                return null;
            }
        }

        private static Double parseDoubleOrNull(String s) {
            try {
                return s == null || s.isBlank() ? null : Double.parseDouble(s.trim());
            } catch (NumberFormatException e) {
                return null;
            }
        }
    }

    /** 窗口累计器(滚动窗口, processing time) */
    static class WinAcc {
        double sum = 0;
        long count = 0;
        double min = Double.MAX_VALUE;
        double max = -Double.MAX_VALUE;
        final long windowStart;
        final long windowEnd;

        WinAcc(long windowStart, long windowEnd) {
            this.windowStart = windowStart;
            this.windowEnd = windowEnd;
        }

        void add(double v) {
            sum += v;
            count++;
            if (v < min) min = v;
            if (v > max) max = v;
        }

        double value(String func) {
            return switch (func == null ? "sum" : func.toLowerCase()) {
                case "count" -> (double) count;
                case "avg" -> count == 0 ? 0.0 : sum / count;
                case "min" -> count == 0 ? 0.0 : min;
                case "max" -> count == 0 ? 0.0 : max;
                default -> sum;
            };
        }
    }

    /** 命中结果(供 sink) */
    public static class Hit {
        public final String entity;
        public final Map<String, String> tags;
        public final long ts;

        public Hit(String entity, Map<String, String> tags, long ts) {
            this.entity = entity;
            this.tags = tags;
            this.ts = ts;
        }
    }
}
