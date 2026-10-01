package com.dcp.tagtask;

import com.dcp.tag.kie.AssetPlan;
import com.dcp.tag.kie.BuildPlan;
import com.dcp.tag.kie.DrlGenerator;
import com.dcp.tag.kie.FactSourceGen;
import com.dcp.tag.kie.KieBaseFactory;
import com.dcp.tag.kie.TagActions;
import com.fasterxml.jackson.databind.JsonNode;
import org.kie.api.KieBase;
import org.kie.api.runtime.KieSession;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 底座任务求值核(语义对齐 dcp-tagstream.StreamEvaluator, P2 切流后与 Flink 版同构):
 *
 * <ul>
 *   <li><b>计划构建</b>(init 一次): datasetFields+derived → 动态 Fact 类源码 → KieBase 编译;
 *       平铺规则(flatOrder) + 规则流(flowOrder) 预解析(含 flow.steps 引用下标);
 *       halt 语义与流版一致(中止当前资产组, 平铺跳过剩余/流内步骤控制)。</li>
 *   <li><b>每事件求值</b>: 行字段(Map) + 派生窗口值 → Fact 装配(L/D/B/S 类型转换) →
 *       evalFact(平铺 + 规则流 hit/miss next|stop|end|jump)。</li>
 *   <li><b>窗口派生</b>: 内存滚动窗口(按 processing time 对齐; entity → WinAcc[]),
 *       语义等价 Flink 版"事件级近似"。无 keyed 重分区: 同 entity 事件可能跨底座分区/实例,
 *       窗口累计按实例分片 —— 与 Flink 版同为近似语义, 精度等级一致。</li>
 * </ul>
 */
public final class TagBundle {

    /** 窗口累计实体数上限(超出淘汰最久未用; 派生为近似语义, 淘汰后重开窗) */
    private static final int MAX_WINDOW_ENTITIES = 20_000;
    private static final int MAX_FLOW_DEPTH = 64;

    private final TagParams cfg;
    private final Map<String, WinAcc[]> winByEntity;
    private Bundle bundle;

    public TagBundle(TagParams cfg) {
        this.cfg = cfg;
        this.winByEntity = new LinkedHashMap<>(256, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, WinAcc[]> eldest) {
                return size() > MAX_WINDOW_ENTITIES;
            }
        };
    }

    /** 计划构建(init 一次): 编译失败抛异常 → 平台判死信, 不带病运行 */
    public void build() {
        this.bundle = Bundle.of(cfg);
    }

    /** 窗口派生更新(entity 当前滚动窗口累计; 返回派生名 → 窗口值) */
    public Map<String, Double> updateWindow(String entity, long now, Map<String, Object> fields) {
        if (cfg.derived.isEmpty()) return Map.of();
        WinAcc[] accs = winByEntity.get(entity);
        if (accs == null) {
            accs = new WinAcc[cfg.derived.size()];
            winByEntity.put(entity, accs);
        }
        Map<String, Double> winVals = new LinkedHashMap<>();
        for (int i = 0; i < cfg.derived.size(); i++) {
            TagParams.DerivedRef d = cfg.derived.get(i);
            WinAcc acc = accs[i];
            if (acc == null || acc.windowEnd <= now) {
                long len = Math.max(1, d.windowSeconds) * 1000L;
                long start = (now / len) * len;
                acc = new WinAcc(start, start + len);
                accs[i] = acc;
            }
            Object fv = fields.get(d.field);
            if (fv instanceof Number n) acc.add(n.doubleValue());
            else if (fv != null) {
                try {
                    acc.add(Double.parseDouble(String.valueOf(fv).trim()));
                } catch (NumberFormatException ignore) {
                }
            }
            winVals.put(d.name, acc.value(d.func));
        }
        return winVals;
    }

    /** 每事件求值: 行字段 + 派生值 → Fact → 求值 → 命中标签(无命中 = 空列表) */
    public List<String[]> eval(Map<String, Object> fields, Map<String, Double> winVals) {
        try {
            Object fact = bundle.newFact(fields, winVals);
            return bundle.evalFact(fact);
        } catch (Exception e) {
            System.err.println("[tag-task] 事件求值失败: " + e.getMessage());
            return List.of();
        }
    }

    /* ---------- 计划模型(对齐 StreamEvaluator.Bundle) ---------- */

    static final class Bundle {
        KieBase base;
        Class<?> factClass;
        List<AssetPlan> assets = new ArrayList<>();
        int[] flatOrder = new int[0];
        int[] flowOrder = new int[0];
        List<BuildPlan.FactField> factFields = new ArrayList<>();
        Map<String, java.lang.reflect.Method> setters = new HashMap<>();
        Map<String, Character> kindByName = new HashMap<>();

        static Bundle of(TagParams cfg) {
            Bundle b = new Bundle();
            // Fact 字段: datasetFields(事件字段, S) + derived(窗口聚合, D)
            Map<String, Character> kinds = new LinkedHashMap<>();
            List<BuildPlan.FactField> factFields = new ArrayList<>();
            for (TagParams.FieldDef f : cfg.datasetFields) {
                if (f == null || f.name == null || f.name.isEmpty() || kinds.containsKey(f.name)) continue;
                char k = (f.kind == null || f.kind.isBlank()) ? 'S' : f.kind.charAt(0);
                if (!"LDBS".contains(String.valueOf(k))) k = 'S';
                kinds.put(f.name, k);
                factFields.add(new BuildPlan.FactField(f.name, k));
            }
            for (TagParams.DerivedRef d : cfg.derived) {
                if (kinds.containsKey(d.name)) continue;
                kinds.put(d.name, 'D');
                factFields.add(new BuildPlan.FactField(d.name, 'D'));
            }
            String pkg = "dcp.tagtask.fact.t" + cfg.taskId;
            String fqcn = pkg + ".Fact";
            String javaSource = FactSourceGen.genSource(pkg, factFields);

            // 资产表 + 绑定序(rule→平铺组, flow→规则流组; 空 bindings 兼容旧平铺 job-json)
            Map<Long, Integer> planIdx = new HashMap<>();
            List<AssetPlan> assets = new ArrayList<>();
            for (TagParams.RuleRef r : cfg.rules) {
                if (r == null || planIdx.containsKey(r.id)) continue;
                AssetPlan ap = new AssetPlan();
                ap.id = r.id;
                ap.kind = r.kind == null ? "rule" : r.kind;
                ap.name = r.name;
                ap.ver = r.ver;
                if ("rule".equals(ap.kind)) {
                    ap.pkgName = DrlGenerator.assetPackage(r.id, r.ver);
                    List<String> errs = new ArrayList<>();
                    String drl = DrlGenerator.genRuleDrl(r, fqcn, kinds, errs);
                    if (drl == null || drl.isBlank()) {
                        throw new IllegalArgumentException("[规则 " + ap.name + " #" + r.id + "] DRL 生成失败: "
                                + String.join("; ", errs));
                    }
                    ap.drl = drl;
                }
                planIdx.put(r.id, assets.size());
                assets.add(ap);
            }
            List<Integer> flat = new ArrayList<>();
            List<Integer> flow = new ArrayList<>();
            List<TagParams.BindingRef> binds = cfg.bindings;
            if (binds == null || binds.isEmpty()) {
                for (AssetPlan a : assets) {
                    if ("rule".equals(a.kind)) flat.add(planIdx.get(a.id));
                }
            } else {
                for (TagParams.BindingRef br : binds) {
                    Integer idx = planIdx.get(br.id);
                    if (idx == null) throw new IllegalArgumentException("绑定资产 id=" + br.id + " 不在资产表中");
                    if ("flow".equals(br.kind)) flow.add(idx);
                    else flat.add(idx);
                }
            }
            // flow 资产解析 steps(引用资产必须在表中; 环由发布闭包拦截, 深度上限防御)
            for (AssetPlan cur : assets) {
                if (!"flow".equals(cur.kind)) continue;
                TagParams.RuleRef rr = findRule(cfg, cur.id);
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
                    AssetPlan target = assets.get(refIdx);
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
            if (assets.isEmpty()) throw new IllegalArgumentException("底座任务无资产");
            b.assets = assets;
            b.flatOrder = flat.stream().mapToInt(Integer::intValue).toArray();
            b.flowOrder = flow.stream().mapToInt(Integer::intValue).toArray();

            List<String> errs = new ArrayList<>();
            KieBaseFactory.KieBuildResult res = KieBaseFactory.buildBase(assets, javaSource, fqcn, errs);
            if (res == null) {
                throw new IllegalArgumentException("KieBase 编译失败: " + String.join("; ", errs));
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

        private static TagParams.RuleRef findRule(TagParams cfg, long id) {
            for (TagParams.RuleRef r : cfg.rules) if (r != null && r.id == id) return r;
            return null;
        }

        /** 按计划求值一个 Fact; 返回命中的标签列表 */
        List<String[]> evalFact(Object fact) {
            TagActions.Collector collector = new TagActions.Collector();
            KieSession session = base.newKieSession();
            try {
                TagActions.bindSink(collector);
                // ① 平铺规则(数组序); halt/停止动作 → 跳过剩余平铺(规则流照跑)
                for (int flat : flatOrder) {
                    fireAsset(session, fact, flat);
                    if (collector.halted()) {
                        collector.resetHalt();
                        break;
                    }
                }
                // ② 规则流(数组序; 终止只作用于当前流)
                for (int flow : flowOrder) {
                    evalFlow(session, fact, flow, 0, collector);
                }
            } catch (Exception e) {
                System.err.println("[tag-task] 计划求值异常: " + e.getMessage());
                return List.of();
            } finally {
                TagActions.clearThread();
                session.dispose();
            }
            return collector.tags();
        }

        /** 执行单个规则资产: insert → fire(仅放行该资产包) → delete */
        private void fireAsset(KieSession session, Object fact, int assetIdx) {
            AssetPlan a = assets.get(assetIdx);
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
                throw new IllegalStateException("规则流嵌套超限(" + MAX_FLOW_DEPTH + ")——发布快照编排成环?");
            }
            AssetPlan flow = assets.get(flowIdx);
            int startTags = collector.tags().size();
            int i = 0;
            while (i < flow.steps.size()) {
                AssetPlan.FlowStep st = flow.steps.get(i);
                AssetPlan target = assets.get(st.assetIdx);
                boolean stepHit;
                if ("rule".equals(target.kind)) {
                    int before = collector.tags().size();
                    fireAsset(session, fact, st.assetIdx);
                    collector.resetHalt();   // 步骤内 halt 只中止该资产组; 流控制继续
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

        /** 行字段(Map) + 派生值(Double) 装配 Fact; 派生缺省回退行内数值/0.0 避免 null 数值比较 */
        Object newFact(Map<String, Object> fields, Map<String, Double> winVals) throws Exception {
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
                    // 行内数值(如 amount): 未命中窗口派生时从行取值; 缺省 0.0 避免 null 比较
                    Object dv = fields.get(f.name);
                    m.invoke(fact, dv instanceof Number n ? n.doubleValue()
                            : dv == null ? 0.0 : parseDoubleOrNull(String.valueOf(dv)));
                    continue;
                }
                Object v = fields.get(f.name);
                if (v == null) continue;
                switch (f.kind) {
                    case 'L' -> m.invoke(fact, v instanceof Number n ? n.longValue() : parseLongOrNull(String.valueOf(v)));
                    case 'B' -> m.invoke(fact, v instanceof Boolean bool ? bool
                            : "true".equalsIgnoreCase(String.valueOf(v).trim()));
                    default -> m.invoke(fact, String.valueOf(v));
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

    /** 滚动窗口累计器(processing time 对齐; 语义与 Flink 版 WinAcc 一致) */
    static final class WinAcc {
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
}
