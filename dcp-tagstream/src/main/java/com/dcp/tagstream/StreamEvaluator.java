package com.dcp.tagstream;

import com.dcp.tagjob.run.TagActions;
import com.dcp.tagstream.config.StreamConfig;
import com.dcp.tagstream.kie.AssetPlan;
import com.dcp.tagstream.kie.DrlGenerator;
import com.dcp.tagstream.kie.FactSourceGen;
import com.dcp.tagstream.kie.KieBaseFactory;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.kie.api.KieBase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 流求值算子: 每个事件(信封 JSON) → 动态 Fact(schema=datasetFields 全 String, 与在线一致) → Drools
 * → 命中标签收集 → 输出 Hit(entity, tags) 供双写 sink。
 *
 * <p>Drools 编译(open 一次): Fact 源码 + 每规则 DRL 入 KieModule KieBuilder 编译(同 executor 姿势);
 * KieBundle 缓存于算子生命周期。事件信封: {op, seq, fields:{...}}(与 ingest dcp.in 一致), 取 fields 映射。
 */
public class StreamEvaluator extends RichFlatMapFunction<String, StreamEvaluator.Hit> {

    private static final ObjectMapper OM = new ObjectMapper();
    private static final long serialVersionUID = 1L;

    private final StreamConfig cfg;
    private transient KieBundle bundle;

    public StreamEvaluator(StreamConfig cfg) {
        this.cfg = cfg;
    }

    @Override
    public void open(Configuration parameters) {
        bundle = buildBundle(cfg);
    }

    @Override
    public void flatMap(String json, Collector<Hit> out) throws Exception {
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
        TagActions.Collector collector = new TagActions.Collector();
        try {
            Object fact = bundle.factClass.getDeclaredConstructor().newInstance();
            for (String f : bundle.factFieldNames) {
                JsonNode v = fields.get(f);
                if (v == null || v.isNull()) continue;
                java.lang.reflect.Method setter = bundle.setters.get(f);
                if (setter == null) continue;
                Class<?> pt = setter.getParameterTypes()[0];
                Object val = v.isNumber() ? (pt == Long.class ? v.asLong() : pt == Double.class ? v.asDouble() : v.asText())
                        : v.isBoolean() ? v.asBoolean() : v.asText();
                setter.invoke(fact, val);
            }
            org.kie.api.runtime.KieSession session = bundle.base.newKieSession();
            try {
                TagActions.bindCollector(collector);
                TagActions.bindSession(session);
                org.kie.api.runtime.rule.FactHandle h = session.insert(fact);
                session.fireAllRules();
                session.delete(h);
            } finally {
                TagActions.clearThread();
                session.dispose();
            }
        } catch (Exception e) {
            // 单事件求值失败不中断流; 记错误(生产接 DLT)
            System.err.println("[tagstream] 事件求值失败: " + e.getMessage());
            return;
        }
        if (!collector.tags().isEmpty()) {
            Map<String, String> tags = new LinkedHashMap<>();
            for (String[] t : collector.tags()) tags.put(t[0], t[1] == null ? "" : t[1]);
            out.collect(new Hit(entity, tags, System.currentTimeMillis()));
        }
    }

    static KieBundle buildBundle(StreamConfig cfg) {
        // Fact 字段 = entityKey + datasetFields(全 String; 数值事件字段运行时按 setter 参数转)
        String pkg = "dcp.tagstream.fact.t" + cfg.taskId;
        String fqcn = pkg + ".Fact";
        List<com.dcp.tagstream.kie.BuildPlan.FactField> factFields = new ArrayList<>();
        List<String> names = new ArrayList<>();
        for (String f : cfg.datasetFields) {
            if (f.equals(cfg.entityKey)) continue;
            names.add(f);
        }
        for (String n : names) factFields.add(new com.dcp.tagstream.kie.BuildPlan.FactField(n, 'S'));
        // entityKey 非 String? 保守全 S, 事件数值会转
        String javaSource = FactSourceGen.genSource(pkg, factFields);
        List<String> errs = new ArrayList<>();
        List<AssetPlan> assets = new ArrayList<>();
        Map<String, Character> kinds = new HashMap<>();
        for (com.dcp.tagstream.kie.BuildPlan.FactField f : factFields) kinds.put(f.name, f.kind);
        for (StreamConfig.RuleRef r : cfg.rules) {
            if (!"rule".equals(r.kind)) continue;   // 流 v1: 平铺规则(flow 引用规则已由平台展开进 rules? 平台下发展开闭包)
            AssetPlan ap = new AssetPlan();
            ap.id = r.id;
            ap.kind = "rule";
            ap.name = r.name;
            ap.ver = r.ver;
            ap.pkgName = DrlGenerator.assetPackage(r.id, r.ver);
            String drl = DrlGenerator.genRuleDrl(r, fqcn, kinds, errs);
            if (drl == null || drl.isBlank()) continue;
            ap.drl = drl;
            assets.add(ap);
        }
        if (assets.isEmpty()) throw new IllegalArgumentException("流任务无规则: " + String.join("; ", errs));
        KieBaseFactory.KieBuildResult res = KieBaseFactory.buildBase(assets, javaSource, fqcn, errs);
        if (res == null) throw new IllegalArgumentException("流 KieBase 编译失败: " + String.join("; ", errs));
        KieBundle b = new KieBundle();
        b.base = res.kieBase();
        try {
            b.factClass = Class.forName(fqcn, true, res.moduleClassLoader());
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException("Fact 类加载失败: " + fqcn, e);
        }
        b.factFieldNames = names;
        for (com.dcp.tagstream.kie.BuildPlan.FactField f : factFields) {
            try {
                b.setters.put(f.name, b.factClass.getMethod("set" + Character.toUpperCase(f.name.charAt(0))
                        + f.name.substring(1), String.class));
            } catch (NoSuchMethodException ignore) {
            }
        }
        // entityKey 字段也需要 Fact 属性? datasetFields 含 entityKey 时规则可能引用; 规则引用字段需在 Fact。
        return b;
    }

    static class KieBundle {
        KieBase base;
        Class<?> factClass;
        List<String> factFieldNames = new ArrayList<>();
        Map<String, java.lang.reflect.Method> setters = new HashMap<>();
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
