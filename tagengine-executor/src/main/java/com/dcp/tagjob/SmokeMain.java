package com.dcp.tagjob;

import com.dcp.tagjob.config.TaskConfig;
import com.dcp.tagjob.drl.DrlGenerator;
import com.dcp.tagjob.drl.KieBaseFactory;
import com.dcp.tagjob.fact.FactSourceGen;
import com.dcp.tagjob.plan.AssetPlan;
import com.dcp.tagjob.plan.BuildPlan;
import com.dcp.tagjob.run.RowSink;
import com.dcp.tagjob.run.TagActions;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.kie.api.KieBase;
import org.kie.api.runtime.KieSession;
import org.kie.api.runtime.rule.FactHandle;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 引擎核心自检(无 Spark 依赖; Docker build 阶段执行, 证明 shade jar 内 drools 栈可用):
 *  Fact Java 源码生成(入 KieModule 由 KieBuilder 与资产 DRL 一并编译) → DRL 生成(决策表/可视化/手写)
 *  → KieBase 编译 → 会话求值 → 动作约定(tag/halt 经 TagActions) → 排他组语义(salience+activation-group)
 *  → halt() 传播。Fact 类从 build 结果 moduleClassLoader 加载(与 KieBase 内部同源), 无 TCCL/无 javax.tools。
 * 退出码: 0=全部通过。
 */
public final class SmokeMain {

    private static final ObjectMapper OM = new ObjectMapper();
    private static final String FACT_PKG = "dcp.tagjob.fact.smoke";

    private static int failures = 0;

    public static void main(String[] args) throws Exception {
        String[] results = runSmoke();
        for (String r : results) System.out.println(r);
        if (failures == 0) {
            System.out.println("SMOKE PASS: Fact源码入KieModule编译 / DRL生成 / KieBase编译 / 会话求值 / tag/halt 动作约定 全部通过");
            System.exit(0);
        } else {
            System.out.println("SMOKE FAIL: " + failures + " 项断言未通过");
            System.exit(1);
        }
    }

    static String[] runSmoke() throws Exception {
        List<String> out = new ArrayList<>();

        // ---------- 字段表(schema 真相模拟) ----------
        List<BuildPlan.FactField> fields = new ArrayList<>();
        fields.add(new BuildPlan.FactField("customer_id", 'L'));
        fields.add(new BuildPlan.FactField("amount", 'D'));
        fields.add(new BuildPlan.FactField("status", 'S'));
        fields.add(new BuildPlan.FactField("region", 'S'));
        fields.add(new BuildPlan.FactField("net_amt", 'D'));
        Map<String, Character> kinds = new LinkedHashMap<>();
        fields.forEach(f -> kinds.put(f.name, f.kind));

        // ---------- 动态 Fact: 生成 Java 源码(写入 KieModule 由 KieBuilder 一并编译, 见 KieBaseFactory) ----------
        String src = FactSourceGen.genSource(FACT_PKG, fields);
        out.add("SMOKE Fact源码生成OK: " + FACT_PKG + ".Fact (" + src.length() + " 字符)");

        // ---------- 资产(规则)快照 ----------
        String tableJson = "{\"mode\":\"table\",\"table\":{\"field\":\"amount\",\"tag\":\"订单/金额档\","
                + "\"rows\":[{\"op\":\">=\",\"th\":\"10000\",\"val\":\"大额\"},{\"op\":\">=\",\"th\":\"1000\",\"val\":\"中额\"}],"
                + "\"elseVal\":\"小额\"}}";
        String visualBlackJson = "{\"mode\":\"visual\",\"visual\":{\"when\":[{\"logic\":\"AND\","
                + "\"conds\":[{\"field\":\"status\",\"op\":\"==\",\"val\":\"black\"}]}],"
                + "\"actions\":[{\"type\":\"tag\",\"tag\":\"风控/黑名单\",\"val\":\"\"}]}}";
        String visualStopJson = "{\"mode\":\"visual\",\"visual\":{\"when\":[{\"logic\":\"AND\","
                + "\"conds\":[{\"field\":\"region\",\"op\":\"==\",\"val\":\"华东\"}]}],\"actions\":["
                + "{\"type\":\"tag\",\"tag\":\"区域/华东\",\"val\":\"\"},{\"type\":\"stop\"}]}}";
        String drlJson = "{\"mode\":\"drl\",\"drl\":\"rule \\\"净支付超限\\\"\\n  when\\n"
                + "    $f : Fact(net_amt > 50000 && status != \\\"black\\\")\\n  then\\n"
                + "    tag($f, \\\"订单/净支付额\\\", \\\"超限\\\");\\nend\"}";

        List<AssetPlan> assets = new ArrayList<>();
        assets.add(ruleAsset(1, "金额档", tableJson, kinds, FACT_PKG + ".Fact", out));
        assets.add(ruleAsset(2, "黑名单命中", visualBlackJson, kinds, FACT_PKG + ".Fact", out));
        assets.add(ruleAsset(3, "华东归属(含停止)", visualStopJson, kinds, FACT_PKG + ".Fact", out));
        assets.add(ruleAsset(4, "净支付超限(DRL手写)", drlJson, kinds, FACT_PKG + ".Fact", out));

        // ---------- KieBase 编译(KieModule 内联 Fact 源码; 无需 TCCL) + Fact 类/会话求值 ----------
        List<String> errs = new ArrayList<>();
        KieBaseFactory.KieBuildResult res = KieBaseFactory.buildBase(assets, src, FACT_PKG + ".Fact", errs);
        if (res == null) {
            out.add("SMOKE FAIL KieBase 编译错误:\n" + String.join("\n", errs));
            failures++;
            return out.toArray(new String[0]);
        }
        KieBase base = res.kieBase();
        Class<?> factClass;
        try {
            factClass = Class.forName(FACT_PKG + ".Fact", true, res.moduleClassLoader());
        } catch (ClassNotFoundException e) {
            out.add("SMOKE FAIL KieModule 内未找到 Fact 类: " + e.getMessage());
            failures++;
            return out.toArray(new String[0]);
        }
        Map<String, Method> setters = new LinkedHashMap<>();
        for (BuildPlan.FactField f : fields) {
            setters.put(f.name, factClass.getMethod("set" + Character.toUpperCase(f.name.charAt(0)) + f.name.substring(1),
                    switch (f.kind) {
                        case 'L' -> Long.class;
                        case 'D' -> Double.class;
                        case 'B' -> Boolean.class;
                        default -> String.class;
                    }));
        }
        out.add("SMOKE KieBase编译OK: 包数=" + assets.size());

        // T1: 决策表排他组 —— amount=20000 → 大额(命中高 salience, 中额/小额被撤销)
        KieSession s = base.newKieSession();
        RowSink r1 = evalOne(s, assets, newFact(factClass, setters, Map.of(
                "customer_id", 1L, "amount", 20000.0d, "status", "gold", "region", "华北", "net_amt", 1000.0d)), 0);
        check(out, r1, "T1 金额档 20000 → 大额", "订单/金额档", "大额");
        checkNoTag(out, r1, "T1 不含中额/小额", "中额", "小额");
        s.dispose();

        // T2: 兜底 —— amount=500 → 小额
        s = base.newKieSession();
        RowSink r2 = evalOne(s, assets, newFact(factClass, setters, Map.of(
                "customer_id", 2L, "amount", 500.0d, "status", "gold", "region", "华北", "net_amt", 10.0d)), 0);
        check(out, r2, "T2 金额档 500 → 小额(兜底)", "订单/金额档", "小额");
        s.dispose();

        // T3: 可视化 —— status=black → 风控/黑名单(布尔标签无值)
        s = base.newKieSession();
        RowSink r3 = evalOne(s, assets, newFact(factClass, setters, Map.of(
                "customer_id", 3L, "amount", 1000.0d, "status", "black", "region", "华北", "net_amt", 10.0d)), 1);
        check(out, r3, "T3 黑名单命中", "风控/黑名单", null);
        s.dispose();

        // T4: 手写 DRL —— net_amt>50000 且非 black → 净支付额=超限
        s = base.newKieSession();
        RowSink r4 = evalOne(s, assets, newFact(factClass, setters, Map.of(
                "customer_id", 4L, "amount", 0.0d, "status", "white", "region", "华北", "net_amt", 60000.0d)), 3);
        check(out, r4, "T4 DRL手写净支付超限", "订单/净支付额", "超限");
        s.dispose();

        // T5: 手写 DRL 不误伤 —— net_amt 小 & black → 无超限标签
        s = base.newKieSession();
        RowSink r5 = evalOne(s, assets, newFact(factClass, setters, Map.of(
                "customer_id", 5L, "amount", 0.0d, "status", "black", "region", "华北", "net_amt", 100.0d)), 3);
        checkNoTag(out, r5, "T5 黑名单不触发超限", "订单/净支付额");
        s.dispose();

        // T6: 可视化 stop 动作 —— region=华东 → 打 区域/华东 且 halt 标志置位(停止动作生效)
        s = base.newKieSession();
        RowSink r6 = evalOne(s, assets, newFact(factClass, setters, Map.of(
                "customer_id", 6L, "amount", 1000.0d, "status", "gold", "region", "华东", "net_amt", 10.0d)), 2);
        check(out, r6, "T6 华东归属+停止动作", "区域/华东", null);
        check(out, r6.consumeHalt(), "T6 halt()/停止动作标志置位");
        s.dispose();

        // T7: 排他组撤销验证 —— 大额命中时小额不得执行(与 T1 同断言, 复核 activation-group 语义)
        s = base.newKieSession();
        RowSink r7 = evalOne(s, assets, newFact(factClass, setters, Map.of(
                "customer_id", 7L, "amount", 99999.0d, "status", "gold", "region", "华北", "net_amt", 0.0d)), 0);
        boolean ok = r7.tags().size() == 1 && "大额".equals(r7.tags().get(0).value);
        check(out, ok, "T7 排他组: 命中恰 1 行(大额), 无兜底混入");
        s.dispose();

        return out.toArray(new String[0]);
    }

    /* ---------- helpers ---------- */

    private static AssetPlan ruleAsset(long id, String name, String defJson,
                                       Map<String, Character> kinds, String factFqcn, List<String> out) throws Exception {
        TaskConfig.RuleSnapshot snap = new TaskConfig.RuleSnapshot();
        snap.id = id;
        snap.kind = "rule";
        snap.name = name;
        snap.ver = 1;
        snap.def = OM.readTree(defJson);
        AssetPlan p = new AssetPlan();
        p.id = id;
        p.kind = "rule";
        p.name = name;
        p.ver = 1;
        p.pkgName = DrlGenerator.assetPackage(id, 1);
        List<String> errs = new ArrayList<>();
        p.drl = DrlGenerator.genRuleDrl(snap, factFqcn, kinds, errs);
        if (p.drl == null) {
            out.add("SMOKE FAIL 资产 " + name + " DRL 生成错误: " + String.join("\n", errs));
            failures++;
        }
        return p;
    }

    private static Object newFact(Class<?> factClass, Map<String, Method> setters,
                                  Map<String, Object> values) throws Exception {
        Object f = factClass.getDeclaredConstructor().newInstance();
        for (Map.Entry<String, Object> e : values.entrySet()) {
            setters.get(e.getKey()).invoke(f, e.getValue());
        }
        return f;
    }

    /** 单资产求值(等价 executor fireAsset: insert → fire(pkg filter) → delete) */
    private static RowSink evalOne(KieSession session, List<AssetPlan> assets, Object fact, int assetIdx) {
        AssetPlan a = assets.get(assetIdx);
        RowSink sink = new RowSink("smoke-entity", assets.size());
        TagActions.bindSink(sink);
        sink.setFocused(assetIdx);
        FactHandle h = session.insert(fact);
        try {
            TagActions.bindSession(session);
            session.fireAllRules(m -> a.pkgName.equals(m.getRule().getPackageName()));
        } finally {
            TagActions.bindSession(null);
            sink.setFocused(-1);
            session.delete(h);
            TagActions.clearThread();
        }
        return sink;
    }

    private static void check(List<String> out, RowSink sink, String name, String tag, String value) {
        boolean found = false;
        for (RowSink.TagEntry e : sink.tags()) {
            if (e.tag.equals(tag) && (value == null ? e.value == null : value.equals(e.value))) {
                found = true;
                break;
            }
        }
        check(out, found, name + " (期望 tag=" + tag + " value=" + value + ", 实际 tags=" + sink.tags() + ")");
    }

    private static void checkNoTag(List<String> out, RowSink sink, String name, String... tags) {
        for (String t : tags) {
            for (RowSink.TagEntry e : sink.tags()) {
                if (t.equals(e.tag)) {
                    check(out, false, name + " —— 不应含 " + t + "(实际 tags=" + sink.tags() + ")");
                    return;
                }
            }
        }
        check(out, true, name + " OK");
    }

    private static void check(List<String> out, boolean ok, String name) {
        out.add((ok ? "PASS " : "FAIL ") + name);
        if (!ok) failures++;
    }
}
