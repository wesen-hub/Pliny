package com.dcp.tagjob;

import com.dcp.tagjob.config.TaskConfig;
import com.dcp.tagjob.drl.DrlGenerator;
import com.dcp.tagjob.drl.KieBaseFactory;
import com.dcp.tagjob.fact.FactSourceGen;
import com.dcp.tagjob.plan.AssetPlan;
import com.dcp.tagjob.plan.BuildPlan;
import com.fasterxml.jackson.databind.JsonNode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 任务执行计划装配(driver 侧, 无 Spark 依赖; §12 细化):
 *  bindings 快照 + 被引用资产 → 资产表(规则=DRL 文本/流=步骤控制流) → Fact Java 源码生成
 *  → driver 侧 KieBase 编译校验(与 executor 同一 KieBaseFactory 路径: Fact 源码 + 每资产 DRL 入 KieModule,
 *  编译错误 → 任务失败, 报错明细列出)。
 */
public final class PlanAssembler {

    private PlanAssembler() {
    }

    /** 装配执行计划; 结构/编译错误抛 IllegalArgumentException(明细聚合) */
    public static BuildPlan assemble(TaskConfig cfg, String entityField, List<BuildPlan.FactField> factFields) {
        List<String> errs = new ArrayList<>();
        String taskKey = "t" + cfg.taskId + "_v" + cfg.taskVer;

        // 资产快照表(id → 定义), 重复 id 拦截
        Map<Long, TaskConfig.RuleSnapshot> snap = new LinkedHashMap<>();
        for (TaskConfig.RuleSnapshot s : cfg.rules) {
            if (snap.put(s.id, s) != null) errs.add("规则快照重复 id=" + s.id);
        }

        // 资产注册表(插入序即资产下标; 先注册后补内容, 保证自引用安全)
        List<AssetPlan> assets = new ArrayList<>();
        Map<Long, Integer> planIdx = new HashMap<>();
        Map<Long, AssetPlan> plans = new LinkedHashMap<>();
        Map<String, Character> fieldKinds = new LinkedHashMap<>();
        for (BuildPlan.FactField f : factFields) fieldKinds.put(f.name, f.kind);
        String factFqcn = FactSourceGen.className(cfg.taskId, cfg.taskVer);

        // 绑定资产确保存在(结构错误收集)
        List<Integer> flat = new ArrayList<>();
        List<Integer> flow = new ArrayList<>();
        for (TaskConfig.BindingRef b : cfg.bindings) {
            TaskConfig.RuleSnapshot s = snap.get(b.id);
            if (s == null) {
                errs.add("绑定引用资产 id=" + b.id + " 不在 rules 快照中(task-json 缺该资产定义)");
                continue;
            }
            if (!s.kind.equals(b.kind)) {
                errs.add("绑定 kind 与快照不符: 绑定=" + b.kind + " 快照=" + s.kind + " id=" + b.id);
                continue;
            }
            ensureAsset(cfg, s, snap, plans, planIdx, assets, fieldKinds, factFqcn, errs);
            Integer idx = planIdx.get(b.id);
            if ("rule".equals(b.kind)) flat.add(idx);
            else flow.add(idx);
        }

        // 被引但未绑定的资产补全(按流步骤 DFS; 新资产追加, 循环推进直到不再新增)
        int pos = 0;
        while (pos < assets.size()) {
            AssetPlan cur = assets.get(pos++);
            if (!"flow".equals(cur.kind)) continue;
            TaskConfig.RuleSnapshot s = snap.get(cur.id);
            JsonNode steps = s.def == null ? null : s.def.path("steps");
            if (steps == null || !steps.isArray() || steps.isEmpty()) {
                errs.add("[规则流 #" + cur.id + "(" + cur.name + ")] 无任何步骤");
                continue;
            }
            for (int i = 0; i < steps.size(); i++) {
                JsonNode st = steps.get(i);
                long refId = st.path("refId").asLong(0);
                if (refId <= 0) {
                    errs.add("[规则流 #" + cur.id + "] 步骤" + i + " 缺 refId");
                    continue;
                }
                Integer refIdx = planIdx.get(refId);
                if (refIdx == null) {
                    TaskConfig.RuleSnapshot ref = snap.get(refId);
                    if (ref == null) {
                        errs.add("[规则流 #" + cur.id + "] 步骤" + i + " 引用资产 id=" + refId + " 不在快照中");
                        continue;
                    }
                    refIdx = ensureAsset(cfg, ref, snap, plans, planIdx, assets, fieldKinds, factFqcn, errs);
                }
                AssetPlan target = assets.get(refIdx);
                String hit = st.path("hit").asText("next");
                String miss = st.path("miss").asText("next");
                if (!List.of("next", "stop", "jump").contains(hit)) errs.add("[规则流 #" + cur.id + "] 步骤" + i + " hit 非法: " + hit);
                if (!List.of("next", "end", "jump").contains(miss)) errs.add("[规则流 #" + cur.id + "] 步骤" + i + " miss 非法: " + miss);
                AssetPlan.FlowStep fs = new AssetPlan.FlowStep();
                fs.assetIdx = refIdx;
                fs.refId = refId;
                fs.refName = target.name;
                fs.hit = hit;
                fs.hitJump = st.path("hitJump").asInt(0);
                fs.miss = miss;
                fs.missJump = st.path("missJump").asInt(0);
                cur.steps.add(fs);
            }
        }

        if (flat.isEmpty() && flow.isEmpty()) errs.add("任务未绑定任何可执行资产(bindings 空或全部不可解析)");
        if (!errs.isEmpty()) {
            throw new IllegalArgumentException("任务计划结构错误:\n" + String.join("\n", errs));
        }

        BuildPlan plan = new BuildPlan();
        plan.taskKey = taskKey;
        plan.taskId = cfg.taskId;
        plan.taskVer = cfg.taskVer;
        plan.entityField = entityField;
        plan.factFields = factFields;
        plan.assets = assets;
        plan.flatOrder = flat.stream().mapToInt(Integer::intValue).toArray();
        plan.flowOrder = flow.stream().mapToInt(Integer::intValue).toArray();

        // 动态 Fact: 生成 Java 源码(写入 KieModule 由 Drools 编译; driver 侧亦先建一次 KieBase 做编译校验)
        String pkg = FactSourceGen.packageOf(cfg.taskId, cfg.taskVer);
        String src = FactSourceGen.genSource(pkg, factFields);
        BuildPlan.FactSpec spec = new BuildPlan.FactSpec();
        spec.className = FactSourceGen.className(cfg.taskId, cfg.taskVer);
        spec.simpleName = "Fact";
        spec.javaSource = src;
        plan.factSpec = spec;

        // driver 侧编译校验(与 executor 同一 KieBaseFactory 路径: KieModule 内联 Java 源, 无需 TCCL/动态 loader)
        List<String> kerrs = new ArrayList<>();
        if (KieBaseFactory.buildBase(assets, src, spec.className, kerrs) == null) {
            throw new IllegalArgumentException("任务 DRL 编译校验失败(规则数=" + assets.size() + "):\n"
                    + String.join("\n", kerrs));
        }
        return plan;
    }

    /** 确保资产存在并返回其下标(规则=生成 DRL; 流=仅占位, 步骤在循环推进阶段解析) */
    private static int ensureAsset(TaskConfig cfg, TaskConfig.RuleSnapshot s,
                                   Map<Long, TaskConfig.RuleSnapshot> snap,
                                   Map<Long, AssetPlan> plans, Map<Long, Integer> planIdx,
                                   List<AssetPlan> assets, Map<String, Character> fieldKinds,
                                   String factFqcn, List<String> errs) {
        Integer ex = planIdx.get(s.id);
        if (ex != null) return ex;
        AssetPlan p = new AssetPlan();
        p.id = s.id;
        p.kind = s.kind;
        p.name = s.name;
        p.ver = s.ver;
        plans.put(s.id, p);
        planIdx.put(s.id, assets.size());
        assets.add(p);
        if ("rule".equals(s.kind)) {
            p.pkgName = DrlGenerator.assetPackage(s.id, s.ver);
            String drl = DrlGenerator.genRuleDrl(s, factFqcn, fieldKinds, errs);
            p.drl = drl;   // 结构错误时 null → 编译期跳过, 错误已收集
        }
        return planIdx.get(s.id);
    }
}
