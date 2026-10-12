package com.perm.common.decision;

import com.perm.common.merge.ConstraintMerger;
import com.perm.common.model.Artifact;
import com.perm.common.model.AuthorizationContext;
import com.perm.common.model.AuthorizationRequest;
import com.perm.common.model.Decision;
import com.perm.common.model.Effect;
import com.perm.common.model.Policy;
import com.perm.common.model.PolicyConstraints;
import com.perm.common.model.PredicateNode;
import com.perm.common.model.Visibility;

import java.util.ArrayList;
import java.util.List;

/**
 * PDP 判定链（主体链路核心）
 * 执行顺序：
 *   ① 策略匹配（subject 域 × resourceId × action × 有效期）
 *   ② Effect 裁决：DENY 命中 → 立即 DENY（R-D2 DENY-overrides）
 *   ③ 表级短路：HIDDEN → DENY（R-D6）；ALL → PERMIT 无行级
 *   ④ GRANT 约束合并（R-D7/R-D8/R-D9/R-D16）
 *   ⑤ 标签门槛短路（R-T2）：tagLevel 不足 → DENY
 *   ⑥ 无任何 GRANT 命中 → DENY（fail-closed，R-F1）
 *   ⑦ 列级短路：合并后可见列为空 → DENY（R-D10）
 */
public final class DecisionEngine {

    private DecisionEngine() {
    }

    /**
     * 判定入口
     * @param request  判定请求
     * @param policies 当前生效版本的策略快照（planVersion 绑定）
     */
    public static Artifact decide(AuthorizationRequest request, List<Policy> policies) {
        AuthorizationContext ctx = request.context();

        List<Policy> grants = new ArrayList<>();
        List<Policy> denies = new ArrayList<>();
        for (Policy p : policies) {
            if (!subjectMatches(p, ctx)) {
                continue;
            }
            if (!p.resourceId().equals(request.resourceId())) {
                continue;
            }
            if (!actionMatches(p, request.action())) {
                continue;
            }
            if (!periodMatches(p)) {
                continue;
            }
            if (p.effect() == Effect.DENY) {
                denies.add(p);
            } else {
                grants.add(p);
            }
        }

        // ② DENY-overrides（R-D2）
        if (!denies.isEmpty()) {
            Policy p = denies.get(0);
            return Artifact.builder()
                    .decision(Decision.DENY)
                    .denyReason("DENY 策略命中: " + p.policyId() + "(" + p.policyName() + ")")
                    .obligations(ConstraintMerger.mergeObligations(
                            denies.stream().map(Policy::obligations).toList()))
                    .matchedPolicy(p.policyId())
                    .build();
        }

        // ⑥ fail-closed（R-F1）
        if (grants.isEmpty()) {
            return Artifact.builder()
                    .decision(Decision.DENY)
                    .denyReason("无任何 GRANT 策略命中（fail-closed）")
                    .build();
        }

        // ③ 表级短路（R-D6）
        boolean hidden = grants.stream()
                .map(g -> g.constraints().visible())
                .anyMatch(v -> v == Visibility.HIDDEN);
        if (hidden) {
            return Artifact.builder()
                    .decision(Decision.DENY)
                    .denyReason("表级可见性 HIDDEN（R-D6 短路）")
                    .matchedPolicies(grants.stream().map(Policy::policyId).toList())
                    .build();
        }

        // ⑤ 标签门槛短路（R-T2）
        for (Policy g : grants) {
            PolicyConstraints.TagScope ts = g.constraints().tagScope();
            if (ts != null && ctx.tagLevelOf(ts.tagPath()) < ts.requiredLevel()) {
                return Artifact.builder()
                        .decision(Decision.DENY)
                        .denyReason("标签门槛不足（tagLevel 需 T" + ts.requiredLevel() + "+）")
                        .matchedPolicies(grants.stream().map(Policy::policyId).toList())
                        .build();
            }
        }

        // ④ 约束合并（行谓词独立于列可见性：visible=ALL 仅表示列级全可见，行过滤仍需注入；
        // 所有策略都无行规则时 mergeRowPredicates 返回 null，不注入）
        PredicateNode rowPredicate = ConstraintMerger.mergeRowPredicates(
                grants.stream().map(g -> g.constraints().rowExpr()).toList());

        List<List<String>> columnsList = grants.stream()
                .map(g -> g.constraints().columns())
                .toList();
        List<List<String>> excludeList = grants.stream()
                .map(g -> g.constraints().excludeColumns())
                .toList();
        List<String> visibleColumns = ConstraintMerger.mergeColumns(columnsList, excludeList);

        List<PolicyConstraints.MaskSpec> masks = ConstraintMerger.mergeMasks(
                grants.stream().map(g -> g.constraints().masks()).toList());

        List<Policy.Obligation> obligations = ConstraintMerger.mergeObligations(
                grants.stream().map(Policy::obligations).toList());

        PolicyConstraints.TagScope tagScope = ConstraintMerger.mergeTagScope(
                grants.stream().map(g -> g.constraints().tagScope()).toList());

        // ⑦ 列级短路（R-D10）
        if (visibleColumns != null && visibleColumns.isEmpty()) {
            return Artifact.builder()
                    .decision(Decision.DENY)
                    .denyReason("合并后可见列为空（R-D10 列级短路）")
                    .matchedPolicies(grants.stream().map(Policy::policyId).toList())
                    .build();
        }

        return Artifact.builder()
                .decision(Decision.PERMIT)
                .rowPredicate(rowPredicate)
                .visibleColumns(visibleColumns)
                .masks(masks)
                .tagScope(tagScope)
                .obligations(obligations)
                .matchedPolicies(grants.stream().map(Policy::policyId).toList())
                .build();
    }

    /** subject 匹配：role 按 roles 列表、user 按 userId、group 按 attributes.groups */
    private static boolean subjectMatches(Policy p, AuthorizationContext ctx) {
        List<String> ids = p.subjectIds();
        if (ids.isEmpty()) {
            return false;
        }
        switch (p.subjectType()) {
            case "role":
                return ctx.roles().stream().anyMatch(ids::contains);
            case "user":
                return ids.contains(ctx.userId());
            case "group":
                Object groups = ctx.attributes().get("groups");
                if (groups instanceof List<?> g) {
                    return g.stream().anyMatch(ids::contains);
                }
                return false;
            default:
                return false;
        }
    }

    /** 动作匹配：空动作集 = 全部动作 */
    private static boolean actionMatches(Policy p, String action) {
        return p.actions().isEmpty() || p.actions().contains(action);
    }

    /** 有效期匹配（可选字段） */
    private static boolean periodMatches(Policy p) {
        if (p.effectivePeriod() == null) {
            return true;
        }
        long now = System.currentTimeMillis();
        Policy.EffectivePeriod ep = p.effectivePeriod();
        if (ep.from() != null && now < java.sql.Date.valueOf(ep.from()).getTime()) {
            return false;
        }
        if (ep.to() != null && now > java.sql.Date.valueOf(ep.to()).getTime()) {
            return false;
        }
        return true;
    }
}
