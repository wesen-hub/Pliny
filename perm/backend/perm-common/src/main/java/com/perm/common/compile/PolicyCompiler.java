package com.perm.common.compile;

import com.perm.common.dto.PolicyDsl;
import com.perm.common.model.Effect;
import com.perm.common.model.InNode;
import com.perm.common.model.MaskType;
import com.perm.common.model.Policy;
import com.perm.common.model.PolicyConstraints;
import com.perm.common.model.PredicateNode;
import com.perm.common.model.Predicates;
import com.perm.common.model.TagExpandMode;
import com.perm.common.model.Value;
import com.perm.common.model.Visibility;

import java.util.ArrayList;
import java.util.List;

/**
 * 策略编译器：管理面简化 DSL（rowRules）→ 策略模型（AST）
 * 三方共用：perm-admin 创建/更新时编译、perm-pdp 热更新快照时编译。
 * rowRules 语义：
 *   {column, op:EQ, ref:"subject.region"}     → region = ${subject.region}
 *   {column, op:EQ, value:"华北"}              → region = '华北'（字面量）
 *   {column, op:IN, ref:"subject.depts"}      → dept IN ${subject.depts}
 *   {column, op:IN, value:["A","B"]}          → dept IN ('A','B')
 * 多条 rowRules 以 AND 连接（v1 不支持 OR 组）。
 */
public final class PolicyCompiler {

    public Policy compile(PolicyDsl dto) {
        Policy.Builder b = Policy.builder()
                .policyId(required(dto.getPolicyId(), "policyId"))
                .policyName(required(dto.getPolicyName(), "policyName"))
                .effect(Effect.valueOf(required(dto.getEffect(), "effect")))
                .subjectType(required(dto.getSubjectType(), "subjectType"))
                .resourceId(required(dto.getResourceId(), "resourceId"))
                .priority(dto.getPriority());
        for (String id : required(dto.getSubjectIds(), "subjectIds")) {
            b.subjectId(id);
        }
        for (String action : required(dto.getActions(), "actions")) {
            b.action(action);
        }
        if (dto.getObligations() != null) {
            for (PolicyDsl.ObligationDsl o : dto.getObligations()) {
                b.obligation(o.type(), o.mode());
            }
        }
        if (dto.getEffectiveFrom() != null && dto.getEffectiveTo() != null) {
            b.effectivePeriod(dto.getEffectiveFrom(), dto.getEffectiveTo());
        }
        if (dto.getConstraints() != null) {
            b.constraints(compileConstraints(dto.getConstraints()));
        }
        return b.build();
    }

    private PolicyConstraints compileConstraints(PolicyDsl.ConstraintDsl c) {
        PolicyConstraints.Builder b = PolicyConstraints.builder();
        if (c.visible() != null) {
            b.visible(Visibility.valueOf(c.visible()));
        }
        if (c.rowRules() != null && !c.rowRules().isEmpty()) {
            List<PredicateNode> nodes = new ArrayList<>();
            for (PolicyDsl.RowRuleDsl rule : c.rowRules()) {
                nodes.add(compileRule(rule));
            }
            b.rowExpr(nodes.size() == 1 ? nodes.get(0)
                    : Predicates.and(nodes.toArray(PredicateNode[]::new)));
        }
        if (c.columns() != null) {
            b.columns(c.columns());
        }
        if (c.masks() != null) {
            c.masks().forEach((col, type) -> b.mask(col, MaskType.valueOf(type)));
        }
        if (c.tagScope() != null) {
            PolicyDsl.TagScopeDsl ts = c.tagScope();
            b.tagScope(new PolicyConstraints.TagScope(
                    ts.tagPath(),
                    TagExpandMode.valueOf(ts.expandMode()),
                    ts.snapshotVersion(),
                    ts.targetKey(),
                    ts.requiredLevel() == null ? 0 : ts.requiredLevel(),
                    ts.entities() == null ? List.of() : ts.entities()));
        }
        return b.build();
    }

    private PredicateNode compileRule(PolicyDsl.RowRuleDsl rule) {
        if (rule.ref() != null) {
            return switch (rule.op()) {
                case "EQ" -> Predicates.eq(rule.column(), rule.ref());
                case "IN" -> Predicates.in(rule.column(), rule.ref());
                default -> throw new IllegalArgumentException("不支持的操作符: " + rule.op());
            };
        }
        if (rule.value() != null) {
            return switch (rule.op()) {
                case "EQ" -> Predicates.eqLit(rule.column(), rule.value());
                case "IN" -> {
                    if (!(rule.value() instanceof List<?> list)) {
                        throw new IllegalArgumentException("IN 字面量必须是列表: " + rule.column());
                    }
                    yield new InNode(rule.column(), new Value.Lit(list));
                }
                default -> throw new IllegalArgumentException("不支持的操作符: " + rule.op());
            };
        }
        throw new IllegalArgumentException("rowRule 必须提供 ref 或 value: " + rule.column());
    }

    private static <T> T required(T value, String field) {
        if (value == null) {
            throw new IllegalArgumentException(field + " 必填");
        }
        return value;
    }
}
