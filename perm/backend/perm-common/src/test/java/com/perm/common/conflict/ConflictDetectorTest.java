package com.perm.common.conflict;

import com.perm.common.model.Effect;
import com.perm.common.model.MaskType;
import com.perm.common.model.Policy;
import com.perm.common.model.PolicyConstraints;
import com.perm.common.model.Predicates;
import com.perm.common.model.Visibility;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 冲突检测测试（R-M7 九类检测 C1~C9 × 三级可判定性）
 */
class ConflictDetectorTest {

    private static final ConflictDetector.AttrRegistry ATTRS =
            ConflictDetector.AttrRegistry.of("subject.region", "subject.allowedStatus", "subject.depts");

    private static final ConflictDetector.TagRegistry TAGS =
            ConflictDetector.TagRegistry.of("业务域/客户价值/高价值", "业务域/客户价值/黑名单");

    private Policy grant(String id, String subjectId) {
        return Policy.builder()
                .policyId(id).policyName(id)
                .effect(Effect.GRANT)
                .subjectType("role").subjectId(subjectId)
                .resourceId("sdm.sales_order")
                .action("select")
                .constraints(PolicyConstraints.builder()
                        .visible(Visibility.CONDITIONAL)
                        .rowExpr(Predicates.eq("region", "subject.region"))
                        .build())
                .build();
    }

    @Test
    @DisplayName("C2 策略内矛盾：AND 子节点同列值域交集为空 → ERROR 阻断")
    void c2InternalContradiction() {
        Policy p = Policy.builder()
                .policyId("pol_new").policyName("自相矛盾")
                .effect(Effect.GRANT)
                .subjectType("role").subjectId("r1")
                .resourceId("sdm.sales_order")
                .action("select")
                .constraints(PolicyConstraints.builder()
                        .visible(Visibility.CONDITIONAL)
                        .rowExpr(Predicates.and(
                                Predicates.eqLit("region", "华东"),
                                Predicates.eqLit("region", "华南")))
                        .build())
                .build();
        ConflictReport report = ConflictDetector.detect(p, List.of(), ATTRS, TAGS);
        assertTrue(report.blocked());
        assertTrue(report.conflicts().stream()
                .anyMatch(c -> c.type() == ConflictType.ROW_PREDICATE_CONTRADICTION
                        && c.severity() == ConflictSeverity.ERROR));
    }

    @Test
    @DisplayName("C2 策略间互斥：值域交集为空 → ERROR（死策略）")
    void c2BetweenContradiction() {
        Policy newPolicy = Policy.builder()
                .policyId("pol_new").policyName("新策略")
                .effect(Effect.GRANT)
                .subjectType("role").subjectId("r1")
                .resourceId("sdm.sales_order")
                .action("select")
                .constraints(PolicyConstraints.builder()
                        .visible(Visibility.CONDITIONAL)
                        .rowExpr(Predicates.eqLit("region", "华东"))
                        .build())
                .build();
        Policy active = Policy.builder()
                .policyId("pol_old").policyName("存量策略")
                .effect(Effect.GRANT)
                .subjectType("role").subjectId("r1")
                .resourceId("sdm.sales_order")
                .action("select")
                .constraints(PolicyConstraints.builder()
                        .visible(Visibility.CONDITIONAL)
                        .rowExpr(Predicates.eqLit("region", "华南"))
                        .build())
                .build();
        ConflictReport report = ConflictDetector.detect(newPolicy, List.of(active), ATTRS, TAGS);
        assertTrue(report.blocked());
        assertTrue(report.conflicts().stream()
                .anyMatch(c -> c.type() == ConflictType.ROW_PREDICATE_CONTRADICTION));
    }

    @Test
    @DisplayName("C3 冗余覆盖：值域真子集 → WARNING")
    void c3RedundantCoverage() {
        Policy newPolicy = Policy.builder()
                .policyId("pol_new").policyName("新策略")
                .effect(Effect.GRANT)
                .subjectType("role").subjectId("r1")
                .resourceId("sdm.sales_order")
                .action("select")
                .constraints(PolicyConstraints.builder()
                        .visible(Visibility.CONDITIONAL)
                        .rowExpr(Predicates.and(
                                Predicates.eqLit("region", "华东"),
                                Predicates.rangeLit("amount", 0, 1000)))
                        .build())
                .build();
        Policy active = Policy.builder()
                .policyId("pol_old").policyName("存量策略")
                .effect(Effect.GRANT)
                .subjectType("role").subjectId("r1")
                .resourceId("sdm.sales_order")
                .action("select")
                .constraints(PolicyConstraints.builder()
                        .visible(Visibility.CONDITIONAL)
                        .rowExpr(Predicates.and(
                                Predicates.eqLit("region", "华东"),
                                Predicates.rangeLit("amount", 0, 5000)))
                        .build())
                .build();
        ConflictReport report = ConflictDetector.detect(newPolicy, List.of(active), ATTRS, TAGS);
        assertFalse(report.blocked());
        assertTrue(report.conflicts().stream()
                .anyMatch(c -> c.type() == ConflictType.ROW_PREDICATE_REDUNDANT
                        && c.severity() == ConflictSeverity.WARNING));
    }

    @Test
    @DisplayName("C4 列可见性冲突：同列 visible + exclude → WARNING")
    void c4ColumnVisibilityConflict() {
        Policy newPolicy = Policy.builder()
                .policyId("pol_new").policyName("新策略")
                .effect(Effect.GRANT)
                .subjectType("role").subjectId("r1")
                .resourceId("sdm.sales_order")
                .action("select")
                .constraints(PolicyConstraints.builder()
                        .visible(Visibility.CONDITIONAL)
                        .columns(List.of("id", "phone"))
                        .build())
                .build();
        Policy active = Policy.builder()
                .policyId("pol_old").policyName("存量策略")
                .effect(Effect.GRANT)
                .subjectType("role").subjectId("r1")
                .resourceId("sdm.sales_order")
                .action("select")
                .constraints(PolicyConstraints.builder()
                        .visible(Visibility.CONDITIONAL)
                        .excludeColumn("phone")
                        .build())
                .build();
        ConflictReport report = ConflictDetector.detect(newPolicy, List.of(active), ATTRS, TAGS);
        assertTrue(report.conflicts().stream()
                .anyMatch(c -> c.type() == ConflictType.COLUMN_VISIBILITY_CONFLICT));
    }

    @Test
    @DisplayName("C5 掩码冲突：HASH vs PARTIAL → WARNING（合并取最严 HASH）")
    void c5MaskConflict() {
        Policy newPolicy = Policy.builder()
                .policyId("pol_new").policyName("新策略")
                .effect(Effect.GRANT)
                .subjectType("role").subjectId("r1")
                .resourceId("sdm.sales_order")
                .action("select")
                .constraints(PolicyConstraints.builder()
                        .visible(Visibility.CONDITIONAL)
                        .mask("amount", MaskType.PARTIAL)
                        .build())
                .build();
        Policy active = Policy.builder()
                .policyId("pol_old").policyName("存量策略")
                .effect(Effect.GRANT)
                .subjectType("role").subjectId("r1")
                .resourceId("sdm.sales_order")
                .action("select")
                .constraints(PolicyConstraints.builder()
                        .visible(Visibility.CONDITIONAL)
                        .mask("amount", MaskType.HASH)
                        .build())
                .build();
        ConflictReport report = ConflictDetector.detect(newPolicy, List.of(active), ATTRS, TAGS);
        assertTrue(report.conflicts().stream()
                .anyMatch(c -> c.type() == ConflictType.MASK_CONFLICT
                        && c.detail().contains("HASH")));
    }

    @Test
    @DisplayName("C6 义务冲突：sync vs async → WARNING")
    void c6ObligationConflict() {
        Policy newPolicy = Policy.builder()
                .policyId("pol_new").policyName("新策略")
                .effect(Effect.GRANT)
                .subjectType("role").subjectId("r1")
                .resourceId("sdm.sales_order")
                .action("select")
                .constraints(PolicyConstraints.builder()
                        .visible(Visibility.CONDITIONAL)
                        .build())
                .obligation("AUDIT", "async")
                .build();
        Policy active = Policy.builder()
                .policyId("pol_old").policyName("存量策略")
                .effect(Effect.GRANT)
                .subjectType("role").subjectId("r1")
                .resourceId("sdm.sales_order")
                .action("select")
                .constraints(PolicyConstraints.builder()
                        .visible(Visibility.CONDITIONAL)
                        .build())
                .obligation("AUDIT", "sync")
                .build();
        ConflictReport report = ConflictDetector.detect(newPolicy, List.of(active), ATTRS, TAGS);
        assertTrue(report.conflicts().stream()
                .anyMatch(c -> c.type() == ConflictType.OBLIGATION_CONFLICT));
    }

    @Test
    @DisplayName("C7 标签引用失效：tagPath 不在标签目录 → ERROR")
    void c7TagRefInvalid() {
        Policy p = Policy.builder()
                .policyId("pol_new").policyName("坏标签引用")
                .effect(Effect.GRANT)
                .subjectType("role").subjectId("r1")
                .resourceId("sdm.sales_order")
                .action("select")
                .constraints(PolicyConstraints.builder()
                        .visible(Visibility.CONDITIONAL)
                        .tagScope(new PolicyConstraints.TagScope(
                                "业务域/风控/不存在的标签",
                                com.perm.common.model.TagExpandMode.LIST,
                                "ts-1", "customer_id", 2, List.of("1")))
                        .build())
                .build();
        ConflictReport report = ConflictDetector.detect(p, List.of(), ATTRS, TAGS);
        assertTrue(report.blocked());
        assertTrue(report.conflicts().stream()
                .anyMatch(c -> c.type() == ConflictType.TAG_REF_INVALID));
    }

    @Test
    @DisplayName("C8 变量引用失效：ref 不在属性目录 → ERROR")
    void c8VarRefInvalid() {
        Policy p = Policy.builder()
                .policyId("pol_new").policyName("坏变量引用")
                .effect(Effect.GRANT)
                .subjectType("role").subjectId("r1")
                .resourceId("sdm.sales_order")
                .action("select")
                .constraints(PolicyConstraints.builder()
                        .visible(Visibility.CONDITIONAL)
                        .rowExpr(Predicates.eq("region", "subject.nonexistentAttr"))
                        .build())
                .build();
        ConflictReport report = ConflictDetector.detect(p, List.of(), ATTRS, TAGS);
        assertTrue(report.blocked());
        assertTrue(report.conflicts().stream()
                .anyMatch(c -> c.type() == ConflictType.VAR_REF_INVALID));
    }

    @Test
    @DisplayName("C1 Effect 对撞：同域 GRANT/DENY 并存 → WARNING")
    void c1EffectCollision() {
        Policy newPolicy = grant("pol_new_grant", "r1");
        Policy deny = Policy.builder()
                .policyId("pol_old_deny").policyName("存量拒绝")
                .effect(Effect.DENY)
                .subjectType("role").subjectId("r1")
                .resourceId("sdm.sales_order")
                .action("select")
                .build();
        ConflictReport report = ConflictDetector.detect(newPolicy, List.of(deny), ATTRS, TAGS);
        assertTrue(report.conflicts().stream()
                .anyMatch(c -> c.type() == ConflictType.EFFECT_COLLISION
                        && c.severity() == ConflictSeverity.WARNING));
    }

    @Test
    @DisplayName("C9 权限静默收缩：新策略列集合收窄 → WARNING")
    void c9PermissionShrink() {
        Policy newPolicy = Policy.builder()
                .policyId("pol_new").policyName("收窄策略")
                .effect(Effect.GRANT)
                .subjectType("role").subjectId("r1")
                .resourceId("sdm.sales_order")
                .action("select")
                .constraints(PolicyConstraints.builder()
                        .visible(Visibility.CONDITIONAL)
                        .columns(List.of("id"))
                        .build())
                .build();
        Policy active = Policy.builder()
                .policyId("pol_old").policyName("宽授权")
                .effect(Effect.GRANT)
                .subjectType("role").subjectId("r1")
                .resourceId("sdm.sales_order")
                .action("select")
                .constraints(PolicyConstraints.builder()
                        .visible(Visibility.CONDITIONAL)
                        .columns(List.of("id", "phone", "amount"))
                        .build())
                .build();
        ConflictReport report = ConflictDetector.detect(newPolicy, List.of(active), ATTRS, TAGS);
        assertTrue(report.conflicts().stream()
                .anyMatch(c -> c.type() == ConflictType.PERMISSION_SHRINK
                        && c.detail().contains("phone")));
    }

    @Test
    @DisplayName("无冲突：干净报告不阻断")
    void cleanReport() {
        Policy p = Policy.builder()
                .policyId("pol_new").policyName("正常策略")
                .effect(Effect.GRANT)
                .subjectType("role").subjectId("r1")
                .resourceId("sdm.sales_order")
                .action("select")
                .constraints(PolicyConstraints.builder()
                        .visible(Visibility.CONDITIONAL)
                        .rowExpr(Predicates.eq("region", "subject.region"))
                        .build())
                .build();
        ConflictReport report = ConflictDetector.detect(p, List.of(), ATTRS, TAGS);
        assertFalse(report.blocked());
        assertEquals(0, report.conflicts().size());
    }
}
