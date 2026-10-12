package com.perm.admin.model;

import java.util.Map;
import java.util.Set;

/**
 * 策略生命周期状态机（R-M1）
 * 转换：DRAFT →submit→ IN_REVIEW →approve→ ACTIVE →deprecate→ DEPRECATED →retire→ RETIRED
 *       IN_REVIEW →reject→ DRAFT
 * 终态 RETIRED 不可逆（删除仅针对 DRAFT/IN_REVIEW）。
 */
public enum PolicyStatus {

    DRAFT, IN_REVIEW, ACTIVE, DEPRECATED, RETIRED;

    /** 合法转换表：from → (to → 动作名) */
    private static final Map<PolicyStatus, Map<PolicyStatus, String>> TRANSITIONS = Map.of(
            DRAFT, Map.of(IN_REVIEW, "submit"),
            IN_REVIEW, Map.of(ACTIVE, "approve", DRAFT, "reject"),
            ACTIVE, Map.of(DEPRECATED, "deprecate"),
            DEPRECATED, Map.of(RETIRED, "retire"));

    /** 校验转换合法性（非法抛异常，fail-fast） */
    public void assertCanTransitionTo(PolicyStatus to, String policyId) {
        Map<PolicyStatus, String> allowed = TRANSITIONS.getOrDefault(this, Map.of());
        if (!allowed.containsKey(to)) {
            throw new IllegalStateException(
                    "策略 " + policyId + " 非法状态转换: " + this + " → " + to
                            + "（合法: " + allowed + "）");
        }
    }

    /** 可编辑状态（DRAFT / IN_REVIEW 拒绝回退） */
    public boolean editable() {
        return this == DRAFT;
    }

    public static Set<PolicyStatus> deletable() {
        return Set.of(DRAFT);
    }
}
