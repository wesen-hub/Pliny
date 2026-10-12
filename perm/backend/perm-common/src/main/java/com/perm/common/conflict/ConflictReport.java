package com.perm.common.conflict;

import java.util.List;

/**
 * 冲突报告（R-M7 输出）
 * @param newPolicyId 被检测的策略 id
 * @param conflicts   冲突条目列表
 * @param blocked     severity=ERROR 存在 → 阻断提交
 */
public record ConflictReport(String newPolicyId, List<ConflictItem> conflicts) {

    public boolean blocked() {
        return conflicts.stream().anyMatch(c -> c.severity() == ConflictSeverity.ERROR);
    }

    public List<ConflictItem> warnings() {
        return conflicts.stream().filter(c -> c.severity() == ConflictSeverity.WARNING).toList();
    }

    public List<ConflictItem> infos() {
        return conflicts.stream().filter(c -> c.severity() == ConflictSeverity.INFO).toList();
    }

    /** 无冲突 */
    public static ConflictReport clean(String policyId) {
        return new ConflictReport(policyId, List.of());
    }
}
