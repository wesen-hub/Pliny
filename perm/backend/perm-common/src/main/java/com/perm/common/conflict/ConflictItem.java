package com.perm.common.conflict;

/**
 * 冲突条目（R-M7 冲突报告结构：severity/type/opposingPolicyId/detail/affectedSubjects/suggestion）
 */
public record ConflictItem(
        ConflictSeverity severity,
        ConflictType type,
        /** 对立策略 id（策略内自检时为自身 id） */
        String opposingPolicyId,
        /** 冲突详情（人读） */
        String detail,
        /** 受影响主体描述 */
        String affectedSubjects,
        /** 处理建议 */
        String suggestion) {
}
