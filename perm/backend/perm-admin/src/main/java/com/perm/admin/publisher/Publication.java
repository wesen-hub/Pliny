package com.perm.admin.publisher;

import com.perm.common.conflict.ConflictItem;
import com.perm.common.conflict.ConflictReport;

import java.time.Instant;
import java.util.List;

/**
 * 发布记录（R-M3 第六步确认的产物）
 * @param version        发布后的 planVersion
 * @param changeType     变更类型：PUBLISH（生效）/ REVISE（修订）/ DEPRECATE（下线过渡）/ RETIRE（退役）
 * @param policyIds      涉及策略
 * @param operator       操作人
 * @param ts             发布时间
 * @param conflictReport 本次发布时通过的冲突报告（含 WARNING/INFO 存档）
 */
public record Publication(
        int version,
        String changeType,
        List<String> policyIds,
        String operator,
        Instant ts,
        ConflictReport conflictReport) {

    /** 冲突摘要（审计展示用） */
    public List<String> conflictSummary() {
        return conflictReport == null ? List.of()
                : conflictReport.conflicts().stream()
                .map(ConflictItem::type)
                .map(Enum::name)
                .toList();
    }
}
