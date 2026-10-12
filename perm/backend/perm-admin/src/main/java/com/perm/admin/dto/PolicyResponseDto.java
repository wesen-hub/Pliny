package com.perm.admin.dto;

import com.perm.common.dto.PolicyDsl;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * 策略响应 DTO（管理面视角：策略语义 + 生命周期 + 元信息）
 */
public record PolicyResponseDto(
        String policyId,
        String policyName,
        String effect,
        String subjectType,
        List<String> subjectIds,
        String resourceId,
        List<String> actions,
        int priority,
        String effectiveFrom,
        String effectiveTo,
        String visible,
        List<String> columns,
        Map<String, String> masks,
        List<PolicyDsl.RowRuleDsl> rowRules,
        PolicyDsl.TagScopeDsl tagScope,
        List<PolicyDsl.ObligationDsl> obligations,
        String status,
        String createdBy,
        Instant createdAt,
        Instant updatedAt,
        int publishedVersion) {
}
