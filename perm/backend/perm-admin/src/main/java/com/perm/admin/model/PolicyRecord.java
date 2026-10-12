package com.perm.admin.model;

import com.perm.common.dto.PolicyDsl;
import com.perm.common.model.Policy;

import java.time.Instant;

/**
 * 策略记录（管理面视角：策略语义 + 原始 DSL + 生命周期 + 元信息）
 * @param policy      编译后语义模型（与 perm-common Policy 一致）
 * @param request     原始请求 DSL（响应回显用）
 * @param status      生命周期状态（R-M1）
 * @param createdBy    创建人
 * @param createdAt    创建时间
 * @param updatedAt    最近更新时间
 * @param publishedVersion  已发布版本号（未发布过 = 0）
 */
public record PolicyRecord(
        Policy policy,
        PolicyDsl request,
        PolicyStatus status,
        String createdBy,
        Instant createdAt,
        Instant updatedAt,
        int publishedVersion) {

    public static PolicyRecord create(Policy policy, PolicyDsl request, String operator) {
        return new PolicyRecord(policy, request, PolicyStatus.DRAFT, operator,
                Instant.now(), Instant.now(), 0);
    }

    public PolicyRecord withStatus(PolicyStatus newStatus) {
        return new PolicyRecord(policy, request, newStatus, createdBy, createdAt, Instant.now(), publishedVersion);
    }

    public PolicyRecord withPolicy(Policy newPolicy, PolicyDsl newRequest) {
        return new PolicyRecord(newPolicy, newRequest, status, createdBy, createdAt, Instant.now(), publishedVersion);
    }

    public PolicyRecord withPublishedVersion(int version) {
        return new PolicyRecord(policy, request, status, createdBy, createdAt, updatedAt, version);
    }
}
