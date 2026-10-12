package com.perm.admin.audit;

import java.time.Instant;
import java.util.List;

/**
 * 审计流水（PDP 判定义务经 Kafka 投递后的消费形态）
 * 与 PDP AuditRecorder.Record 同构（跨服务事件契约）。
 */
public record AuditRecord(
        Instant ts,
        String userId,
        String resourceId,
        String action,
        String decision,
        String mode,
        List<String> matchedPolicies) {
}
