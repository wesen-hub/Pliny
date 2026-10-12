package com.perm.pdp.audit;

import java.time.Instant;
import java.util.List;

/**
 * 审计记录器（义务执行：AUDIT sync/async，R-A）
 * sync：判定路径内直写审计通道（失败重试，不阻断判定——审计不可用降级为本地视图）
 * async：审计积压队列后台异步冲刷（批量投递）
 * 实现：InMemoryAuditRecorder（无 Kafka 时演示兜底）/ KafkaAuditRecorder（正式形态主通道）
 */
public interface AuditRecorder {

    record Record(
            Instant ts,
            String userId,
            String resourceId,
            String action,
            String decision,
            String mode,
            List<String> matchedPolicies) {
    }

    /** 执行审计义务（mode=sync 直写通道；mode=async 积压队列异步冲刷） */
    void record(String userId, String resourceId, String action,
                String decision, String mode, List<String> matchedPolicies);

    /** 本地最近视图（运维排查兜底；正式查询入口在 admin 审计接口） */
    List<Record> recent(int limit);

    /** 本地视图容量 */
    int size();
}
