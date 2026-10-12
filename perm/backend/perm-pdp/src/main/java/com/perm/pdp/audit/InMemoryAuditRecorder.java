package com.perm.pdp.audit;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * 内存审计记录器（perm.kafka.enabled=false 时生效，演示/离线兜底）
 * 仅本地视图，重启即失；正式形态由 KafkaAuditRecorder 投递 admin 落库。
 */
@Component
@ConditionalOnProperty(name = "perm.kafka.enabled", havingValue = "false")
public class InMemoryAuditRecorder implements AuditRecorder {

    private final ConcurrentLinkedQueue<Record> records = new ConcurrentLinkedQueue<>();

    @Override
    public void record(String userId, String resourceId, String action,
                       String decision, String mode, List<String> matchedPolicies) {
        records.add(new Record(Instant.now(), userId, resourceId, action, decision, mode, matchedPolicies));
    }

    @Override
    public List<Record> recent(int limit) {
        return records.stream().limit(limit).toList();
    }

    @Override
    public int size() {
        return records.size();
    }
}
