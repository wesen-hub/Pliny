package com.perm.admin.audit;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * 内存审计存储（perm.storage=memory 模式，演示兜底；重启即失）
 */
@Component
@ConditionalOnProperty(name = "perm.storage", havingValue = "memory", matchIfMissing = true)
public class InMemoryAuditStore implements AuditStore {

    private final ConcurrentLinkedQueue<AuditRecord> records = new ConcurrentLinkedQueue<>();

    @Override
    public void save(AuditRecord record) {
        records.add(record);
    }

    @Override
    public List<AuditRecord> recent(int limit) {
        return records.stream().limit(limit).toList();
    }
}
