package com.perm.admin.store;

import com.perm.admin.model.PolicyRecord;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 内存策略存储（演示/单机模式；perm.storage=memory 时生效，默认）
 */
@Component
@ConditionalOnProperty(name = "perm.storage", havingValue = "memory", matchIfMissing = true)
public class InMemoryPolicyStore implements PolicyStore {

    private final Map<String, PolicyRecord> records = new ConcurrentHashMap<>();
    private final AtomicInteger planVersion = new AtomicInteger(1);
    /** 发布锁（原子发布第三步：锁定） */
    private final ReentrantLock publishLock = new ReentrantLock();

    @Override
    public PolicyRecord get(String policyId) {
        return records.get(policyId);
    }

    @Override
    public List<PolicyRecord> all() {
        return new ArrayList<>(records.values());
    }

    @Override
    public PolicyRecord put(PolicyRecord record) {
        records.put(record.policy().policyId(), record);
        return record;
    }

    @Override
    public PolicyRecord remove(String policyId) {
        return records.remove(policyId);
    }

    @Override
    public int planVersion() {
        return planVersion.get();
    }

    @Override
    public int nextVersion() {
        return planVersion.incrementAndGet();
    }

    @Override
    public ReentrantLock publishLock() {
        return publishLock;
    }
}
