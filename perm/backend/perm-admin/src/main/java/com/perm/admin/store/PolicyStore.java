package com.perm.admin.store;

import com.perm.admin.model.PolicyRecord;

import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 策略存储抽象（内存/MySQL 双实现，配置 perm.storage=memory|jdbc 切换）
 * 发布锁：R-M3 第③步"锁定"——同一时刻只有一个发布在执行。
 * planVersion 单调递增（R-M4），PDP 快照版本号与之对齐。
 */
public interface PolicyStore {

    PolicyRecord get(String policyId);

    List<PolicyRecord> all();

    /** ACTIVE 状态策略（发布到 PDP 的快照内容） */
    default List<PolicyRecord> activeRecords() {
        return all().stream()
                .filter(r -> r.status() == com.perm.admin.model.PolicyStatus.ACTIVE)
                .toList();
    }

    PolicyRecord put(PolicyRecord record);

    PolicyRecord remove(String policyId);

    int planVersion();

    /** 发布成功 → 版本 +1（原子发布第四步：持久化版本） */
    int nextVersion();

    ReentrantLock publishLock();
}
