package com.dcp.outlet.control;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 租约管理器:控制面侧的"谁在消费哪个分区"的唯一裁决者。
 *
 * <p>与 {@code consumer_unit} 表上的唯一键 `(task, topic, partition, epoch)` 构成**双保险**:
 * 数据库唯一键防"记录重复",租约防"运行时双消费"。
 */
public class LeaseManager {

    private final Map<String, LeaseGrant> byPartition = new ConcurrentHashMap<>();   // topic#p#epoch → grant
    private final Map<String, String> unitToPartition = new ConcurrentHashMap<>();   // unitId → topic#p#epoch

    private String key(String topic, int partition, int epoch) {
        return topic + "#" + partition + "#" + epoch;
    }

    /** 授予租约;同分区同代已有活跃租约且属其他单元 → 拒绝。 */
    public LeaseGrant grant(String unitId, String taskId, String topic, int partition, int epoch,
                            long ttlMillis, long now) {
        String k = key(topic, partition, epoch);
        LeaseGrant existing = byPartition.get(k);
        if (existing != null && !existing.unitId().equals(unitId) && existing.expireAt() > now) {
            throw new LeaseConflictException(topic, partition, epoch, existing.unitId());
        }
        LeaseGrant g = new LeaseGrant(unitId, taskId, topic, partition, epoch, now + ttlMillis);
        byPartition.put(k, g);
        unitToPartition.put(unitId, k);
        return g;
    }

    /** 续约:租约不存在或已被他人接管(过期后被重新授予)→ false,单元应自停。 */
    public boolean renew(String unitId, long ttlMillis, long now) {
        String k = unitToPartition.get(unitId);
        if (k == null) return false;
        LeaseGrant g = byPartition.get(k);
        if (g == null || !g.unitId().equals(unitId)) return false;
        g.expireAt(now + ttlMillis);
        return true;
    }

    public void release(String unitId) {
        String k = unitToPartition.remove(unitId);
        if (k != null) {
            LeaseGrant g = byPartition.get(k);
            if (g != null && g.unitId().equals(unitId)) byPartition.remove(k);
        }
    }

    /** 巡检:返回已过期租约并释放(调用方据此告警 —— 平台只告警,不自动补齐)。 */
    public List<LeaseGrant> expireScan(long now) {
        List<LeaseGrant> expired = new ArrayList<>();
        for (Map.Entry<String, LeaseGrant> e : byPartition.entrySet()) {
            LeaseGrant g = e.getValue();
            if (g.expireAt() <= now) {
                expired.add(g);
                byPartition.remove(e.getKey(), g);
                unitToPartition.remove(g.unitId(), e.getKey());
            }
        }
        return expired;
    }

    public LeaseGrant leaseOf(String topic, int partition, int epoch) {
        return byPartition.get(key(topic, partition, epoch));
    }

    public int activeCount() { return byPartition.size(); }
}
