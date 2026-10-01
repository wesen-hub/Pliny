package com.dcp.scheduler.alloc;

import com.dcp.scheduler.model.Enums;
import com.dcp.scheduler.model.InstanceRecord;
import com.dcp.scheduler.model.NsInstanceRecord;
import com.dcp.scheduler.model.PartRecord;
import com.dcp.scheduler.store.EtcdStore;
import com.dcp.scheduler.store.Json;
import com.dcp.scheduler.store.KeySpace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * 分配器:把 PENDING 分区 CAS 分配到实例(总纲 §6.3 / 调度算法子文档 §三)。
 * 决策纯逻辑在此,写归属走 EtcdStore.casPut(读时 value 字节做条件,防陈旧主)。
 */
public class Allocator {

    private static final Logger log = LoggerFactory.getLogger(Allocator.class);

    private final String ns;
    private final EtcdStore store;
    private volatile Scoring scoring;

    public Allocator(String ns, EtcdStore store, Scoring scoring) {
        this.ns = ns; this.store = store; this.scoring = scoring;
    }

    /** 打分系数热更新(/dcp/{ns}/scoring 变化时重建)。 */
    public void setScoring(Scoring s) {
        this.scoring = s;
    }

    /**
     * 一轮分配:对每个 PENDING 分区选实例,CAS 写归属。
     * 锁裁决(子文档⑧ §三):钉住且锁未过期的分区只允许定向分配给 pinnedTo,
     * 目标不存活/不可接单则留 PENDING 并告警。
     *
     * @param pending       PENDING 分区及其"读时 value 字节"(CAS 条件)
     * @param candidates    本 ns 候选实例(调度注册)
     * @param metas         全局实例身份(取机器指标)
     * @param assigned      实例已分配分区数
     * @return 成功分配数
     */
    public int allocate(Map<String, byte[]> pending,
                        List<NsInstanceRecord> candidates,
                        Map<String, InstanceRecord> metas,
                        Map<String, Integer> assigned) {
        int ok = 0;
        for (Map.Entry<String, byte[]> e : pending.entrySet()) {
            String partKey = e.getKey();
            int partId = KeySpace.partIdOf(partKey);
            String taskId = KeySpace.taskIdOfPartKey(partKey);
            PartRecord prev = Json.fromBytes(e.getValue(), PartRecord.class); // 读时快照
            Optional<String> target;
            if (prev.locked()) {
                // 定向分配:只允许钉住目标,且目标必须存活可接单
                target = Optional.ofNullable(prev.pin.pinnedTo)
                        .filter(t -> acceptable(t, candidates, assigned));
                if (target.isEmpty()) {
                    log.warn("[{}] 分区 {}/{} 已钉住 {} 但目标不可接单,留 PENDING",
                            ns, taskId, partId, prev.pin.pinnedTo);
                    continue;
                }
            } else {
                target = scoring.pick(candidates, metas, assigned, null);
                if (target.isEmpty()) {
                    log.warn("[{}] 分区 {}/{} 无候选实例,留 PENDING", ns, taskId, partId);
                    continue;
                }
            }
            PartRecord next = new PartRecord();
            next.partId = partId;
            next.owner = target.get();
            next.state = Enums.PartState.ASSIGNED;
            next.epoch = prev.epoch + 1; // 防陈旧写
            next.assignedAt = System.currentTimeMillis(); // 卡死宽限期/再平衡分区冷却基准
            next.pin = prev.pin;         // 锁保留(分配不改变钉住)
            if (store.casPut(KeySpace.part(ns, taskId, partId), e.getValue(), next, null)) {
                assigned.merge(target.get(), 1, Integer::sum);
                log.info("[{}] 分配 {}/{} → {}{}", ns, taskId, partId, target.get(),
                        prev.locked() ? " (钉住定向)" : "");
                ok++;
            }
            // CAS 失败:已被并发分配,下轮重试
        }
        return ok;
    }

    /** 候选实例中按 id 查可接单性(槽位未满且 ONLINE 未暂停)。 */
    private static boolean acceptable(String id, List<NsInstanceRecord> candidates,
                                      Map<String, Integer> assigned) {
        for (NsInstanceRecord i : candidates) {
            if (id.equals(i.id)) {
                return i.acceptable() && assigned.getOrDefault(id, 0) < i.capacity;
            }
        }
        return false;
    }

    /** 实例已分配分区数统计(从 parts 读)。 */
    public Map<String, Integer> countAssigned(Map<String, byte[]> allParts) {
        Map<String, Integer> out = new HashMap<>();
        for (byte[] v : allParts.values()) {
            try {
                PartRecord p = Json.fromBytes(v, PartRecord.class);
                if (p.owner != null && Enums.PartState.ASSIGNED.equals(p.state)) {
                    out.merge(p.owner, 1, Integer::sum);
                }
            } catch (Exception ignored) { /* 跳过坏记录 */ }
        }
        return out;
    }
}
