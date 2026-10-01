package com.dcp.scheduler.alloc;

import com.dcp.scheduler.model.Enums;
import com.dcp.scheduler.model.InstanceRecord;
import com.dcp.scheduler.model.NsInstanceRecord;
import com.dcp.scheduler.model.PartRecord;
import com.dcp.scheduler.model.RebalanceRules;
import com.dcp.scheduler.store.EtcdStore;
import com.dcp.scheduler.store.Json;
import com.dcp.scheduler.store.KeySpace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 再平衡器(调度算法子文档 §四):稳态负载均衡,与判死分工——故障接管优先,再平衡只在健康时进行。
 *   触发:槽位失衡(最忙占用率&gt;occupancyHi 且差&gt;gap)或进程 cpu 超载(cpu&gt;cpuHi),均持续 periods 周期。
 *   步进:单轮迁移 ≤ 1 个分区(迁完下一轮再评估);防抖:全局冷却/分区级冷却/目标实例冷却。
 *   锁:钉住分区跳过(子文档⑧ 锁语义矩阵:再平衡跳过)。
 *   迁移执行:
 *     - AGGRESSIVE 任务:直接 CAS 改 owner(实例对账自停自起,双消费由业务幂等吸收);
 *     - CONSERVATIVE 任务:走 MIGRATING 中间态 + 临时 TTL 锁定向目标(复用手动迁移形态,
 *       源主动停 stopped 快照推进、定向分配交给 pinnedTo、锁到期自动解锁恢复算法管理)。
 */
public class Rebalancer {

    private static final Logger log = LoggerFactory.getLogger(Rebalancer.class);

    private final String ns;
    private final EtcdStore store;
    private volatile Scoring scoring;

    /** 规则(运维后台热更新;缺省 = RebalanceRules 内置默认,子文档 §七)。 */
    private volatile RebalanceRules rules = new RebalanceRules();

    /** 防抖状态(主内存:换主重来可接受,防抖本不追求跨主)。 */
    private int imbalanceStreak;                     // 槽位失衡连续周期
    private final Map<String, Integer> cpuStreak = new HashMap<>(); // 实例 → cpu 超载连续周期
    private long rebalanceCoolUntil;                 // 全局冷却(失衡解除后起算)
    private final Map<String, Long> partCoolUntil = new HashMap<>();    // task/part → 冷却截止
    private final Map<String, Long> instCoolUntil = new HashMap<>();    // 实例 → 冷却截止(不作为源)

    public Rebalancer(String ns, EtcdStore store, Scoring scoring) {
        this.ns = ns;
        this.store = store;
        this.scoring = scoring;
    }

    /** 打分系数热更新(/dcp/{ns}/scoring 变化时重建)。 */
    public void setScoring(Scoring s) {
        this.scoring = s;
    }

    /** 热更新规则(null 复位内置默认)。 */
    public void updateRules(RebalanceRules r) {
        this.rules = r != null ? r : new RebalanceRules();
    }

    /**
     * 一轮再平衡:槽位失衡或 cpu 超载持续期判定 → 选迁移对 → 执行 ≤1 个分区迁移。
     *
     * @param parts     ASSIGNED 分区的"读时 value 字节"(CAS 条件);MIGRATING/PENDING 由其他环节处理
     * @param live      存活实例(调度注册)
     * @param assigned  实例已分配分区数
     * @param metas     实例 meta(机器指标:cpu 驱逐判定 + 目标选择差异化)
     * @param taskHandover taskId → handover 模式(AGGRESSIVE 直接改,CONSERVATIVE 走中间态)
     * @param rps       分区瞬时 rps(选迁移代价最小分区;快照差分滞后一轮可接受)
     * @return 迁移分区数(0/1)
     */
    public int rebalance(Map<String, byte[]> parts,
                         List<NsInstanceRecord> live,
                         Map<String, Integer> assigned,
                         Map<String, InstanceRecord> metas,
                         Map<String, String> taskHandover,
                         Map<String, Double> rps) {
        RebalanceRules r = rules;
        long now = System.currentTimeMillis();
        // 冷却内:不触发新一轮失衡再平衡(连续步进不受限:冷却在失衡解除后才起算)
        if (now < rebalanceCoolUntil) return 0;

        // ① 失衡量化:槽位最忙/最闲 + cpu 最高实例(超载连续期独立累计)
        double busy = 0, idle = Double.MAX_VALUE;
        String busyInst = null;
        for (NsInstanceRecord i : live) {
            if (!i.acceptable()) continue;
            double load = (double) assigned.getOrDefault(i.id, 0) / i.capacity;
            if (load > busy) { busy = load; busyInst = i.id; }
            if (load < idle) idle = load;
        }
        boolean slotImbalanced = busyInst != null && busy > r.occupancyHi && (busy - idle) > r.gap;

        // cpu 超载 streak(每实例独立;回落即清零)——"响应慢就迁移"的驱逐机制
        Map<String, Integer> newStreak = new HashMap<>();
        String cpuHotInst = null;
        int cpuHotVal = 0;
        for (NsInstanceRecord i : live) {
            if (!i.acceptable()) continue;
            InstanceRecord m = metas.get(i.id);
            int cpu = m != null ? m.meta.cpu : 0;
            if (cpu > r.cpuHi) {
                int s = cpuStreak.getOrDefault(i.id, 0) + 1;
                newStreak.put(i.id, s);
                if (s >= r.periods && cpu > cpuHotVal) { cpuHotInst = i.id; cpuHotVal = cpu; }
            }
        }
        cpuStreak.clear();
        cpuStreak.putAll(newStreak);

        // ② 槽位持续期防抖:连续 periods 个决策周期失衡才触发
        if (slotImbalanced) imbalanceStreak++; else imbalanceStreak = 0;
        if (imbalanceStreak < r.periods && cpuHotInst == null) return 0;

        // ③ 选迁移对:源 = cpu 超载实例优先,否则最忙槽位实例(不在实例冷却内)
        String src = cpuHotInst != null ? cpuHotInst : busyInst;
        if (src == null) return 0;
        if (now < instCoolUntil.getOrDefault(src, 0L)) {
            imbalanceStreak = Math.max(0, imbalanceStreak - 1); // 源冷却:推迟重估
            return 0;
        }
        Map.Entry<String, byte[]> picked = pickPartition(parts, src, rps, now);
        if (picked == null) { imbalanceStreak = 0; return 0; }

        // ④ 目标 = 非源实例中打分最高者(迁移前校验:存活、槽位未满、cpu 未超载)
        PartRecord prev = Json.fromBytes(picked.getValue(), PartRecord.class);
        String taskId = KeySpace.taskIdOfPartKey(picked.getKey());
        List<NsInstanceRecord> candidates = new ArrayList<>();
        for (NsInstanceRecord i : live) {
            if (src.equals(i.id) || !i.acceptable()
                    || assigned.getOrDefault(i.id, 0) >= i.capacity) continue;
            InstanceRecord m = metas.get(i.id);
            if (m != null && m.meta.cpu > r.cpuHi) continue; // 目标不接高 cpu(防踢皮球)
            candidates.add(i);
        }
        String target = scoring.pick(candidates, metas, assigned, null).orElse(null);
        if (target == null) { imbalanceStreak = 0; return 0; }

        // ⑤ 执行迁移
        if (migrate(picked.getKey(), picked.getValue(), prev, taskId, target,
                taskHandover.getOrDefault(taskId, ""), "rebalance")) {
            log.info("[{}] {} {}/{} {} → {}(占用率={} 差={} 源cpu={})",
                    ns, cpuHotInst != null ? "cpu 驱逐" : "再平衡迁移",
                    taskId, prev.partId, src, target, String.format("%.2f", busy),
                    String.format("%.2f", busy - idle), cpuHotVal);
            imbalanceStreak = 0;
            cpuStreak.remove(src); // 迁移后重新观察(再超载 3 周期后继续步进)
            // 防抖登记:分区级(该分区不再迁)+ 目标实例级(刚接过的实例不迁出)
            partCoolUntil.put(taskId + "/" + prev.partId, now + r.partCoolMinutes * 60_000L);
            instCoolUntil.put(target, now + r.targetCoolMinutes * 60_000L);
            // 若迁移后槽位仍失衡或仍有超载实例,下一轮继续步进;都不失衡才置全局冷却
            if (!stillImbalanced(live, assigned, metas, r)) rebalanceCoolUntil = now + r.cooldownMinutes * 60_000L;
            return 1;
        }
        return 0;
    }

    /** 源实例上瞬时 rps 最低的 ASSIGNED 分区(迁移代价最小者;跳过锁分区与分区冷却内分区)。 */
    private Map.Entry<String, byte[]> pickPartition(Map<String, byte[]> parts, String src,
                                                    Map<String, Double> rps, long now) {
        Map.Entry<String, byte[]> best = null;
        double bestRps = Double.MAX_VALUE;
        for (Map.Entry<String, byte[]> e : parts.entrySet()) {
            PartRecord p;
            try {
                p = Json.fromBytes(e.getValue(), PartRecord.class);
            } catch (Exception ex) {
                continue;
            }
            if (!src.equals(p.owner) || !Enums.PartState.ASSIGNED.equals(p.state)) continue;
            if (p.locked()) continue; // 钉住分区跳过(子文档⑧)
            String taskId = KeySpace.taskIdOfPartKey(e.getKey());
            if (now < partCoolUntil.getOrDefault(taskId + "/" + p.partId, 0L)) continue;
            double thr = rps.getOrDefault(taskId + "/" + p.partId, 0.0);
            if (thr < bestRps) {
                bestRps = thr;
                best = e;
            }
        }
        return best;
    }

    /** 执行迁移:AGGRESSIVE 直接改 owner;CONSERVATIVE 走 MIGRATING + 临时 TTL 锁定向目标。
     *  reason:交接起因(rebalance=再平衡 / instance-drain=实例下架迁出,回滚规则均不回滚)。 */
    public boolean migrate(String partKey, byte[] readValue, PartRecord prev, String taskId,
                           String target, String handover, String reason) {
        PartRecord next = new PartRecord();
        next.partId = prev.partId;
        next.owner = prev.owner;   // MIGRATING 期间归属仍是源(激进分支覆盖为目 标)
        next.state = prev.state;
        next.epoch = prev.epoch + 1;
        next.assignedAt = prev.assignedAt;
        next.pin = prev.pin;
        if (Enums.Handover.CONSERVATIVE.equals(handover)) {
            next.state = Enums.PartState.MIGRATING;
            next.handover = new PartRecord.HandoverState();
            next.handover.sourceOwner = prev.owner;
            next.handover.confirmDeadline = System.currentTimeMillis() + 30_000;
            next.handover.reason = reason;
            // 临时定向锁:交接完成后定向分配交给 pinnedTo;TTL 到期 expirePins 自动解锁恢复算法管理
            next.pin = new PartRecord.Pin();
            next.pin.pinnedTo = target;
            next.pin.pinnedAt = System.currentTimeMillis();
            next.pin.expiresAt = next.handover.confirmDeadline + 60_000;
            next.pin.reason = reason;
        } else {
            next.owner = target;
            next.assignedAt = System.currentTimeMillis();
        }
        boolean ok = store.casPut(KeySpace.part(ns, taskId, prev.partId), readValue, next, null);
        return ok;
    }

    /** 迁移执行后的复评:槽位是否仍失衡、是否仍有 cpu 超载实例(决定全局冷却起点)。 */
    private boolean stillImbalanced(List<NsInstanceRecord> live, Map<String, Integer> assigned,
                                    Map<String, InstanceRecord> metas, RebalanceRules r) {
        double busy = 0, idle = Double.MAX_VALUE;
        for (NsInstanceRecord i : live) {
            if (!i.acceptable()) continue;
            double load = (double) assigned.getOrDefault(i.id, 0) / i.capacity;
            busy = Math.max(busy, load);
            idle = Math.min(idle, load);
        }
        boolean slot = busy > r.occupancyHi && (busy - idle) > r.gap;
        for (NsInstanceRecord i : live) {
            if (!i.acceptable()) continue;
            InstanceRecord m = metas.get(i.id);
            if (m != null && m.meta.cpu > r.cpuHi) return true; // 仍有超载实例,继续步进
        }
        return slot;
    }
}
