package com.dcp.scheduler.model;

/** 分区归属记录:调度主写 CAS,实例 watch。 */
public class PartRecord {

    public int partId;
    public String owner;            // 实例 id;null = PENDING
    public String state = Enums.PartState.PENDING;
    public long epoch;              // CAS 条件,防陈旧主/并发写
    public long assignedAt;         // 最近一次分配时刻(卡死宽限期/再平衡分区冷却基准)
    public HandoverState handover;  // 保守交接中间态(持久化,换主可续);激进交接为 null
    public Pin pin;                 // 钉住锁(子文档⑧);null = 无锁,受算法自动管理
    public DrainState drain;        // DELETING 中间态(缩容追平/注销),持久化换主可续

    public static class HandoverState {
        public String sourceOwner;  // 迁移前归属
        public long confirmDeadline;// 判死双窗口 deadline(epoch millis)
        /** 交接起因:death=判死 / stuck=卡死 / manual=手动迁移 / rebalance=再平衡。
         *  回滚规则:death 看源 lease 复活;stuck 看源快照恢复;manual/rebalance 不回滚。 */
        public String reason;
    }

    /** 缩容/注销追平中间态(扩缩容子文档 §四):钩子切路由后置 DELETING,追平判 lag 归零后清记录。 */
    public static class DrainState {
        public String reason;      // 起因:scale-in=缩容 / drain-task=注销
        public long deadline;      // 追平 deadline(epoch millis),超时告警不 自动回滚
        public long routeCutAt;    // 钩子切路由完成时间(幂等重调基准)
        public String topic;       // 置 DELETING 时的 Kafka topic(lag 实查;注销后 spec 已删仍可查)
    }

    /** 钉住锁:运维意图优先于算法最优。锁与归属同键原子,不存在中间态。 */
    public static class Pin {
        public String pinnedTo;     // 钉住目标实例 id
        public long pinnedAt;       // 钉住时间
        public Long expiresAt;      // null = 永久锁;有值 = TTL 锁,到期自动解锁
        public String reason;       // 运维备注
    }

    public boolean assignedTo(String instId) {
        return Enums.PartState.ASSIGNED.equals(state) && instId.equals(owner);
    }

    /** 锁是否生效(TTL 已过视为无锁,交由 expirePins 清场)。 */
    public boolean locked() {
        return pin != null && (pin.expiresAt == null || pin.expiresAt > System.currentTimeMillis());
    }

    @Override
    public String toString() {
        return "part=" + partId + " state=" + state + " owner=" + owner + " epoch=" + epoch
                + (pin != null ? " pinnedTo=" + pin.pinnedTo : "");
    }
}
