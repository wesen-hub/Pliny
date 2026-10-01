package com.dcp.scheduler.model;

/** 任务状态(/dcp/{ns}/tasks/{taskId}/status,调度主写)。 */
public class TaskStatus {

    public String state = Enums.TaskState.SUBMITTED;
    public long since;
    public String reason;
    /** 框架期望并发分区数(扩缩容决策域):初始 = spec.partitions;扩/缩容决策推进。
     *  null = 未初始化(按 spec.partitions 处理)。ensureParts 按此补分区,防缩容删除的分区被补回。 */
    public Integer expectedParts;
    /** 上次对账时的 spec.partitions:spec.partitions 变大 = 外壳扩了 Kafka → 期 望分区数上调。
     *  逻辑缩容不改 spec.partitions,故不能用 spec>expected 判对账(缩容后 spec>expected 是稳态)。 */
    public Integer lastSpecPartitions;
    /** 上线时的 Kafka topic:注销后 spec 已删,仍靠它给 DELETING 分区查 lag。 */
    public String topic;
    /** 判死重派累计次数(一次性任务 F3 重试上限用; 重新提交/上线重置)。 */
    public int reassigns;

    public static TaskStatus of(String state, String reason) {
        TaskStatus s = new TaskStatus();
        s.state = state;
        s.since = System.currentTimeMillis();
        s.reason = reason;
        return s;
    }
}
