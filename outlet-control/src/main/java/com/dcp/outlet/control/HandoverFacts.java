package com.dcp.outlet.control;

import java.util.Optional;

/**
 * 批交接物化事实端口:handover-pending 判据(CP2b 修复路径)的数据源。
 *
 * <p>批阶段终态物化在元库 batch_job 行;activate 失败(或控制面中断)后批已 SUCCEEDED
 * 但 handover_at 未回写,该行即 handover-pending 判据——Supervisor 巡检时读 t0 直接补交接,
 * 而不是把 activate 失败归并进批失败告警(批本身没失败,重做批是错误药方)。
 *
 * <p>outlet-control 内核保持零元库依赖:本端口只定义契约,实现由 outlet-adapters 装配(BatchJobStore)。
 */
public interface HandoverFacts {

    /** 批 SUCCEEDED ∧ 未交接 → 交接输入 t0(判据与数据一体);否则 empty。 */
    Optional<Long> pendingT0(String taskId);

    /** 交接完成回写(CP2b 判据收敛依据:补发成功即回写,否则判据永不收敛、每轮重复补发)。 */
    void markHandover(String taskId);
}
