package com.dcp.outlet.control;

/**
 * 框架任务探针端口:巡检器(Supervisor)读框架事实,不依赖框架类型。
 * 实现由 outlet-service 装配(读 etcd spec/status/事件键)。
 */
public interface TaskFrameworkProbe {

    /** 任务在框架的 phase 视图:null=框架无此任务(未登记/已注销);BATCHING/PAUSED/RUNNING(派生)。 */
    String phaseOf(String namespace, String taskId);

    /** 批阶段是否已失败(框架事件键 batch-failed 存在)。 */
    boolean batchFailed(String namespace, String taskId);

    /** 框架任务状态(SUBMITTED/RUNNING/DRAINING/GONE 等;无任务返回 null)。 */
    String stateOf(String namespace, String taskId);

    /** 注销框架 TaskSpec(CP3 自动修复:期望停止但 spec 残留 → 补注销;deleteSpec 幂等)。 */
    void cancelTask(String namespace, String taskId);
}
