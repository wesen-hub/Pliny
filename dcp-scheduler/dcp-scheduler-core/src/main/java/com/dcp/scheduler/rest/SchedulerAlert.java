package com.dcp.scheduler.rest;

/**
 * 统一调度框架告警事件(只读展示模型):主写 /dcp/{ns}/alerts/{alertId}(滚动保留)。
 * 维度:一条事件带全维度字段(ns/实例/任务/分区,不涉及留空),页面按实例维度聚合、可下钻筛选。
 */
public class SchedulerAlert {

    public String alertId;
    public String ns;
    public String level;         // critical | warning
    public String type;          // SUSPECT / EXEC_STUCK / DRAIN_TIMEOUT / LAG_HIGH / ERROR_RATE
                                 // / CAPACITY_FULL / MIGRATE_TIMEOUT / NO_INSTANCE
    public String instanceId;    // 实例维度(可空)
    public String taskId;        // 任务维度(可空)
    public Integer partId;       // 分区维度(可空)
    public String message;
    public long since;           // 首次触发时间
    public long resolvedAt;      // 恢复时间(0 = 未恢复)
}
