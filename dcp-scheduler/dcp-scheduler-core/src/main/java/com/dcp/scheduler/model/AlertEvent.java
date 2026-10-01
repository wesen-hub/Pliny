package com.dcp.scheduler.model;

/**
 * 告警事件(监控子文档 §六):主侧判定,写事件流键(后台页面可见,滚动保留)。
 * 维度:一条事件带全维度字段(ns/实例/任务/分区,不涉及留空),页面按实例维度聚合、可下钻筛选。
 * 生命周期:同对象同类型 10min 去重;状态恢复时同一 alertId 覆盖写 resolvedAt(页面显示"已恢复")。
 */
public class AlertEvent {

    public String alertId;       // UUID(事件流键后缀,恢复时覆盖同一键)
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
