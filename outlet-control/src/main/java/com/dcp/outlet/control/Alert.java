package com.dcp.outlet.control;

/** 巡检告警。平台**只告警不自动补齐**(D12):补容器/补单元由外部编排或人工负责。 */
public final class Alert {

    public enum Kind {
        /** 租约过期(单元可能已失联):需确认是否双消费风险 */
        LEASE_EXPIRED,
        /** 期望运行但单元未下发/缺失 */
        UNIT_MISSING,
        /** 单元长时间无心跳:疑似宕机或卡死 */
        UNIT_STALLED,
        /** 任务期望 running,但没有任何单元在 RUNNING */
        TASK_NOT_RUNNING,
        /** 批阶段失败:需人工重做批(换新 T0) */
        BATCH_FAILED
    }

    private final Kind kind;
    private final String taskId;
    private final String unitId;
    private final String detail;
    private final long at;

    public Alert(Kind kind, String taskId, String unitId, String detail, long at) {
        this.kind = kind; this.taskId = taskId; this.unitId = unitId; this.detail = detail; this.at = at;
    }

    public Kind kind() { return kind; }
    public String taskId() { return taskId; }
    public String unitId() { return unitId; }
    public String detail() { return detail; }
    public long at() { return at; }

    @Override public String toString() {
        return "[" + kind + "] task=" + taskId + (unitId == null ? "" : " unit=" + unitId) + " " + detail;
    }
}
