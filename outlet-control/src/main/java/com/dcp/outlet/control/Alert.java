package com.dcp.outlet.control;

/** 巡检告警。自动修复已落地(CP1 重放 onCreated / CP3 补注销);批失败/污染死信仍保持人工(D12)。 */
public final class Alert {

    public enum Kind {
        /** 租约过期(单元可能已失联):需确认是否双消费风险 */
        LEASE_EXPIRED,
        /** 期望运行但单元未下发/缺失(自动重放 onCreated 修复) */
        UNIT_MISSING,
        /** 单元长时间无心跳:疑似宕机或卡死 */
        UNIT_STALLED,
        /** 任务期望 running,但没有任何单元在 RUNNING */
        TASK_NOT_RUNNING,
        /** 批阶段失败:需人工重做批(换新 T0) */
        BATCH_FAILED,
        /** 批已完成但交接未落地(activate 失败/控制面中断):自动补交接(幂等 put 覆盖) */
        HANDOVER_PENDING,
        /** 期望停止但框架 spec 残留(自动补注销) */
        SPEC_ORPHAN,
        /** 索引器分区解析失败污染(C1 死信):该分区不再产栅栏, 查询退化 fallback;人工确认后 DELETE 恢复 */
        INDEXER_POISONED,
        /** 索引器消费/位点提交异常(常驻循环不退出, 继续重试) */
        INDEXER_ERROR
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
