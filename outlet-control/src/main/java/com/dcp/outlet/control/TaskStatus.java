package com.dcp.outlet.control;

/** 任务状态机。 */
public enum TaskStatus {
    CREATED,        // 已落库,尚未推进
    BATCH_RUNNING,  // 批阶段打底中
    BATCH_DONE,     // 批完成,startTso 已写
    BATCH_FAILED,   // 批失败 → 人工触发重做批(换新 T0)
    RUNNING,        // 至少一个单元已下发(首个心跳后由计算面上报确认)
    PAUSED,
    STOPPED,
    FAILED
}
