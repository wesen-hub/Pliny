package com.dcp.outlet.control;

import java.util.List;

/** 单元运行状态来源(生产实现 = consumer_unit 表 + 心跳上报;测试 = 内存)。 */
public interface UnitStatusStore {

    /** 单元状态快照。 */
    final class UnitStatus {
        public final String unitId;
        public final String taskId;
        public final String state;        // PENDING | DISPATCHED | RUNNING | PAUSED | FAILED
        public final long lastOffset;
        public final long lastTso;
        public final long lastSeenAt;     // 最近一次心跳(ms);0 = 从未
        public final long dispatchedAt;   // 下发时刻(ms);0 = 尚未下发

        public UnitStatus(String unitId, String taskId, String state,
                          long lastOffset, long lastTso, long lastSeenAt, long dispatchedAt) {
            this.unitId = unitId; this.taskId = taskId; this.state = state;
            this.lastOffset = lastOffset; this.lastTso = lastTso;
            this.lastSeenAt = lastSeenAt; this.dispatchedAt = dispatchedAt;
        }
    }

    List<UnitStatus> byTask(String taskId);

    List<UnitStatus> all();
}
