package com.dcp.scheduler.model;

/** 状态与模式枚举(字符串化存储,便于 JSON 与跨语言扩展)。 */
public final class Enums {

    /** 分区归属状态。 */
    public static final class PartState {
        public static final String PENDING = "PENDING";     // 无归属,待分配
        public static final String ASSIGNED = "ASSIGNED";   // 归属确定
        public static final String MIGRATING = "MIGRATING"; // 保守交接中间态
        public static final String DELETING = "DELETING";   // 缩容追平/注销中
        private PartState() {}
    }

    /** 任务状态。 */
    public static final class TaskState {
        public static final String SUBMITTED = "SUBMITTED";
        public static final String RUNNING = "RUNNING";
        public static final String SUCCEEDED = "SUCCEEDED";   // 一次性任务终态(执行器上报 done, 分区槽位已释放)
        public static final String FAILED = "FAILED";         // 一次性任务终态(判死重派达上限)
        public static final String DRAINING = "DRAINING";
        public static final String GONE = "GONE";
        private TaskState() {}
    }

    /** 任务阶段(TaskSpec.phase, 框架只读)。 */
    public static final class Phase {
        public static final String BATCHING = "BATCHING";
        public static final String PAUSED = "PAUSED";
        /** 一次性任务(文件同步等): 执行器上报 done(stopped 快照) → 框架置 SUCCEEDED 释放槽位, 不重派。 */
        public static final String ONCE = "ONCE";
        private Phase() {}
    }

    /** 实例(每 ns 调度注册)状态。 */
    public static final class InstStatus {
        public static final String ONLINE = "ONLINE";
        public static final String DRAINING = "DRAINING";
        public static final String SUSPECT = "SUSPECT";
        private InstStatus() {}
    }

    /** 交接模式。 */
    public static final class Handover {
        public static final String AGGRESSIVE = "AGGRESSIVE";   // 幂等兜底,直接接管
        public static final String CONSERVATIVE = "CONSERVATIVE"; // 双窗口确认死亡
        private Handover() {}
    }

    /** 执行器装载类型。 */
    public static final class HandlerType {
        public static final String EMBEDDED = "EMBEDDED"; // 框架内置
        public static final String SPI = "SPI";           // 业务 SPI 插件
        private HandlerType() {}
    }

    private Enums() {}
}
