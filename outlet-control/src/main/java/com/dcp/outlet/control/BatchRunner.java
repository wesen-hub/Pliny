package com.dcp.outlet.control;

/**
 * 批阶段执行端口(不碰 Kafka):用**同一个 logic** 以 {@code mode=SNAPSHOT} 打底,
 * 产出边界 T0;失败由人工触发重做(换新 T0)。
 */
public interface BatchRunner {

    void runAsync(String taskId, BatchCallback callback);

    interface BatchCallback {
        void done(long t0, boolean ok);
    }
}
