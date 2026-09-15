package com.dcp.outlet.control.support;

import com.dcp.outlet.control.BatchRunner;

import java.util.ArrayList;
import java.util.List;

/** 记录"批阶段被触发",便于测试手动回调批完成(成功/失败)。 */
public class RecordingBatchRunner implements BatchRunner {

    private final List<String> started = new ArrayList<>();
    private BatchCallback callback;
    private String taskId;

    @Override public void runAsync(String taskId, BatchCallback callback) {
        this.taskId = taskId;
        this.callback = callback;
        started.add(taskId);
    }

    public int startedCount() { return started.size(); }

    /** 手动模拟批阶段完成。 */
    public void complete(long t0, boolean ok) {
        if (callback == null) throw new IllegalStateException("batch not started");
        callback.done(t0, ok);
    }
}
