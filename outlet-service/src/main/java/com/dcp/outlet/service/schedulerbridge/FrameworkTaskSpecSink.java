package com.dcp.outlet.service.schedulerbridge;

import com.dcp.outlet.control.FrameworkTaskSink;
import com.dcp.outlet.control.TaskRecord;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * {@link FrameworkTaskSink} 生产实现(彻底版):任务级登记/交接/暂停/停止全部转 TaskSpec。
 * 归属分配 / 判死 / 迁移 / 防双消费(框架归属键唯一)由框架统一负责。
 */
@Component
public class FrameworkTaskSpecSink implements FrameworkTaskSink {

    private final OutletFrameworkRuntime runtime;

    public FrameworkTaskSpecSink(OutletFrameworkRuntime runtime) {
        this.runtime = runtime;
    }

    @Override
    public void submitBatching(TaskRecord task, String topic, String stream, int partitions) {
        runtime.submitBatching(task, topic, stream, partitions);
    }

    @Override
    public void activate(TaskRecord task, String topic, String kafkaBootstrap, String stream,
                         Map<Integer, Long> startOffsets, Map<Integer, Long> startTsos) {
        runtime.activate(task, topic, kafkaBootstrap, stream, startOffsets, startTsos);
    }

    @Override
    public void setPhase(String namespace, String taskId, String phase) {
        runtime.setPhase(namespace, taskId, phase);
    }

    @Override
    public void cancelTask(String namespace, String taskId) {
        runtime.cancelTask(namespace, taskId);
    }

    @Override
    public void markBatchFailed(String namespace, String taskId, String detail) {
        runtime.markBatchFailed(namespace, taskId, detail);
    }
}
