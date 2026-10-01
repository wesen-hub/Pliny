package com.dcp.outlet.control.support;

import com.dcp.outlet.control.FrameworkTaskSink;
import com.dcp.outlet.control.TaskRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** 记录型框架出口(彻底版):捕获 TaskScheduler 提交的任务级登记/激活/暂停/停止/批失败。 */
public class RecordingFrameworkTaskSink implements FrameworkTaskSink {

    /** 一次 activate 的参数快照(任务 + 流 + 起点表)。 */
    public record Activation(String taskId, String topic, String kafkaBootstrap, String stream,
                             Map<Integer, Long> startOffsets, Map<Integer, Long> startTsos) {}

    private final List<String> batching = new ArrayList<>();
    private final List<Activation> activations = new ArrayList<>();
    private final List<String> cancelled = new ArrayList<>();
    private final List<String> batchFailed = new ArrayList<>();
    private final List<String[]> phases = new ArrayList<>();

    @Override public void submitBatching(TaskRecord task, String topic, String stream, int partitions) {
        batching.add(task.taskId() + "#" + stream);
    }

    @Override public void activate(TaskRecord task, String topic, String kafkaBootstrap, String stream,
                                   Map<Integer, Long> startOffsets, Map<Integer, Long> startTsos) {
        activations.add(new Activation(task.taskId(), topic, kafkaBootstrap, stream, startOffsets, startTsos));
    }

    @Override public void setPhase(String namespace, String taskId, String phase) {
        phases.add(new String[]{taskId, phase});
    }

    @Override public void cancelTask(String namespace, String taskId) { cancelled.add(taskId); }

    @Override public void markBatchFailed(String namespace, String taskId, String detail) { batchFailed.add(taskId); }

    public List<String> batching() { return List.copyOf(batching); }

    public List<Activation> activations() { return List.copyOf(activations); }

    public List<String> cancelled() { return List.copyOf(cancelled); }

    public List<String> batchFailed() { return List.copyOf(batchFailed); }

    public int count() { return activations.size(); }
}
