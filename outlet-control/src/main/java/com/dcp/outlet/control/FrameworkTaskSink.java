package com.dcp.outlet.control;

import java.util.Map;

/**
 * 框架接管端口(彻底版):任务级下发/生命周期控制。
 *
 * <p>一个消费任务 = 框架一个 TaskSpec(partitions = 真实分区数),归属分配/判死/迁移/
 * <b>防双消费(框架归属键唯一)</b>统一由框架负责;租约/单元/推模式下发已整体退位。
 *
 * <p>outlet-control 内核保持零框架依赖:本端口只定义契约,实现由 outlet-service 装配。
 */
public interface FrameworkTaskSink {

    /** 批阶段登记:写 TaskSpec(phase=BATCHING),框架只登记不分配(等批完成推进)。 */
    void submitBatching(TaskRecord task, String topic, String stream, int partitions);

    /** 批完成/直接交接:更新 TaskSpec(去 phase + bizMeta 起点表)→ 框架推进 RUNNING 分配。
     *
     *  <p>{@code kafkaBootstrap}: 流所在 Kafka 地址随指令透传(ns 与 Kafka 解耦);缺 = 计算面 ns 环境。 */
    void activate(TaskRecord task, String topic, String kafkaBootstrap, String stream,
                  Map<Integer, Long> startOffsets, Map<Integer, Long> startTsos);

    /** 任务级暂停/恢复:spec.phase = PAUSED / null(框架追平回收 / 重新分配)。 */
    void setPhase(String namespace, String taskId, String phase);

    /** 任务停止:删 TaskSpec(框架推进 DRAINING → GONE)。 */
    void cancelTask(String namespace, String taskId);

    /** 批失败:写框架事件键 batch-failed(巡检读此告警;spec 保持 BATCHING 供重做批)。 */
    void markBatchFailed(String namespace, String taskId, String detail);
}
