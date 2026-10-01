package com.dcp.scheduler.model;

import java.util.Map;

/**
 * 任务契约:业务外壳提交,框架只读。
 * 五组字段:① 基础标识 ② 并发边界 ③ 执行契约 ④ 协作与规则 ⑤ 业务元数据。
 */
public class TaskSpec {

    // ① 基础标识
    public String namespace;
    public String taskId;
    public String topic;
    public int partitions;          // 初始分区数 = 初始并发度

    // ② 并发边界(扩缩容决策域)
    public int minPartitions;
    public int maxPartitions;
    public long targetRps;          // 单分区目标吞吐

    // ③ 执行契约
    public HandlerRef handlerRef;
    public String handover = Enums.Handover.AGGRESSIVE;

    // ④ 协作与规则(可选)
    public String scaleDownHooks;   // null = 只扩不缩
    public ScaleRules scaleRules;   // null = 继承 ns 默认

    /** 任务阶段(外壳提交,框架只读;null = RUNNING 兼容旧语义):
     *  BATCHING = 批阶段,框架只登记不分配;PAUSED = 任务级暂停,分区追平回收。 */
    public String phase;

    // ⑤ 业务元数据(框架不解释,原样透传执行器)
    public Map<String, String> bizMeta;

    /** 执行器装载引用。 */
    public static class HandlerRef {
        public String type;         // EMBEDDED | SPI
        public String ref;          // 内置执行器名 | 插件名
    }

    public boolean conservative() {
        return Enums.Handover.CONSERVATIVE.equals(handover);
    }

    public boolean batching() {
        return "BATCHING".equals(phase);
    }

    public boolean paused() {
        return "PAUSED".equals(phase);
    }

    /** 一次性任务(文件同步等; F1): 执行器上报 done → 框架置 SUCCEEDED 释放槽位, 不重派。 */
    public boolean once() {
        return "ONCE".equals(phase);
    }

    @Override
    public String toString() {
        return "TaskSpec{" + namespace + "/" + taskId + " topic=" + topic
                + " parts=" + partitions + " [" + minPartitions + "," + maxPartitions + "]"
                + " targetRps=" + targetRps + " handler="
                + (handlerRef == null ? "?" : handlerRef.type + ":" + handlerRef.ref)
                + " handover=" + handover + "}";
    }
}
