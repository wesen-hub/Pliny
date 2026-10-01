package com.dcp.scheduler.model;

/**
 * 任务级派生指标(监控子文档 §二:分区聚合 + 容量水位;主侧每决策轮写入,后台直读)。
 */
public class TaskMetrics {

    public long ts;
    public String taskId;
    public String state;            // 任务状态(CREATED/RUNNING/DRAINING…)
    public int parts;               // 当前归属分区数
    public int expectedParts;       // 期望分区数(容量水位 = parts / expectedParts)
    public int minParts;
    public int maxParts;
    public double targetRps;
    public double rps;              // Σ 分区 rps(相邻帧差分)
    public long processedCount;     // Σ 累计处理条数
    public long errorCount;         // Σ 累计错误条数
    public double errorRate;        // Δerror / Δprocessed(最近帧)
    public double avgLatencyMs;     // ΔtotalLatency / Δprocessed(最近帧)
    public long lag;                // Σ 分区 lag(endOffset − committedOffset;无源 = -1)
}
