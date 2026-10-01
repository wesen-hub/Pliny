package com.dcp.scheduler.model;

/**
 * 分区级派生指标(监控子文档 §二:四级下钻最底层;主侧每决策轮写入,后台直读)。
 * 无快照分区(PENDING/卡死)计数与 lag 置 0/-1,状态字段保证下钻页面可渲染。
 */
public class PartMetrics {

    public long ts;
    public String taskId;
    public int partId;
    public String state;        // PENDING/ASSIGNED/MIGRATING/DELETING
    public String owner;        // 实例 id;null = 未分配
    public double rps;          // 相邻帧差分(条/秒;无快照 = 0)
    public long processedCount; // 累计处理条数
    public long committedOffset;// 已提交位点
    public long errorCount;     // 累计错误条数
    public double errorRate;    // Δerror / Δprocessed(最近帧;无差分 = 0)
    public double avgLatencyMs; // ΔtotalLatency / Δprocessed(最近帧;无差分 = 0)
    public long lag;            // endOffset − committedOffset;无 Kafka 源 = -1
}
