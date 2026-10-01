package com.dcp.scheduler.model;

/**
 * 执行器快照(四个原始计数器):执行器 10s 续写,TTL 30s。
 * 业务只维护累计值,零指标概念;所有指标由框架派生。
 */
public class ExecutorSnapshot {

    public long ts;
    public String taskId;
    public int partId;
    public long processedCount;     // 累计处理条数(提交成功才算)
    public long committedOffset;    // 已提交位点(执行器自管,框架仅用于 lag 计算)
    public long errorCount;         // 累计错误条数
    public long totalLatencyMs;     // 累计处理耗时
    public boolean stopped;         // 主动停的最后一帧(区分"主动停"与"卡死")

    public static ExecutorSnapshot of(String taskId, int partId, long processed,
                                      long offset, long errors, long latency, boolean stopped) {
        ExecutorSnapshot s = new ExecutorSnapshot();
        s.ts = System.currentTimeMillis();
        s.taskId = taskId;
        s.partId = partId;
        s.processedCount = processed;
        s.committedOffset = offset;
        s.errorCount = errors;
        s.totalLatencyMs = latency;
        s.stopped = stopped;
        return s;
    }
}
