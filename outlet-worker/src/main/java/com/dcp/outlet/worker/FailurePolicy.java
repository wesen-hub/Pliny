package com.dcp.outlet.worker;

/** 失败策略(任务通用参数的一部分)。 */
public final class FailurePolicy {

    /** 重试耗尽后的行为。 */
    public enum OnFailure {
        /** 停止该单元(默认):不丢数据,但要人工介入 */
        STOP,
        /** 跳过该批并提交,继续消费(仅用于确认可丢的场景) */
        SKIP
    }

    private final int maxRetries;
    private final long backoffMillis;
    private final OnFailure onFailure;

    public FailurePolicy(int maxRetries, long backoffMillis, OnFailure onFailure) {
        this.maxRetries = maxRetries;
        this.backoffMillis = backoffMillis;
        this.onFailure = onFailure;
    }

    public static FailurePolicy defaults() {
        return new FailurePolicy(5, 2000L, OnFailure.STOP);
    }

    public int maxRetries() { return maxRetries; }

    public long backoffMillis() { return backoffMillis; }

    public OnFailure onFailure() { return onFailure; }
}
