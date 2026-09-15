package com.dcp.outlet.control;

/** 失败策略(任务通用参数)。控制面只透传给计算面,不解释语义。 */
public final class FailureSpec {

    private final int maxRetries;
    private final long backoffMillis;
    private final String dlqPolicy;     // block | skip | dlq
    private final String onFailure;     // stop | continue

    public FailureSpec(int maxRetries, long backoffMillis, String dlqPolicy, String onFailure) {
        this.maxRetries = maxRetries; this.backoffMillis = backoffMillis;
        this.dlqPolicy = dlqPolicy; this.onFailure = onFailure;
    }

    public static FailureSpec defaults() { return new FailureSpec(5, 2000L, "dlq", "stop"); }

    public int maxRetries() { return maxRetries; }
    public long backoffMillis() { return backoffMillis; }
    public String dlqPolicy() { return dlqPolicy; }
    public String onFailure() { return onFailure; }
}
