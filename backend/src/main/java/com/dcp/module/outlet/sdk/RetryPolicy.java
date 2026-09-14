package com.dcp.module.outlet.sdk;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.Callable;

/**
 * 退避重试策略(设计 §6.1 SDK 职责)。
 *
 * <p>分类处置:
 * <ul>
 *   <li>{@code 5xx/IO} 与 **配额超限** → 指数退避重试;</li>
 *   <li>**位点过期 / epoch 变化** → 不重试,交由 {@link OutletConsumer} 走重快照;</li>
 *   <li>**权限失败** → 不重试,直接失败(需要人工修契约)。</li>
 * </ul>
 */
@Slf4j
public class RetryPolicy {

    /** 可注入的睡眠实现,便于单测不真的等 */
    public interface Sleeper {
        void sleep(long ms) throws InterruptedException;
    }

    private final int maxAttempts;
    private final long baseDelayMs;
    private final long maxDelayMs;
    private final Sleeper sleeper;

    public RetryPolicy(int maxAttempts, long baseDelayMs, long maxDelayMs, Sleeper sleeper) {
        this.maxAttempts = Math.max(1, maxAttempts);
        this.baseDelayMs = baseDelayMs;
        this.maxDelayMs = maxDelayMs;
        this.sleeper = sleeper == null ? Thread::sleep : sleeper;
    }

    public static RetryPolicy defaults() {
        return new RetryPolicy(3, 200, 5000, null);
    }

    public <T> T execute(String what, Callable<T> action) {
        RuntimeException last = null;
        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            try {
                return action.call();
            } catch (OutletApiException e) {
                if (e.isForbidden() || e.needsResnapshot()) {
                    throw e;              // 不可重试类:交给上层决定(重快照 / 失败)
                }
                last = e;
            } catch (Exception e) {
                last = e instanceof RuntimeException re ? re : new RuntimeException(e);
            }
            if (attempt < maxAttempts) {
                long delay = Math.min(maxDelayMs, baseDelayMs * (1L << (attempt - 1)));
                log.warn("[outlet-sdk] {} 第 {} 次失败({}),{}ms 后重试", what, attempt,
                        last == null ? "-" : last.getMessage(), delay);
                try {
                    sleeper.sleep(delay);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("重试被中断", ie);
                }
            }
        }
        throw last == null ? new RuntimeException(what + " 失败") : last;
    }

    public int maxAttempts() {
        return maxAttempts;
    }
}
