package com.dcp.module.outlet;

import com.dcp.common.BizException;
import com.dcp.module.outlet.model.ConsumerAccount;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 配额与限流(设计 §7.2 第 ⑤ 道判定)。
 *
 * <p>维度:QPS / 日行数 / 并发。超限一律抛 {@link OutletErrorCode#QUOTA_EXCEEDED}(对应 HTTP 429 语义)。
 *
 * <p>骨架实现为**单实例内存计数**;多实例部署时应替换为集中计数(Redis 或元库计数表),
 * 否则 N 个实例 = N 倍配额。
 */
@Component
public class OutletQuotaLimiter {

    private static final class Bucket {
        final AtomicInteger qps = new AtomicInteger();
        final AtomicLong windowSecond = new AtomicLong();
        final AtomicLong rowsToday = new AtomicLong();
        final AtomicInteger concurrent = new AtomicInteger();
        volatile LocalDate day = LocalDate.now();
    }

    private final Map<String, Bucket> buckets = new ConcurrentHashMap<>();

    private Bucket bucket(String consumerId) {
        return buckets.computeIfAbsent(consumerId, k -> new Bucket());
    }

    /** 请求进入时调用:校验 QPS 与并发;返回值请务必在 finally 里 {@link #release} */
    public void acquire(ConsumerAccount c) {
        Bucket b = bucket(c.getConsumerId());
        long nowSec = System.currentTimeMillis() / 1000;
        if (b.windowSecond.getAndSet(nowSec) != nowSec) {
            b.qps.set(0);
        }
        if (b.qps.incrementAndGet() > c.getQuotaQps()) {
            b.qps.decrementAndGet();
            throw new BizException(OutletErrorCode.QUOTA_EXCEEDED,
                    "QPS 超限:" + b.qps.get() + " > " + c.getQuotaQps());
        }
        if (b.concurrent.incrementAndGet() > c.getQuotaConcurrent()) {
            b.concurrent.decrementAndGet();
            b.qps.decrementAndGet();
            throw new BizException(OutletErrorCode.QUOTA_EXCEEDED,
                    "并发超限:" + b.concurrent.get() + " > " + c.getQuotaConcurrent());
        }
        rollDay(b);
    }

    public void release(ConsumerAccount c) {
        bucket(c.getConsumerId()).concurrent.decrementAndGet();
    }

    /** 行数计入(按实际下发行数),超日限额直接抛 */
    public void addRows(ConsumerAccount c, long rows) {
        Bucket b = bucket(c.getConsumerId());
        rollDay(b);
        long total = b.rowsToday.addAndGet(rows);
        if (total > c.getQuotaRowsPerDay()) {
            throw new BizException(OutletErrorCode.QUOTA_EXCEEDED,
                    "日行数超限:" + total + " > " + c.getQuotaRowsPerDay());
        }
    }

    private void rollDay(Bucket b) {
        LocalDate today = LocalDate.now();
        if (!today.equals(b.day)) {
            b.day = today;
            b.rowsToday.set(0);
        }
    }

    public Map<String, Object> snapshot(String consumerId) {
        Bucket b = bucket(consumerId);
        return Map.of("qps", b.qps.get(), "concurrent", b.concurrent.get(), "rowsToday", b.rowsToday.get());
    }
}
