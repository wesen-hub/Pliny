package com.dcp.outlet.worker;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/** 计数器版指标(测试 / 无 Prometheus 环境)。 */
public class SimpleUnitMetrics implements UnitMetrics {

    private final Map<String, AtomicLong> counters = new ConcurrentHashMap<>();
    private final AtomicLong rows = new AtomicLong();
    private final AtomicLong batches = new AtomicLong();
    private final AtomicLong commits = new AtomicLong();

    private AtomicLong c(String name) { return counters.computeIfAbsent(name, k -> new AtomicLong()); }

    @Override public void batchProcessed(int p, int r, long d, long tso) { batches.incrementAndGet(); rows.addAndGet(r); }

    @Override public void emptyBatch(int p) { c("emptyBatch").incrementAndGet(); }

    @Override public void failure(int p, Throwable e) { c("failure").incrementAndGet(); }

    @Override public void retry(int p, int attempt) { c("retry").incrementAndGet(); }

    @Override public void deadLetter(int p) { c("dlq").incrementAndGet(); }

    @Override public void commit(int p, long offset) { commits.incrementAndGet(); }

    public long batches() { return batches.get(); }
    public long rows() { return rows.get(); }
    public long commits() { return commits.get(); }
    public long count(String name) { return c(name).get(); }
}
