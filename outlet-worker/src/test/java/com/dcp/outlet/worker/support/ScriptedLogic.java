package com.dcp.outlet.worker.support;

import com.dcp.outlet.sdk.Batch;
import com.dcp.outlet.sdk.ChangeRecord;
import com.dcp.outlet.sdk.ConsumerContext;
import com.dcp.outlet.sdk.ConsumerLogic;
import com.dcp.outlet.sdk.LogicInitContext;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/** 可脚本化逻辑:可指定"前 N 次 process 抛异常";记录收到的记录/线程/并发/上下文。 */
public class ScriptedLogic implements ConsumerLogic {

    private final AtomicInteger failFirstN;
    private int initCount, closeCount;
    private final AtomicInteger processCount = new AtomicInteger();
    private final List<ChangeRecord> received = new ArrayList<>();
    private final Set<String> threadNames = ConcurrentHashMap.newKeySet();
    private final AtomicInteger inFlight = new AtomicInteger();
    private final AtomicInteger maxConcurrent = new AtomicInteger();
    private final List<Long> safeWatermarks = new ArrayList<>();
    private final Map<String, Object> paramsSeen = new LinkedHashMap<>();
    private final AtomicInteger explicitCommits = new AtomicInteger();

    public ScriptedLogic(int failFirstN) { this.failFirstN = new AtomicInteger(failFirstN); }

    @Override
    public void init(LogicInitContext ctx) {
        initCount++;
        paramsSeen.putAll(ctx.params());
    }

    @Override
    public void process(Batch batch, ConsumerContext ctx) {
        processCount.incrementAndGet();
        threadNames.add(Thread.currentThread().getName());
        int cur = inFlight.incrementAndGet();
        maxConcurrent.accumulateAndGet(cur, Math::max);
        try {
            if (failFirstN.getAndDecrement() > 0) throw new IllegalStateException("scripted failure");
            synchronized (received) { received.addAll(batch.records()); }
            safeWatermarks.add(ctx.safeWatermark());
        } finally {
            inFlight.decrementAndGet();
        }
    }

    @Override
    public void close() { closeCount++; }

    public int initCount() { return initCount; }
    public int closeCount() { return closeCount; }
    public int processCount() { return processCount.get(); }
    public List<ChangeRecord> received() { return List.copyOf(received); }
    public Set<String> threadNames() { return threadNames; }
    public int maxConcurrent() { return maxConcurrent.get(); }
    public List<Long> safeWatermarks() { return List.copyOf(safeWatermarks); }
    public Map<String, Object> paramsSeen() { return paramsSeen; }
    public int explicitCommits() { return explicitCommits.get(); }

    public String columnsOfFirstRecord() {
        ChangeRecord r = received.isEmpty() ? null : received.get(0);
        return r == null || r.after() == null ? "" : String.join(",", r.after().keySet());
    }
}
