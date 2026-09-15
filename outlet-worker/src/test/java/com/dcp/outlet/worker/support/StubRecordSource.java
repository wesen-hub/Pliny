package com.dcp.outlet.worker.support;

import com.dcp.outlet.worker.RawMessage;
import com.dcp.outlet.worker.RecordSource;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/** 脚本化消费源:按脚本逐批吐出消息;脚本耗尽后返回空批。 */
public class StubRecordSource implements RecordSource {

    private final List<List<RawMessage>> script = new ArrayList<>();
    private final AtomicInteger batchIndex = new AtomicInteger();
    private final Map<Integer, Long> committed = new LinkedHashMap<>();
    private final Map<Integer, Long> seeked = new LinkedHashMap<>();
    private final AtomicInteger pollCount = new AtomicInteger();
    private volatile boolean woken;

    public StubRecordSource script(List<RawMessage> batch) {
        script.add(batch);
        return this;
    }

    public int pollCount() { return pollCount.get(); }

    public boolean woken() { return woken; }

    public Map<Integer, Long> committed() { return committed; }

    public Map<Integer, Long> seeked() { return seeked; }

    public int commitCalls() { return commitCalls.get(); }

    private final AtomicInteger commitCalls = new AtomicInteger();

    @Override
    public List<RawMessage> poll(long timeoutMillis) {
        pollCount.incrementAndGet();
        int i = batchIndex.getAndIncrement();
        return i < script.size() ? script.get(i) : List.of();
    }

    @Override
    public void seek(int partition, long offset) { seeked.put(partition, offset); }

    @Override
    public void commitSync(int partition, long offset) {
        committed.put(partition, offset);
        commitCalls.incrementAndGet();
    }

    @Override
    public void wakeup() { woken = true; }

    @Override
    public void close() { }
}
