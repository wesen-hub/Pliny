package com.dcp.outlet.worker.support;

import com.dcp.outlet.worker.Heartbeat;

import java.util.concurrent.atomic.AtomicInteger;

public class StubHeartbeat implements Heartbeat {

    private final AtomicInteger count = new AtomicInteger();
    private volatile long lastOffset;

    public int count() { return count.get(); }

    public long lastOffset() { return lastOffset; }

    @Override
    public void send(String unitId, int partition, long offset, long tso) {
        count.incrementAndGet();
        lastOffset = offset;
    }
}
