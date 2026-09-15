package com.dcp.outlet.integration.support;

import com.dcp.outlet.sdk.Batch;
import com.dcp.outlet.sdk.ChangeRecord;
import com.dcp.outlet.sdk.ConsumerContext;
import com.dcp.outlet.sdk.ConsumerLogic;
import com.dcp.outlet.sdk.LogicInitContext;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/** 幂等写内存表的逻辑(按业务主键 upsert / delete),用于端到端验证"不丢不重"。 */
public class MemorySinkLogic implements ConsumerLogic {

    private final Map<String, Map<String, Object>> sink;
    private final AtomicInteger applied = new AtomicInteger();
    private final AtomicInteger processCalls = new AtomicInteger();

    public MemorySinkLogic(Map<String, Map<String, Object>> sink) { this.sink = sink; }

    @Override public void init(LogicInitContext ctx) { }

    @Override
    public void process(Batch batch, ConsumerContext ctx) {
        processCalls.incrementAndGet();
        for (ChangeRecord r : batch.records()) {
            Map<String, Object> row = r.after();
            if (row == null) row = r.before();
            if (row == null) continue;
            String id = String.valueOf(row.get("id"));
            synchronized (sink) {
                if (r.op() == com.dcp.outlet.sdk.Op.DELETE) sink.remove(id);
                else sink.put(id, row);                        // 幂等:同 id 覆盖
            }
            applied.incrementAndGet();
        }
    }

    @Override public void close() { }

    public int applied() { return applied.get(); }

    public int processCalls() { return processCalls.get(); }
}
