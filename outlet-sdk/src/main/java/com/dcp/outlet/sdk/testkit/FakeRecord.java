package com.dcp.outlet.sdk.testkit;

import com.dcp.outlet.sdk.ChangeRecord;
import com.dcp.outlet.sdk.Op;

import java.util.LinkedHashMap;
import java.util.Map;

/** 测试替身:构造一条变更记录。 */
public final class FakeRecord implements ChangeRecord {

    private Op op = Op.INSERT;
    private Map<String, Object> key = new LinkedHashMap<>();
    private Map<String, Object> before = new LinkedHashMap<>();
    private Map<String, Object> after = new LinkedHashMap<>();
    private long tso;
    private int partition;
    private long offset;

    public static FakeRecord of(Op op, long tso) {
        FakeRecord r = new FakeRecord();
        r.op = op;
        r.tso = tso;
        return r;
    }

    public FakeRecord key(String k, Object v) { key.put(k, v); return this; }

    public FakeRecord before(String k, Object v) { before.put(k, v); return this; }

    public FakeRecord after(String k, Object v) { after.put(k, v); return this; }

    public FakeRecord after(Map<String, Object> row) { after.putAll(row); return this; }

    public FakeRecord partition(int p) { this.partition = p; return this; }

    public FakeRecord offset(long o) { this.offset = o; return this; }

    @Override public Op op() { return op; }
    @Override public Map<String, Object> key() { return key; }
    @Override public Map<String, Object> before() { return before; }
    @Override public Map<String, Object> after() { return after; }
    @Override public long tso() { return tso; }
    @Override public int partition() { return partition; }
    @Override public long offset() { return offset; }
}
