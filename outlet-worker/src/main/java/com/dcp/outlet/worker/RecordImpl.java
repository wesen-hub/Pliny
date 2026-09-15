package com.dcp.outlet.worker;

import com.dcp.outlet.sdk.ChangeRecord;
import com.dcp.outlet.sdk.Mode;
import com.dcp.outlet.sdk.Op;

import java.util.Map;

/** SDK {@link ChangeRecord} 的运行时实现(平台侧构造)。 */
public class RecordImpl implements ChangeRecord {

    private final Op op;
    private final Map<String, Object> key;
    private final Map<String, Object> before;
    private final Map<String, Object> after;
    private final long tso;
    private final int partition;
    private final long offset;
    private final String stream;
    private final Mode mode;

    public RecordImpl(Op op, Map<String, Object> key, Map<String, Object> before, Map<String, Object> after,
                      long tso, int partition, long offset, String stream, Mode mode) {
        this.op = op; this.key = key; this.before = before; this.after = after;
        this.tso = tso; this.partition = partition; this.offset = offset;
        this.stream = stream; this.mode = mode;
    }

    @Override public Op op() { return op; }
    @Override public Map<String, Object> key() { return key; }
    @Override public Map<String, Object> before() { return before; }
    @Override public Map<String, Object> after() { return after; }
    @Override public long tso() { return tso; }
    @Override public int partition() { return partition; }
    @Override public long offset() { return offset; }

    public String stream() { return stream; }

    public Mode mode() { return mode; }
}
