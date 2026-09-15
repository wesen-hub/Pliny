package com.dcp.outlet.adapters;

import com.dcp.outlet.sdk.ChangeRecord;
import com.dcp.outlet.sdk.Op;

import java.util.Map;

/** 批阶段快照行(op 固定 SNAPSHOT;tso = 批边界 T0)。 */
public class SnapshotRecord implements ChangeRecord {

    private final Map<String, Object> row;
    private final long tso;
    private final String stream;

    public SnapshotRecord(Map<String, Object> row, long tso, String stream) {
        this.row = row; this.tso = tso; this.stream = stream;
    }

    @Override public Op op() { return Op.SNAPSHOT; }
    @Override public Map<String, Object> key() { return row; }
    @Override public Map<String, Object> before() { return Map.of(); }
    @Override public Map<String, Object> after() { return row; }
    @Override public long tso() { return tso; }
    @Override public int partition() { return -1; }
    @Override public long offset() { return -1L; }
}
