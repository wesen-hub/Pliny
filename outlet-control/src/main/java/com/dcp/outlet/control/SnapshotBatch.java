package com.dcp.outlet.control;

import com.dcp.outlet.sdk.Batch;
import com.dcp.outlet.sdk.ChangeRecord;
import com.dcp.outlet.sdk.Mode;

import java.util.List;

/** 批阶段的一批记录:mode 固定为 {@link Mode#SNAPSHOT}(与增量共用同一个 {@code process})。 */
public class SnapshotBatch implements Batch {

    private final String stream;
    private final List<ChangeRecord> records;
    private final int sliceIndex;
    private final int sliceTotal;

    public SnapshotBatch(String stream, List<ChangeRecord> records, int sliceIndex, int sliceTotal) {
        this.stream = stream; this.records = records;
        this.sliceIndex = sliceIndex; this.sliceTotal = sliceTotal;
    }

    @Override public Mode mode() { return Mode.SNAPSHOT; }
    @Override public String stream() { return stream; }
    @Override public List<ChangeRecord> records() { return records; }
    @Override public int size() { return records.size(); }
    @Override public boolean isEmpty() { return records.isEmpty(); }

    public int sliceIndex() { return sliceIndex; }
    public int sliceTotal() { return sliceTotal; }
}
