package com.dcp.outlet.rt;

import com.dcp.outlet.sdk.Batch;
import com.dcp.outlet.sdk.ChangeRecord;
import com.dcp.outlet.sdk.Mode;

import java.util.List;

/** SDK {@link Batch} 的运行时实现。 */
public class BatchImpl implements Batch {

    private final Mode mode;
    private final String stream;
    private final List<ChangeRecord> records;

    public BatchImpl(Mode mode, String stream, List<ChangeRecord> records) {
        this.mode = mode;
        this.stream = stream;
        this.records = records;
    }

    @Override public Mode mode() { return mode; }
    @Override public String stream() { return stream; }
    @Override public List<ChangeRecord> records() { return records; }
    @Override public int size() { return records.size(); }
    @Override public boolean isEmpty() { return records.isEmpty(); }
}
