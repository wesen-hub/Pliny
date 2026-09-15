package com.dcp.outlet.sdk.testkit;

import com.dcp.outlet.sdk.Batch;
import com.dcp.outlet.sdk.ChangeRecord;
import com.dcp.outlet.sdk.Mode;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/** 测试替身:构造一批记录。 */
public final class FakeBatch implements Batch {

    private Mode mode = Mode.INCREMENTAL;
    private String stream = "rt_sync_c";
    private final List<ChangeRecord> records = new ArrayList<>();

    public static FakeBatch of(Mode mode, String stream, ChangeRecord... records) {
        FakeBatch b = new FakeBatch();
        b.mode = mode;
        b.stream = stream;
        b.records.addAll(Arrays.asList(records));
        return b;
    }

    public static FakeBatch incremental(ChangeRecord... records) {
        return of(Mode.INCREMENTAL, "rt_sync_c", records);
    }

    public static FakeBatch snapshot(ChangeRecord... records) {
        return of(Mode.SNAPSHOT, "rt_sync_c", records);
    }

    public FakeBatch stream(String s) { this.stream = s; return this; }

    public FakeBatch add(ChangeRecord... r) { records.addAll(Arrays.asList(r)); return this; }

    @Override public Mode mode() { return mode; }
    @Override public String stream() { return stream; }
    @Override public List<ChangeRecord> records() { return records; }
    @Override public int size() { return records.size(); }
    @Override public boolean isEmpty() { return records.isEmpty(); }
}
