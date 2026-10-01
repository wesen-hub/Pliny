package com.dcp.outlet.rt;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** 内存死信(测试 / 单机联调)。 */
public class InMemoryDeadLetterSink implements DeadLetterSink {

    /** 一条死信记录。 */
    public static final class Entry {
        public final int partition;
        public final long offset;
        public final long tso;
        public final String error;

        Entry(int partition, long offset, long tso, String error) {
            this.partition = partition; this.offset = offset; this.tso = tso; this.error = error;
        }

        @Override public String toString() { return "p" + partition + "@" + offset + ": " + error; }
    }

    private final List<Entry> entries = Collections.synchronizedList(new ArrayList<>());

    @Override
    public void record(String taskId, String unitId, int partition, long offset, long tso,
                       DecodedMessage message, Throwable error) {
        entries.add(new Entry(partition, offset, tso, error == null ? "unknown" : error.getMessage()));
    }

    public List<Entry> entries() { return List.copyOf(entries); }

    public int size() { return entries.size(); }
}
