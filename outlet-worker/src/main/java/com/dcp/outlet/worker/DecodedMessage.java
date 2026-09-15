package com.dcp.outlet.worker;

import com.dcp.outlet.sdk.Op;

import java.util.Map;

/**
 * 解析后的消息。
 *
 * <p>{@code WATERMARK} 不进入业务逻辑,只用于推进 {@code safeWatermark} —— 这是"水位"与"事件"的分界。
 */
public final class DecodedMessage {

    public enum Kind { EVENT, WATERMARK, DDL, OTHER }

    private final Kind kind;
    private final int partition;
    private final long offset;
    private final long tso;                 // 事件 commitTs / 水位 watermarkTs
    private final String stream;            // 逻辑表名
    private final Op op;
    private final Map<String, Object> key;
    private final Map<String, Object> before;
    private final Map<String, Object> after;

    private DecodedMessage(Kind kind, int partition, long offset, long tso, String stream,
                           Op op, Map<String, Object> key, Map<String, Object> before, Map<String, Object> after) {
        this.kind = kind; this.partition = partition; this.offset = offset; this.tso = tso;
        this.stream = stream; this.op = op; this.key = key; this.before = before; this.after = after;
    }

    public static DecodedMessage watermark(int partition, long offset, long watermarkTs) {
        return new DecodedMessage(Kind.WATERMARK, partition, offset, watermarkTs, null, null, null, null, null);
    }

    public static DecodedMessage event(int partition, long offset, long commitTs, String stream, Op op,
                                       Map<String, Object> key, Map<String, Object> before, Map<String, Object> after) {
        return new DecodedMessage(Kind.EVENT, partition, offset, commitTs, stream, op, key, before, after);
    }

    public static DecodedMessage other(Kind kind, int partition, long offset, long tso) {
        return new DecodedMessage(kind, partition, offset, tso, null, null, null, null, null);
    }

    public Kind kind() { return kind; }
    public int partition() { return partition; }
    public long offset() { return offset; }
    public long tso() { return tso; }
    public String stream() { return stream; }
    public Op op() { return op; }
    public Map<String, Object> key() { return key; }
    public Map<String, Object> before() { return before; }
    public Map<String, Object> after() { return after; }
}
