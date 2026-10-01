package com.dcp.outlet.rt;

/** Kafka 原始消息(未解析)。 */
public final class RawMessage {

    private final int partition;
    private final long offset;
    private final byte[] payload;

    public RawMessage(int partition, long offset, byte[] payload) {
        this.partition = partition;
        this.offset = offset;
        this.payload = payload;
    }

    public int partition() { return partition; }

    public long offset() { return offset; }

    public byte[] payload() { return payload; }

    @Override public String toString() {
        return "p" + partition + "@" + offset + "(" + (payload == null ? 0 : payload.length) + "B)";
    }
}
