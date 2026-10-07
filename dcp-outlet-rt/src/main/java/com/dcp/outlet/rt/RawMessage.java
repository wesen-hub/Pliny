package com.dcp.outlet.rt;

/** Kafka 原始消息(未解析)。 */
public final class RawMessage {

    private final String topic;
    private final int partition;
    private final long offset;
    private final byte[] payload;

    public RawMessage(int partition, long offset, byte[] payload) {
        this(null, partition, offset, payload);
    }

    public RawMessage(String topic, int partition, long offset, byte[] payload) {
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
        this.payload = payload;
    }

    /** 来源 topic(解码器按 topic 分发格式: dcp.cdc.* 行数据 / dcp.tag.events 打标事件);旧链路缺 topic 为 null。 */
    public String topic() { return topic; }

    public int partition() { return partition; }

    public long offset() { return offset; }

    public byte[] payload() { return payload; }

    @Override public String toString() {
        return "p" + partition + "@" + offset + "(" + (payload == null ? 0 : payload.length) + "B)";
    }
}
