package com.dcp.indexer.core;

/** C1 污染死信记录:解析失败分区的持久化标记(人工介入确认后 DELETE 恢复采样)。 */
public final class Poison {

    private final String topic;
    private final int partition;
    private final int epoch;

    public Poison(String topic, int partition, int epoch) {
        this.topic = topic;
        this.partition = partition;
        this.epoch = epoch;
    }

    public String topic() { return topic; }

    public int partition() { return partition; }

    public int epoch() { return epoch; }

    @Override public String toString() {
        return topic + "#" + partition + "@" + epoch;
    }
}
