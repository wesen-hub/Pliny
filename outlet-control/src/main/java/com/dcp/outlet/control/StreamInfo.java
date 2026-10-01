package com.dcp.outlet.control;

/** 已登记的流:逻辑表名 → 物理 topic + 分区数 + 所在 Kafka(随任务指令透传, ns 与 Kafka 解耦)。 */
public final class StreamInfo {

    private final String stream;       // 逻辑表名(rt_sync_c)
    private final String topic;        // dcp.cdc.target_db.rt_sync_c
    private final String kafkaBootstrap;  // 该流所在 Kafka 地址(缺省 null = 由计算面 ns 环境决定, 兼容旧通道)
    private final int partitions;

    public StreamInfo(String stream, String topic, int partitions) {
        this(stream, topic, null, partitions);
    }

    public StreamInfo(String stream, String topic, String kafkaBootstrap, int partitions) {
        this.stream = stream; this.topic = topic; this.kafkaBootstrap = kafkaBootstrap; this.partitions = partitions;
    }

    public String stream() { return stream; }
    public String topic() { return topic; }
    public String kafkaBootstrap() { return kafkaBootstrap; }
    public int partitions() { return partitions; }
}
