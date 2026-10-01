package com.dcp.ingest.send;

/**
 * 信封发送端口(→ dcp.{逻辑表} 主 topic)。
 * 生产实现 {@link KafkaEnvelopeSender}; 单测以 fake 实现注入(断言信封内容)。
 */
public interface EnvelopeSender extends AutoCloseable {

    /**
     * 发送一条 CdcRecord 信封 JSON(指定分区, 业务键作 Kafka key)。
     * 失败必须抛(调用方=底座任务主链路: 抛 → 不提交 offset → 整批重投, at-least-once 目标幂等)。
     */
    void send(String topic, int partition, String businessKey, String envelopeJson);

    @Override
    void close();
}
