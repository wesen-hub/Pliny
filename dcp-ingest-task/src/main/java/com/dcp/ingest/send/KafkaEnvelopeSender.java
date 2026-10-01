package com.dcp.ingest.send;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Kafka 信封生产者(与 KafkaIngestEngine 发送语义一致: 指定分区 + 业务键 + 同步确认)。
 * 同步 send().get() —— 失败立即抛, 底座据此不提交 offset 整批重投(目标侧 seq 幂等)。
 */
public class KafkaEnvelopeSender implements EnvelopeSender {

    private final KafkaProducer<String, String> producer;

    public KafkaEnvelopeSender(String bootstrap) {
        Properties p = new Properties();
        p.put("bootstrap.servers", bootstrap);
        p.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        p.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        this.producer = new KafkaProducer<>(p);
    }

    @Override
    public void send(String topic, int partition, String businessKey, String envelopeJson) {
        try {
            producer.send(new ProducerRecord<>(topic, partition, businessKey, envelopeJson))
                    .get(5, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new IllegalStateException("[ingest-task] 写 dcp 失败 topic=" + topic
                    + " partition=" + partition + " cause=" + e.getMessage(), e);
        }
    }

    @Override
    public void close() {
        try {
            producer.close(java.time.Duration.ofSeconds(5));
        } catch (Exception ignore) {
        }
    }
}
