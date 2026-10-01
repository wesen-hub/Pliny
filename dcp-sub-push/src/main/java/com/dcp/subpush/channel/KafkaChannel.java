package com.dcp.subpush.channel;

import com.dcp.subpush.PushParams;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * kafka 推送通道(契约 topic, 缺省 dcp.sub.{subscriber}.{contractId}; 与切流前
 * DataSubService.kafkaSend 同款同步发送, 失败抛 → 平台重试)。
 */
public final class KafkaChannel implements PushChannel {

    private final String topic;
    private final KafkaProducer<String, String> producer;

    public KafkaChannel(PushParams cfg) {
        String t = cfg.topic;
        this.topic = (t == null || t.isBlank()) ? "dcp.sub." + cfg.subscriber + "." + cfg.contractId : t;
        Properties p = new Properties();
        p.put("bootstrap.servers", cfg.kafkaBootstrap == null || cfg.kafkaBootstrap.isBlank()
                ? "kafka:9092" : cfg.kafkaBootstrap);
        p.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        p.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        p.put("acks", "all");
        this.producer = new KafkaProducer<>(p);
    }

    @Override
    public void send(String body) throws Exception {
        producer.send(new ProducerRecord<>(topic, body)).get(5, TimeUnit.SECONDS);
    }

    @Override
    public void close() {
        producer.close();
    }
}
