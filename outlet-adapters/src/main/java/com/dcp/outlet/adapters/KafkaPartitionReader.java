package com.dcp.outlet.adapters;

import com.dcp.outlet.control.PartitionReader;
import com.dcp.outlet.control.TsoSample;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * 真实 Kafka 的轻量读取:索引器采样与起点短扫共用。
 *
 * <p>只解析 {@code commitTs}(见 {@link CanalJson}),不物化业务数据 —— 成本模型的核心。
 */
public class KafkaPartitionReader implements PartitionReader, AutoCloseable {

    private final String bootstrap;
    private final AdminClient admin;
    private KafkaConsumer<byte[], byte[]> consumer;

    public KafkaPartitionReader(String bootstrap) {
        this.bootstrap = bootstrap;
        Properties p = new Properties();
        p.put("bootstrap.servers", bootstrap);
        p.put("request.timeout.ms", "8000");
        p.put("default.api.timeout.ms", "15000");
        this.admin = AdminClient.create(p);
    }

    private synchronized KafkaConsumer<byte[], byte[]> consumer() {
        if (consumer == null) {
            Properties p = new Properties();
            p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
            p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
            p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
            p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
            p.put(ConsumerConfig.GROUP_ID_CONFIG, "dcp-outlet-indexer-" + System.nanoTime());
            p.put(ConsumerConfig.CLIENT_ID_CONFIG, "dcp-outlet-reader");
            consumer = new KafkaConsumer<>(p);
        }
        return consumer;
    }

    @Override
    public long earliestOffset(String topic, int partition) {
        return offsetOf(topic, partition, OffsetSpec.earliest());
    }

    @Override
    public long latestOffset(String topic, int partition) {
        return offsetOf(topic, partition, OffsetSpec.latest());
    }

    private long offsetOf(String topic, int partition, OffsetSpec spec) {
        Map<TopicPartition, OffsetSpec> req = new HashMap<>();
        TopicPartition tp = new TopicPartition(topic, partition);
        req.put(tp, spec);
        ListOffsetsResult r = admin.listOffsets(req);
        try {
            return r.partitionResult(tp).get().offset();
        } catch (Exception e) {
            throw new IllegalStateException("listOffsets failed " + tp, e);
        }
    }

    @Override
    public List<TsoSample> readFrom(String topic, int partition, long fromOffset, int maxRows) {
        TopicPartition tp = new TopicPartition(topic, partition);
        KafkaConsumer<byte[], byte[]> c = consumer();
        c.assign(List.of(tp));
        c.seek(tp, fromOffset);
        long end = c.endOffsets(List.of(tp)).get(tp);
        List<TsoSample> out = new ArrayList<>();
        long deadline = System.currentTimeMillis() + 4000;
        while (out.size() < maxRows && System.currentTimeMillis() < deadline) {
            ConsumerRecords<byte[], byte[]> recs = c.poll(Duration.ofMillis(400));
            for (ConsumerRecord<byte[], byte[]> r : recs) {
                long ts = CanalJson.commitTs(new String(r.value(), StandardCharsets.UTF_8));
                if (ts < 0) continue;                       // 非 canal-json,跳过
                out.add(new TsoSample(r.offset(), ts));
                if (out.size() >= maxRows) break;
            }
            if (out.isEmpty() && c.position(tp) >= end) break;      // 已到末尾且无新消息
            if (!recs.isEmpty() && c.position(tp) >= end) break;
        }
        return out;
    }

    @Override
    public Long offsetForTime(String topic, int partition, long timeMillis) {
        TopicPartition tp = new TopicPartition(topic, partition);
        Map<TopicPartition, Long> req = new HashMap<>();
        req.put(tp, timeMillis);
        Map<TopicPartition, OffsetAndTimestamp> r = consumer().offsetsForTimes(req);
        OffsetAndTimestamp v = r.get(tp);
        return v == null ? null : v.offset();
    }

    @Override
    public void close() {
        if (consumer != null) consumer.close(Duration.ofMillis(500));
        admin.close(Duration.ofMillis(500));
    }
}
