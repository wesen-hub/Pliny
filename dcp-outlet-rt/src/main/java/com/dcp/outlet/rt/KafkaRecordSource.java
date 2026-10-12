package com.dcp.outlet.rt;

import com.dcp.outlet.rt.CreateUnitCmd;
import com.dcp.outlet.rt.RawMessage;
import com.dcp.outlet.rt.RecordSource;
import com.dcp.outlet.rt.RecordSourceFactory;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * 计算面的真实消费源:**单分区组 assign + seek + commitSync**。
 *
 * <p>刻意**不用 subscribe**(避免组内 rebalance 覆盖平台下发的起点,见 ADR-1);
 * 分区归属由控制面决定,这里只忠实执行。
 *
 * <p><b>重启续传</b>(消费者重启位点续传设计, 2026-10-13):
 * 启动时由上层查询 {@link #committed(int)} 与任务单起点取 max 后 seek;
 * auto.offset.reset=earliest 是第三级兜底 —— committed 与任务单起点都被 retention
 * 清理时从分区最早位置重扫(出口幂等吸收), 绝不静默跳最新丢数据。
 */
public class KafkaRecordSource implements RecordSource {

    private final KafkaConsumer<byte[], byte[]> consumer;
    private final String topic;
    private final List<Integer> partitions = new ArrayList<>();
    private final Map<Integer, Long> positions = new HashMap<>();
    private volatile boolean closed;

    public KafkaRecordSource(String bootstrap, String groupId, String topic, List<Integer> partitions) {
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        p.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        p.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "500");
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");  // 第三级兜底:宁重扫不跳最新
        p.put(ConsumerConfig.CLIENT_ID_CONFIG, "dcp-outlet-unit-" + groupId);
        this.consumer = new KafkaConsumer<>(p);
        this.topic = topic;
        this.partitions.addAll(partitions);
        List<TopicPartition> tps = new ArrayList<>();
        for (int part : partitions) tps.add(new TopicPartition(topic, part));
        consumer.assign(tps);                                  // ★ 不用 subscribe
    }

    @Override
    public List<RawMessage> poll(long timeoutMillis) {
        if (closed) return List.of();
        ConsumerRecords<byte[], byte[]> recs = consumer.poll(Duration.ofMillis(Math.max(1, timeoutMillis)));
        List<RawMessage> out = new ArrayList<>();
        for (ConsumerRecord<byte[], byte[]> r : recs) {
            out.add(new RawMessage(r.topic(), r.partition(), r.offset(), r.value()));
            positions.put(r.partition(), r.offset() + 1);
        }
        return out;
    }

    @Override
    public void seek(int partition, long offset) {
        consumer.seek(new TopicPartition(topic, partition), offset);
        positions.put(partition, offset);
    }

    @Override
    public void commitSync(int partition, long offset) {
        Map<TopicPartition, org.apache.kafka.clients.consumer.OffsetAndMetadata> off = new HashMap<>();
        off.put(new TopicPartition(topic, partition),
                new org.apache.kafka.clients.consumer.OffsetAndMetadata(offset));
        consumer.commitSync(off);
    }

    @Override
    public Long committed(int partition) {
        if (closed) return null;
        try {
            org.apache.kafka.clients.consumer.OffsetAndMetadata off =
                    consumer.committed(new TopicPartition(topic, partition));
            return off == null ? null : off.offset();
        } catch (Exception e) {
            // 查询失败视为无记录,由上层回退任务单起点(at-least-once, 重复无害),不阻塞启动
            return null;
        }
    }

    @Override
    public void wakeup() { consumer.wakeup(); }

    @Override
    public void close() {
        closed = true;
        try { consumer.close(Duration.ofMillis(500)); } catch (Exception ignored) { }
    }

    /** 工厂:按 CreateUnitCmd 建源(生产实现)。 */
    public static class Factory implements RecordSourceFactory {
        private final String bootstrap;
        private final String groupSuffix;

        public Factory(String bootstrap) { this(bootstrap, ""); }

        public Factory(String bootstrap, String groupSuffix) {
            this.bootstrap = bootstrap; this.groupSuffix = groupSuffix;
        }

        @Override
        public RecordSource create(CreateUnitCmd cmd) {
            // Kafka 地址随指令透传优先(ns 与 Kafka 解耦);缺 = 回退计算面 ns 环境(兼容旧通道)
            String bs = cmd.kafkaBootstrap() == null || cmd.kafkaBootstrap().isBlank()
                    ? bootstrap : cmd.kafkaBootstrap();
            return new KafkaRecordSource(bs, cmd.consumerGroup() + groupSuffix, cmd.topic(), cmd.partitions());
        }
    }
}
