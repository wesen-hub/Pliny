package com.dcp.outlet.adapters;

import com.dcp.outlet.control.AnchorIndexer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

/**
 * 锚点索引器**常驻组件**:订阅变更流(按 topic 正则,新表自动纳入),
 * 只解析 {@code commitTs / watermark},喂给 {@link AnchorIndexer} 落稀疏锚点与水位。
 *
 * <p>不存事件、不服务数据查询 —— 一次顺序读 + 轻量解析(见详细设计 §7.1)。
 */
public class IndexerRunner implements AutoCloseable {

    /** 运行统计(用于自检与监控上报)。 */
    public static final class Stats {
        public long events;
        public long watermarks;
        public long skipped;
        public long anchors;

        @Override public String toString() {
            return "events=" + events + " watermarks=" + watermarks + " anchors=" + anchors + " skipped=" + skipped;
        }
    }

    private final String bootstrap;
    private final Pattern topicPattern;
    private final AnchorIndexer indexer;
    private final int epoch;
    private final boolean fromBeginning;
    private final Stats stats = new Stats();
    private final AtomicBoolean running = new AtomicBoolean();
    private KafkaConsumer<byte[], byte[]> consumer;

    public IndexerRunner(String bootstrap, String topicPrefix, AnchorIndexer indexer,
                         int epoch, boolean fromBeginning) {
        this.bootstrap = bootstrap;
        this.topicPattern = Pattern.compile(Pattern.quote(topicPrefix) + ".*");
        this.indexer = indexer;
        this.epoch = epoch;
        this.fromBeginning = fromBeginning;
    }

    private KafkaConsumer<byte[], byte[]> consumer() {
        if (consumer == null) {
            Properties p = new Properties();
            p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
            p.put(ConsumerConfig.GROUP_ID_CONFIG, "dcp-outlet-indexer");
            p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
            p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
            p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
            p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, fromBeginning ? "earliest" : "latest");
            p.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1000");
            p.put(ConsumerConfig.CLIENT_ID_CONFIG, "dcp-outlet-indexer");
            consumer = new KafkaConsumer<>(p);
            consumer.subscribe(topicPattern);
        }
        return consumer;
    }

    /** 消费一段时长(自检 / 定时批量),返回统计。 */
    public Stats runOnce(long durationMillis) {
        long deadline = System.currentTimeMillis() + durationMillis;
        KafkaConsumer<byte[], byte[]> c = consumer();
        while (System.currentTimeMillis() < deadline) {
            handle(c.poll(Duration.ofMillis(500)));
        }
        return stats;
    }

    /** 常驻模式:后台线程持续消费,直到 {@link #close()}。 */
    public void startAsync() {
        if (!running.compareAndSet(false, true)) return;
        Thread t = new Thread(() -> {
            KafkaConsumer<byte[], byte[]> c = consumer();
            while (running.get()) {
                try { handle(c.poll(Duration.ofMillis(500))); }
                catch (org.apache.kafka.common.errors.WakeupException e) { break; }
                catch (Exception e) { /* 索引器不因单次失败退出;真实实现应告警 */ }
            }
        }, "outlet-indexer");
        t.setDaemon(true);
        t.start();
    }

    private void handle(ConsumerRecords<byte[], byte[]> records) {
        for (ConsumerRecord<byte[], byte[]> r : records) {
            String json = new String(r.value(), StandardCharsets.UTF_8);
            if (CanalJson.isWatermark(json)) {
                long wm = CanalJson.watermarkTs(json);
                if (wm > 0) {
                    indexer.onWatermark(r.topic(), r.partition(), epoch, r.offset(), wm);
                    stats.watermarks++;
                } else stats.skipped++;
            } else if (CanalJson.isDml(json)) {
                long ts = CanalJson.commitTs(json);
                if (ts > 0) {
                    boolean anchored = indexer.onEvent(r.topic(), r.partition(), epoch, r.offset(), ts);
                    stats.events++;
                    if (anchored) stats.anchors++;
                } else stats.skipped++;
            } else {
                stats.skipped++;              // DDL / 其它:索引器不关心
            }
        }
    }

    public Stats stats() { return stats; }

    @Override
    public void close() {
        running.set(false);
        if (consumer != null) {
            try { consumer.wakeup(); consumer.close(Duration.ofMillis(500)); } catch (Exception ignored) { }
        }
    }
}
