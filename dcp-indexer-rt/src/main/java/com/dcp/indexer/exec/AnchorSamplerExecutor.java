package com.dcp.indexer.exec;

import com.dcp.indexer.core.CanalJson;
import com.dcp.indexer.core.AnchorIndexer;
import com.dcp.scheduler.exec.PartitionExecutor;
import com.dcp.scheduler.model.ExecutorSnapshot;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;

/**
 * anchor-sampler 执行器(接入设计 §五):IndexerRunner 收编,框架每分区一线程。
 *
 * <pre>
 *   原模型:subscribe 正则大组(一个 consumer 覆盖全部 topic 分区,常驻线程)。
 *   框架模型:assign 单分区(每分区一执行器),共享 AnchorIndexer;解析/锚点/水位/污染逻辑不变。
 *   位点:处理完一批 commitSync;索引器全程幂等(锚点 upsert/水位 upsert/污染 INSERT IGNORE),
 *   故 handover=AGGRESSIVE(崩溃重放安全)。
 * </pre>
 */
public class AnchorSamplerExecutor implements PartitionExecutor {

    private static final Logger log = LoggerFactory.getLogger("outlet-indexer");

    private final String bootstrap;
    private final AnchorIndexer indexer;
    private final int epoch;
    private final String groupId;
    private final boolean fromBeginning;
    /** 可空:告警出口(生产接 AlertSink;未接线则静默)。 */
    private final BiConsumer<String, String> alert;

    private final AtomicLong processed = new AtomicLong();
    private final AtomicLong poisoned = new AtomicLong();
    private final AtomicLong lastOffset = new AtomicLong(-1);
    private final AtomicLong latency = new AtomicLong();

    private volatile ExecutorContext ctx;
    private volatile KafkaConsumer<byte[], byte[]> kafka;
    private volatile boolean stopped = true;
    private Thread worker;

    public AnchorSamplerExecutor(String bootstrap, AnchorIndexer indexer, int epoch,
                                 String groupId, boolean fromBeginning,
                                 BiConsumer<String, String> alert) {
        this.bootstrap = bootstrap;
        this.indexer = indexer;
        this.epoch = epoch;
        this.groupId = groupId;
        this.fromBeginning = fromBeginning;
        this.alert = alert;
    }

    @Override
    public synchronized void start(ExecutorContext ctx) {
        this.ctx = ctx;
        stopped = false;
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        p.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");  // 显式提交:处理完一批就交(污染也交, 不卡位点)
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, fromBeginning ? "earliest" : "latest");
        p.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1000");
        p.put(ConsumerConfig.CLIENT_ID_CONFIG, "dcp-outlet-indexer");
        kafka = new KafkaConsumer<>(p);
        kafka.assign(List.of(new TopicPartition(ctx.topic, ctx.partId)));
        worker = new Thread(this::run, "anchor-sampler-" + ctx.taskId + "-" + ctx.partId);
        worker.setDaemon(true);
        worker.start();
        log.info("[anchor-sampler] 执行器启动 ns={} task={} topic={} part={} epoch={}",
                ctx.ns, ctx.taskId, ctx.topic, ctx.partId, epoch);
    }

    private void run() {
        while (!stopped) {
            try {
                ConsumerRecords<byte[], byte[]> records = kafka.poll(Duration.ofMillis(500));
                if (!records.isEmpty()) {
                    long t0 = System.currentTimeMillis();
                    handle(records);
                    latency.addAndGet(System.currentTimeMillis() - t0);
                }
            } catch (WakeupException we) {
                break;   // stop() 唤醒:正常退出
            } catch (Exception e) {
                // 索引器不因单次失败退出;失败上报告警(循环继续重试,与 IndexerRunner 常驻语义一致)
                if (alert != null) alert.accept("consume", "索引器消费异常: " + e);
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
        try {
            kafka.close(Duration.ofMillis(500));
        } catch (Exception ignore) {
        }
    }

    /** 与 IndexerRunner.handle 同构:watermark/DML 解析 → 锚点/水位/污染;处理完一批就提交。 */
    private void handle(ConsumerRecords<byte[], byte[]> records) {
        for (ConsumerRecord<byte[], byte[]> r : records) {
            String json = new String(r.value(), StandardCharsets.UTF_8);
            if (CanalJson.isWatermark(json)) {
                long wm = CanalJson.watermarkTs(json);
                if (wm > 0) {
                    indexer.onWatermark(r.topic(), r.partition(), epoch, r.offset(), wm);
                }
            } else if (CanalJson.isDml(json)) {
                long ts = CanalJson.commitTs(json);
                if (ts > 0) {
                    indexer.onEvent(r.topic(), r.partition(), epoch, r.offset(), ts);
                } else {
                    // C1:解析失败不可静默跳过 —— 污染该分区(不再产栅栏, 查询退化 fallback)并告警
                    indexer.onPoison(r.topic(), r.partition());
                    poisoned.incrementAndGet();
                }
            }
            processed.incrementAndGet();
            lastOffset.set(Math.max(lastOffset.get(), r.offset()));
        }
        try {
            kafka.commitSync();             // 处理完一批就提交(含污染/解析失败, 位点不卡死);
        } catch (Exception e) {             // 失败只告警:下轮继续消费并重试提交(重启重放对索引器幂等)
            if (alert != null) alert.accept("commit", "索引器位点提交失败: " + e);
        }
    }

    @Override
    public synchronized void stop() {
        stopped = true;
        if (kafka != null) {
            kafka.wakeup();
        }
        if (worker != null) {
            worker.interrupt();
        }
        log.info("[anchor-sampler] 执行器停止 {}/{}", ctx.taskId, ctx.partId);
    }

    @Override
    public ExecutorSnapshot metrics() {
        // committedOffset 上报最后处理 offset(提交失败重试中可能略超前;索引器幂等, 重放无害)
        return ExecutorSnapshot.of(ctx.taskId, ctx.partId, processed.get(),
                Math.max(lastOffset.get(), 0), poisoned.get(), latency.get(), stopped);
    }
}
