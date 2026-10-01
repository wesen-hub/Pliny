package com.dcp.synclink.exec;

import com.dcp.scheduler.exec.PartitionExecutor;
import com.dcp.scheduler.model.ExecutorSnapshot;
import com.dcp.synclink.core.BatchWriteService;
import com.dcp.synclink.core.SyncConsumer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * sync-writer 执行器(接入设计 §四):SyncConsumer 收编,框架每分区一线程。
 *
 * <pre>
 *   消费循环:assign 单分区 → poll → SyncConsumer.handleBatch(解析/路由/投递攒批)
 *     → BatchWriteService.commitAfterOffer(等待写线程追平 → commitSync 提交已写游标)
 *   空闲时 commitIdle 补交(尾批假卡 lag 兜底);崩溃重放由 ODKU 幂等吸收(AGGRESSIVE)。
 *   位点自管:assign 单分区 + group.id(原 consumer_group),commitSync 不依赖组成员身份;
 *   重启从提交位点续读,与旧链路语义一致。
 *
 *   无 Spring 装配: Kafka 连接参数由 SyncWriterFactory 从 ExecutorEnvironment 注入。
 * </pre>
 */
@Slf4j
public class SyncWriterExecutor implements PartitionExecutor {

    private final String kafkaBootstrap;
    private final SyncConsumer syncConsumer;
    private final BatchWriteService batchWriteService;
    private final AtomicLong processed = new AtomicLong();
    private final AtomicLong errors = new AtomicLong();
    private final AtomicLong latency = new AtomicLong();

    private volatile ExecutorContext ctx;
    private volatile Consumer<String, String> kafka;
    private volatile boolean stopped = true;
    private Thread worker;

    public SyncWriterExecutor(String kafkaBootstrap,
                              SyncConsumer syncConsumer,
                              BatchWriteService batchWriteService) {
        this.kafkaBootstrap = kafkaBootstrap;
        this.syncConsumer = syncConsumer;
        this.batchWriteService = batchWriteService;
    }

    @Override
    public synchronized void start(ExecutorContext ctx) {
        this.ctx = ctx;
        stopped = false;
        // 消费组:原逻辑表 consumer_group(bizMeta 透传),位点记账单位不变
        String groupId = (ctx.bizMeta != null && ctx.bizMeta.get("consumerGroup") != null
                && !ctx.bizMeta.get("consumerGroup").isBlank())
                ? ctx.bizMeta.get("consumerGroup") : "dcp-sync-" + ctx.taskId;
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrap);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        kafka = new KafkaConsumer<>(props);
        kafka.assign(List.of(new TopicPartition(ctx.topic, ctx.partId)));
        batchWriteService.ensureQueue(ctx.taskId);
        worker = new Thread(this::run, "sync-writer-" + ctx.taskId + "-" + ctx.partId);
        worker.setDaemon(true);
        worker.start();
        log.info("[sync-writer] 执行器启动 ns={} task={} topic={} part={} group={}",
                ctx.ns, ctx.taskId, ctx.topic, ctx.partId, groupId);
    }

    private void run() {
        while (!stopped) {
            try {
                ConsumerRecords<String, String> records = kafka.poll(Duration.ofMillis(500));
                if (!records.isEmpty()) {
                    long t0 = System.currentTimeMillis();
                    // 解析/路由/投递攒批(队列满时内部背压等待,不阻塞消费者语义)
                    List<ConsumerRecord<String, String>> list = new java.util.ArrayList<>();
                    records.forEach(list::add);
                    syncConsumer.handleBatch(list);
                    Map<TopicPartition, Long> offered = new HashMap<>();
                    for (ConsumerRecord<String, String> r : records) {
                        offered.merge(new TopicPartition(r.topic(), r.partition()), r.offset(), Math::max);
                        processed.incrementAndGet();
                    }
                    latency.addAndGet(System.currentTimeMillis() - t0);
                    // 等写线程追平本次 poll → 提交已写游标(提交值绝不超前写库)
                    batchWriteService.commitAfterOffer(kafka, offered);
                } else {
                    // 空闲补交:尾批追平超时后再无新消息的场景(假卡 lag 兜底)
                    batchWriteService.commitIdle(kafka);
                }
            } catch (WakeupException we) {
                if (!stopped) {
                    log.warn("[sync-writer] 意外 wakeup: {}/{}", ctx.taskId, ctx.partId);
                }
                break;
            } catch (Exception e) {
                errors.incrementAndGet();
                log.error("[sync-writer] 消费循环异常 {}/{}: {}", ctx.taskId, ctx.partId, e.getMessage());
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
        // 收尾:尽力补交剩余游标后关 consumer
        try {
            batchWriteService.commitIdle(kafka);
        } catch (Exception ignore) {
        }
        try {
            kafka.close(Duration.ofSeconds(5));
        } catch (Exception ignore) {
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
        log.info("[sync-writer] 执行器停止 {}/{}", ctx.taskId, ctx.partId);
    }

    @Override
    public ExecutorSnapshot metrics() {
        long written = batchWriteService.writtenOffset(ctx.topic, ctx.partId);
        // committedOffset 上报"已写游标"(框架 lag 派生 = endOffset - 已写,口径准确);
        // 无记录时 0(避免 -1 放大 lag)
        return ExecutorSnapshot.of(ctx.taskId, ctx.partId, processed.get(),
                written < 0 ? 0 : written, errors.get(), latency.get(), stopped);
    }
}
