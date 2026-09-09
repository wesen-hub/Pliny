package com.dcp.sync;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Lazy;
import org.springframework.jdbc.core.JdbcTemplate;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.event.EventListener;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.event.ListenerContainerIdleEvent;
import org.springframework.kafka.listener.AbstractMessageListenerContainer;
import org.springframework.kafka.listener.BatchConsumerAwareMessageListener;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 消费端动态 topic 注册(V3 §5: 一组一 topic; 逻辑表提交/保存后即可被监听, 无需改配置重启)。
 *
 * <p>背景: {@code @KafkaListener} 的 topic 列表是启动时静态解析的, 新逻辑表(新 topic)
 * 提交后无法被监听。本组件在:<br>
 * ① 应用启动: 注册 application.yml {@code dcp.sync.topics}(兼容旧演示链路) + 元库 logical_table 全部 topic;<br>
 * ② 逻辑表保存时({@link LogicalTableConfigService#save}): 注册该逻辑表的 topic。<br>
 * 消费逻辑复用 {@link SyncConsumer#handleBatch}(批量解析/路由/投递 + 水位推进),
 * offset 提交由**消费者线程**(本容器监听线程)周期调用
 * {@link BatchWriteService#tryCommit} 提交"已写游标"(commitSync 必须持有组成员身份的 consumer)。
 */
@Slf4j
@Component
public class SyncTopicRegistry {

    private final KafkaProperties kafkaProperties;
    private final JdbcTemplate metaJdbc;
    private final SyncConsumer consumer;
    private final BatchWriteService batchWriteService;
    private final ApplicationEventPublisher eventPublisher;
    private final Map<String, AbstractMessageListenerContainer<String, String>> containers = new ConcurrentHashMap<>();

    @Value("${dcp.sync.topics:}")
    private String configuredTopics;

    public SyncTopicRegistry(KafkaProperties kafkaProperties, JdbcTemplate metaJdbc,
                             @Lazy SyncConsumer consumer,
                             @Lazy BatchWriteService batchWriteService,
                             ApplicationEventPublisher eventPublisher) {
        this.kafkaProperties = kafkaProperties;
        this.metaJdbc = metaJdbc;
        this.consumer = consumer;
        this.batchWriteService = batchWriteService;
        this.eventPublisher = eventPublisher;
    }

    @PostConstruct
    public void init() {
        // ① 兼容旧演示链路配置的静态 topics
        if (configuredTopics != null && !configuredTopics.isBlank()) {
            for (String t : configuredTopics.split(",")) {
                if (!t.isBlank()) {
                    registerTopic(t.trim(), null, null, 2);
                }
            }
        }
        // ② 元库已配置的逻辑表
        try {
            List<Map<String, Object>> tables = metaJdbc.queryForList(
                    "SELECT table_name, topic, consumer_group, partition_count FROM logical_table");
            for (Map<String, Object> row : tables) {
                String table = String.valueOf(row.get("table_name"));
                Object topicObj = row.get("topic");
                if (topicObj == null) {
                    continue;
                }
                String topic = String.valueOf(topicObj);
                Object group = row.get("consumer_group");
                // 启动时同样确保分区数(重启后 topic 分区不足会丢指定分区投递)
                int pc = row.get("partition_count") == null ? 8
                        : ((Number) row.get("partition_count")).intValue();
                ensureTopicPartitions(topic, pc > 0 ? pc : 8);
                registerTopic(topic, table, group == null ? null : String.valueOf(group), 2);
            }
        } catch (Exception e) {
            log.warn("启动扫描逻辑表 topic 失败(元库可能为空): {}", e.getMessage());
        }
    }

    /** 逻辑表配置保存后调用: 注册该逻辑表的消费监听(topic 已注册则跳过) */
    public synchronized void register(String logicalTable, String topic, String consumerGroup, int partitionCount) {
        if (topic == null || topic.isBlank()) {
            return;
        }
        ensureTopicPartitions(topic, partitionCount > 0 ? partitionCount : 8);
        registerTopic(topic, logicalTable, consumerGroup, 2);
    }

    /** 确保 topic 存在且分区数 ≥ partitionCount(自动创建默认 1 分区会导致指定分区投递失败) */
    private void ensureTopicPartitions(String topic, int partitionCount) {
        try (org.apache.kafka.clients.admin.AdminClient admin = org.apache.kafka.clients.admin.AdminClient.create(
                kafkaProperties.buildAdminProperties(null))) {
            var desc = admin.describeTopics(java.util.List.of(topic)).allTopicNames().get(5, java.util.concurrent.TimeUnit.SECONDS);
            var existing = desc.get(topic);
            if (existing == null) {
                try {
                    admin.createTopics(java.util.List.of(new org.apache.kafka.clients.admin.NewTopic(topic, partitionCount, (short) 1)))
                            .all().get(30, java.util.concurrent.TimeUnit.SECONDS);
                    log.info("Kafka topic 已创建: {} partitions={}", topic, partitionCount);
                } catch (Exception e) {
                    // 消费者/生产者 auto-create 可能抢先建出 1 分区 topic → 落入下方统一校验扩分区
                    log.warn("Kafka topic 创建竞态(待校验扩分区): {} - {}", topic, e.getMessage());
                }
            }
            // 统一校验: 无论新建/已存在/竞态 auto-create, 分区不足就扩(循环内防 topic 尚未就绪 NPE,
            // 创建/扩容失败都 sleep 重试, 最后仍不足才抛给外层告警 —— 修复 auto-create 1 分区竞态)
            int lastCur = -1;
            for (int i = 0; i < 8; i++) {
                var tt = admin.describeTopics(java.util.List.of(topic)).allTopicNames()
                        .get(5, java.util.concurrent.TimeUnit.SECONDS).get(topic);
                if (tt == null) {          // topic 尚未被任何角色建出(metadata 未同步) → 补一次创建再等
                    try {
                        admin.createTopics(java.util.List.of(new org.apache.kafka.clients.admin.NewTopic(topic, partitionCount, (short) 1)))
                                .all().get(10, java.util.concurrent.TimeUnit.SECONDS);
                        log.info("Kafka topic 补建: {} partitions={}", topic, partitionCount);
                    } catch (Exception ignore) {
                    }
                    lastCur = 0;
                } else {
                    lastCur = tt.partitions().size();
                }
                if (lastCur >= partitionCount) {
                    break;
                }
                try {
                    admin.createPartitions(java.util.Map.of(topic,
                                    org.apache.kafka.clients.admin.NewPartitions.increaseTo(partitionCount)))
                            .all().get(15, java.util.concurrent.TimeUnit.SECONDS);
                    log.info("Kafka topic 扩分区: {} → {}(当前 {})", topic, partitionCount, lastCur);
                } catch (Exception retryEx) {
                    // auto-create/metadata 未就绪期间扩容失败属正常, 下轮重试
                }
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
            if (lastCur < partitionCount) {
                log.warn("Kafka topic 分区仍不足: {}={}(期望 {})", topic, lastCur, partitionCount);
            }
        } catch (Exception e) {
            log.warn("确保 topic 分区失败(可能已存在, 继续): {} - {}", topic, e.getMessage());
        }
    }

    private void registerTopic(String topic, String logicalTable, String groupId, int concurrency) {
        if (containers.containsKey(topic)) {
            return;
        }
        // 解耦攒批: 启动即为该逻辑表预注册有界队列(消费者投递/写线程轮询共用)
        if (logicalTable != null && !logicalTable.isBlank() && batchWriteService != null) {
            batchWriteService.ensureQueue(logicalTable);
        }
        DefaultKafkaConsumerFactory<String, String> factory =
                new DefaultKafkaConsumerFactory<>(kafkaProperties.buildConsumerProperties(null));
        ContainerProperties props = new ContainerProperties(topic);
        props.setGroupId(groupId != null && !groupId.isBlank()
                ? groupId : "dcp-sync-" + (logicalTable != null ? logicalTable : topic));
        // ackMode MANUAL + auto.commit 关闭: 提交完全由消费者线程驱动(commitAfterOffer + 空闲补交),
        // 不依赖 Spring ack(其提交 poll position 会超前于写库进度)
        props.setAckMode(ContainerProperties.AckMode.MANUAL);
        props.setMessageListener((BatchConsumerAwareMessageListener<String, String>) (data, kafkaConsumer) -> {
            consumer.handleBatch(data);
            // 提交在本消费者线程内执行(同线程 commitSync 合法): 等写线程追平本次 poll 的记录后,
            // 按组节流提交"已写游标"(追平有界等待, 见 BatchWriteService#commitAfterOffer)
            Map<TopicPartition, Long> offered = new HashMap<>();
            for (ConsumerRecord<String, String> r : data) {
                offered.merge(new TopicPartition(r.topic(), r.partition()), r.offset(), Math::max);
            }
            batchWriteService.commitAfterOffer(kafkaConsumer, offered);
        });
        // 空闲事件: poll 无新消息超空闲间隔 → 容器在**消费者线程**上发事件;
        // 用于补交"写线程追平但当时无新消息触发提交"的尾部 offset(见 BatchWriteService#commitIdle)
        props.setIdleEventInterval(1000L);
        ConcurrentMessageListenerContainer<String, String> container =
                new ConcurrentMessageListenerContainer<>(factory, props);
        // 手工 new 的容器默认无事件发布者 → 手动注入, 否则空闲事件不触发
        container.setApplicationEventPublisher(eventPublisher);
        // 每实例每逻辑表 1 个消费者线程(分区预置后水平扩容=加实例, rebalance 自动分摊)
        container.setConcurrency(1);
        container.start();
        containers.put(topic, container);
        log.info("消费监听已注册: topic={} logicalTable={} group={} (解耦攒批; 消费者线程提交已写游标)",
                topic, logicalTable, props.getGroupId());
    }

    /** 空闲事件(消费者线程): 无新消息时周期自检补交 —— 写线程已把"超时未追平"的尾部刷完
     *  (游标已更新), 这里提交它, 不再依赖新消息/重启触发。 */
    @EventListener
    public void onIdle(ListenerContainerIdleEvent event) {
        try {
            if (event.getConsumer() == null) {
                return;
            }
            batchWriteService.commitIdle(event.getConsumer());
        } catch (Exception e) {
            log.warn("空闲补交 offset 异常: {}", e.getMessage());
        }
    }

    @PreDestroy
    public void shutdown() {
        containers.values().forEach(c -> {
            try {
                c.stop();
            } catch (Exception ignore) {
            }
        });
        containers.clear();
    }
}
