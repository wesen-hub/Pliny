package com.dcp.sync;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * 扩容服务(V3 §6): lag 查询 + 加分区(创建分区 → 重算桶映射 → 元库热更新)。
 *
 * <pre>
 *   触发: lag 持续增长(监控见 LagMonitor)
 *   第一步: 加消费实例(利用闲置并行度, 无副作用)
 *   第二步: 加分区: AdminClient.createPartitions → BucketMapService.rebuild → 采集端热更新
 *   第三步: 加 broker(低频人工, 不在此实现)
 * </pre>
 */
@Slf4j
@Service
public class ScaleService {

    private final BucketMapService bucketMapService;
    private final LogicalTableConfigService tableConfigService;
    private final String bootstrap;
    private final String defaultGroup;

    public ScaleService(BucketMapService bucketMapService,
                        LogicalTableConfigService tableConfigService,
                        @Value("${spring.kafka.bootstrap-servers:localhost:9092}") String bootstrap,
                        @Value("${spring.kafka.consumer.group-id:dcp-sync-demo}") String defaultGroup) {
        this.bucketMapService = bucketMapService;
        this.tableConfigService = tableConfigService;
        this.bootstrap = bootstrap;
        this.defaultGroup = defaultGroup;
    }

    private AdminClient admin() {
        return AdminClient.create(Map.of(
                org.apache.kafka.clients.admin.AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap));
    }

    /** 逻辑表当前各分区 lag(消费组位点 vs 分区末端) */
    public Map<String, Object> lagOf(String logicalTable) {
        LogicalTableConfigService.LogicalTableConfig cfg = tableConfigService.load(logicalTable);
        // 消费组: 逻辑表配置优先; 缺省用 dcp-sync-{逻辑表}(与 SyncTopicRegistry 注册规则一致,
        // 避免误查 spring.kafka.consumer.group-id 默认组导致 lag 虚高)
        String group = cfg.getConsumerGroup();
        if (group == null || group.isBlank()) {
            group = "dcp-sync-" + logicalTable;
        }
        String topic = cfg.getTopic();
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("logicalTable", logicalTable);
        out.put("topic", topic);
        out.put("consumerGroup", group);
        if (topic == null || topic.isBlank()) {
            // 逻辑表未配置/已被清理(旧页面或监控仍在查): 返回空 lag 而非 NPE
            out.put("partitionCount", 0);
            out.put("totalLag", 0);
            out.put("partitions", java.util.List.of());
            return out;
        }
        try (AdminClient admin = admin()) {
            var topicDesc = admin.describeTopics(List.of(topic)).all().get(10, TimeUnit.SECONDS).get(topic);
            List<TopicPartition> tps = new ArrayList<>();
            topicDesc.partitions().forEach(p -> tps.add(new TopicPartition(topic, p.partition())));
            Map<TopicPartition, OffsetSpec> specs = new HashMap<>();
            tps.forEach(tp -> specs.put(tp, OffsetSpec.latest()));
            Map<TopicPartition, Long> endOffsets = new HashMap<>();
            admin.listOffsets(specs).all().get(10, TimeUnit.SECONDS)
                    .forEach((tp, info) -> endOffsets.put(tp, info.offset()));
            // 消费组已提交位点
            Map<TopicPartition, OffsetAndMetadata> committed = admin
                    .listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get(10, TimeUnit.SECONDS);
            long totalLag = 0;
            List<Map<String, Object>> partitions = new ArrayList<>();
            for (TopicPartition tp : tps) {
                long end = endOffsets.getOrDefault(tp, 0L);
                long pos = committed.get(tp) == null ? 0L : committed.get(tp).offset();
                long lag = Math.max(0, end - pos);
                totalLag += lag;
                Map<String, Object> p = new LinkedHashMap<>();
                p.put("partition", tp.partition());
                p.put("endOffset", end);
                p.put("committedOffset", pos);
                p.put("lag", lag);
                partitions.add(p);
            }
            out.put("partitionCount", tps.size());
            out.put("totalLag", totalLag);
            out.put("partitions", partitions);
            return out;
        } catch (Exception e) {
            throw new IllegalStateException("查询 lag 失败: " + e.getMessage(), e);
        }
    }

    /**
     * 加分区扩容: createPartitions → 重算桶映射 → 元库热更新。
     * 旧分区存量数据不搬(留在原分区消费完), 新数据按新映射投递, 新旧分居乱序由 seq 兜底。
     */
    public Map<String, Object> expandPartitions(String logicalTable, int targetPartitions) {
        LogicalTableConfigService.LogicalTableConfig cfg = tableConfigService.load(logicalTable);
        if (targetPartitions <= cfg.getPartitionCount()) {
            throw new IllegalArgumentException("目标分区数必须大于当前 " + cfg.getPartitionCount());
        }
        try (AdminClient admin = admin()) {
            admin.createPartitions(Map.of(cfg.getTopic(), NewPartitions.increaseTo(targetPartitions)))
                    .all().get(30, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new IllegalStateException("Kafka 加分区失败: " + e.getMessage(), e);
        }
        bucketMapService.rebuild(logicalTable, targetPartitions);
        log.warn("扩容完成: {} 分区 {} → {}, 桶映射已热更新", logicalTable, cfg.getPartitionCount(), targetPartitions);
        return Map.of("logicalTable", logicalTable, "oldPartitions", cfg.getPartitionCount(),
                "newPartitions", targetPartitions, "bucketMapRebuilt", true);
    }
}
