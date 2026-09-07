package com.dcp.sync;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

/**
 * 采集端投递器(V3 §4.4 上游): 领 seq → 逐条打标 → 桶路由 → 写 Kafka。
 *
 * <pre>
 *   seq:    SeqAllocator 区间预分配(批量领号, 逐条打标)
 *   路由:   桶号 = hash(业务主键) % bucketCount
 *          分区 = bucketMap[桶号](P0 用顺序映射 bucket % partitionCount, 正式版从元库读映射)
 *   发送:   指定分区投递, key = 业务主键
 * </pre>
 *
 * TODO(P2+): 桶→分区映射从 logical_table.bucket_map 加载并热更新; 幂等生产者已开启(enable-idempotence)。
 */
@Slf4j
@Component
public class SyncProducer {

    private final KafkaTemplate<String, String> kafka;
    private final SeqAllocator seqAllocator;
    private final BucketMapService bucketMapService;

    public SyncProducer(KafkaTemplate<String, String> kafka, SeqAllocator seqAllocator,
                        BucketMapService bucketMapService) {
        this.kafka = kafka;
        this.seqAllocator = seqAllocator;
        this.bucketMapService = bucketMapService;
    }

    /**
     * 批量投递一批 CDC 行。
     *
     * @param logicalTable     逻辑表名(seq 计数器 + 路由参数来源)
     * @param topic            Kafka topic
     * @param bucketCount      固定桶数(如 4096)
     * @param partitionCount   当前分区数(顺序映射: partition = bucket % partitionCount)
     * @param businessKeyField 记录中业务主键字段名(如 order_id; 多字段组合由上游先归一)
     * @param keyColumns       目标唯一键列
     * @param rowKind          INSERT | UPDATE | DELETE
     * @param rows             业务行
     * @return 本批分配到的 seq 区间 [start, end]
     */
    public long[] sendBatch(String logicalTable, String topic, int bucketCount, int partitionCount,
                            String businessKeyField, List<String> keyColumns, String rowKind,
                            List<Map<String, Object>> rows) {
        long[] range = seqAllocator.allocate(logicalTable, Math.max(1, rows.size()));
        long seq = range[0];
        for (Map<String, Object> row : rows) {
            Object bkRaw = row.get(businessKeyField);
            String businessKey = bkRaw == null ? "__NULL_KEY__" : String.valueOf(bkRaw);
            int bucket = RouteUtil.hash(businessKey) % bucketCount;
            int partition = bucketMapService.partitionOf(logicalTable, bucket);   // 桶→分区映射(扩容热更新)
            CdcRecord rec = CdcRecord.of(logicalTable, businessKey, keyColumns, rowKind, seq, row);
            try {
                kafka.send(topic, partition, businessKey, Json.toJson(rec)).get();
            } catch (Exception e) {
                throw new IllegalStateException("Kafka 投递失败 topic=" + topic + " seq=" + seq
                        + " cause=" + (e.getCause() != null ? e.getCause().getMessage() : e.getMessage()), e);
            }
            seq++;
        }
        log.info("已投递 {} 条 → {} (seq {}-{}, 桶路由 bucket % {} → partition)", rows.size(), topic, range[0], seq - 1, bucketCount);
        return range;
    }

    /** 单条投递(演示用): 领一个号, 直接投递预构造记录(如主键修改事件, 带 beforeKey) */
    public long[] sendRaw(String logicalTable, String topic, int bucketCount, int partitionCount,
                          CdcRecord rec) {
        long[] range = seqAllocator.allocate(logicalTable, 1);
        rec.setSeq(range[0]);
        int bucket = RouteUtil.hash(rec.getBusinessKey()) % bucketCount;
        int partition = bucketMapService.partitionOf(logicalTable, bucket);   // 桶→分区映射(扩容热更新)
        try {
            kafka.send(topic, partition, rec.getBusinessKey(), Json.toJson(rec)).get();
        } catch (Exception e) {
            throw new IllegalStateException("Kafka 投递失败 topic=" + topic + " seq=" + rec.getSeq()
                    + " cause=" + (e.getCause() != null ? e.getCause().getMessage() : e.getMessage()), e);
        }
        return range;
    }

    /** 轻量 JSON(避免引入额外依赖; 生产可用 ObjectMapper 替换) */
    public static final class Json {
        private static final com.fasterxml.jackson.databind.ObjectMapper OM;

        static {
            OM = new com.fasterxml.jackson.databind.ObjectMapper();
            // JDBC 时间类型(LocalDateTime 等)需要 jsr310 模块(spring-boot-starter-json 自带)
            OM.registerModule(new com.fasterxml.jackson.datatype.jsr310.JavaTimeModule());
            OM.disable(com.fasterxml.jackson.databind.SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        }

        public static String toJson(Object o) {
            try {
                return OM.writeValueAsString(o);
            } catch (Exception e) {
                throw new IllegalStateException("JSON 序列化失败", e);
            }
        }

        public static com.fasterxml.jackson.databind.JsonNode readTree(String s) {
            try {
                return OM.readTree(s);
            } catch (Exception e) {
                throw new IllegalStateException("JSON 解析失败", e);
            }
        }

        public static <T> T fromJson(String s, Class<T> type) {
            try {
                return OM.readValue(s, type);
            } catch (Exception e) {
                throw new IllegalStateException("JSON 反序列化失败", e);
            }
        }
    }
}
