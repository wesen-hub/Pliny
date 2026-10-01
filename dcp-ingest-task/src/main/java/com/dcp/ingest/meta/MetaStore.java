package com.dcp.ingest.meta;

import java.util.List;
import java.util.Map;

/**
 * 元库访问端口(seq 领号 / DLT 落库 / 逻辑表·模型配置读)。
 * 生产实现 {@link JdbcMetaStore}; 单测以 fake 实现注入(LogicHarness 可观察)。
 */
public interface MetaStore extends AutoCloseable {

    /** 逻辑表配置(元库 logical_table 一行, 字段与 LogicalTableConfigService 对齐) */
    final class LtConfig {
        public String tableName;
        public String topic;                 // 信封目标 topic(dcp.{表})
        public int bucketCount = 4096;
        public int partitionCount = 8;
        public List<String> keyColumns;
        /** 桶→分区映射(bucket → partition); null/空 = 顺序映射 bucket % partitionCount */
        public Map<Integer, Integer> bucketMap;

        /** 分区归属: 有桶映射按映射, 否则顺序映射 */
        public int partitionOf(int bucket) {
            if (bucketMap != null && bucketMap.containsKey(bucket)) {
                return bucketMap.get(bucket);
            }
            return Math.floorMod(bucket, Math.max(1, partitionCount));
        }
    }

    /** 加载逻辑表配置(带缓存; 扩容后桶映射 TTL 内自动刷新) */
    LtConfig loadLt(String logicalTable);

    /** 模型字段 {name → type}(验证用; 每次查询, 模型发布即时生效) */
    Map<String, String> loadColumns(String logicalTable);

    /**
     * 领号段 [start, end](闭区间, batch 个号; seq_counter 行锁, 区间互不重叠)。
     *
     * @param seqKey 计数器键(入站 = "kafka|{逻辑表}|{appKey}", 每应用独立可比序列)
     */
    long[] allocateSeq(String seqKey, int batch);

    /** 死信落库(dlt_message; 与 KafkaIngestEngine.dlt 同结构) */
    void dlt(String logicalTable, String sourceTable, String topic, int partition, long offset,
             String businessKey, String payload, String error);

    @Override
    void close();
}
