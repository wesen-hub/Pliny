package com.dcp.laketask.meta;

import java.util.List;

/**
 * 元库访问端口(逻辑表业务键读 / DLT 落库)。
 * 生产实现 {@link JdbcMetaStore}; 单测以 fake 实现注入。
 */
public interface MetaStore extends AutoCloseable {

    /** 逻辑表配置(元库 logical_table 一行, 仅湖增量所需字段) */
    final class LtConfig {
        public String tableName;
        public List<String> keyColumns;
    }

    /** 加载逻辑表业务键(带缓存; 湖增量写湖用, 键列变更频率极低) */
    LtConfig loadLt(String logicalTable);

    /** 死信落库(dlt_message; 与 KafkaIngestEngine.dlt 同结构; source_table=topic) */
    void dlt(String logicalTable, String sourceTable, String topic, int partition, long offset,
             String businessKey, String payload, String error);

    @Override
    void close();
}
