package com.dcp.scheduler.lag;

import java.util.Map;

/**
 * Kafka endOffset 数据源(框架对 Kafka 只读):主侧周期实查,用于 lag = endOffset − committedOffset。
 * 实现:生产用 AdminLagSource(bootstrapServers);测试/smoke 可注入 mock;null = 无源(追平判定回退 rps 近似)。
 */
public interface KafkaLagSource {

    /** 查 topic 各分区最新 endOffset;查询失败抛异常(调用方回退 rps 近似)。 */
    Map<Integer, Long> endOffsets(String topic) throws Exception;
}
