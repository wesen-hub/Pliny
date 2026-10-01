package com.dcp.scheduler.model;

/**
 * Kafka 连接配置(运维后台写 /dcp/{ns}/kafka,可选):框架只读 Kafka——仅 Admin API 查 endOffset 算 lag。
 * 缺失时追平判定回退 rps 归零近似(护栏,不阻塞缩容/注销)。
 */
public class KafkaConfig {

    public String bootstrapServers;   // 如 "127.0.0.1:9092,127.0.0.1:9093"
    public Integer probeSec = 60;     // endOffset 周期实查间隔(设计缺省 60s)
}
