package com.dcp.sync;

import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;

/**
 * V3 同步链路配置(P1): @EnableKafka 启用提交面 KafkaTemplate(KafkaIngestEngine 入站 topic 管理)。
 *
 * <p>写路径执行体(SyncConsumer/AtomicUpsertDao/BatchWriteService 系)已迁至 dcp-synclink
 * 业务 jar(P0b), 不再在本进程装配 Bean; 入站消费段(容器/领号/信封)已迁至底座任务
 * dcp-ingest-task(P3), 本进程无 @KafkaListener 消费; 目标库连接由业务 jar 经
 * ExecutorEnvironment 注入。
 */
@Configuration
@EnableKafka
public class SyncConfig {
}
