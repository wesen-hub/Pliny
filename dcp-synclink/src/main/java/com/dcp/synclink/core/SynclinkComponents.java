package com.dcp.synclink.core;

import com.dcp.scheduler.exec.ExecutorEnvironment;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.Map;

/**
 * 写路径组件组装器(业务 jar 自足, 无 Spring 容器):
 * 由 SyncWriterFactory.init(env) 装配一次, 进程级共享攒批/写库层
 * (与原 Spring 单例语义一致: 每分区执行器线程共用 BatchWriteService 攒批)。
 *
 * <pre>
 *   组装链: ExecutorEnvironment
 *     ├─ kafkaBootstrap(ns=sync)          → SyncWriterExecutor 消费连接
 *     ├─ metaDataSource()                 → JdbcTemplate(元库) → LogicalTableReader / DltService / TargetDbRegistry
 *     └─ env() 通用配置                    → 目标库连接 + 攒批参数 + 默认分表参数
 * </pre>
 */
@Slf4j
public class SynclinkComponents implements AutoCloseable {

    private final ExecutorEnvironment env;
    private final String kafkaBootstrap;
    private final JdbcTemplate metaJdbc;
    private final TargetDbRegistry targetDb;
    private final DltService dltService;
    private final BatchWriteService batchWriteService;
    private final SyncConsumer syncConsumer;

    public SynclinkComponents(ExecutorEnvironment env) {
        this.env = env;
        this.kafkaBootstrap = env.kafkaBootstrap("sync");
        this.metaJdbc = new JdbcTemplate(env.metaDataSource());
        this.targetDb = new TargetDbRegistry(metaJdbc,
                str("dcp.sync.target.url"),
                str("dcp.sync.target.username", "root"),
                str("dcp.sync.target.password", "root123"));
        this.dltService = new DltService(metaJdbc);
        this.batchWriteService = new BatchWriteService(targetDb, dltService,
                num("dcp.sync.buffer.queue-capacity", 2000),
                num("dcp.sync.buffer.batch-size", 500),
                num("dcp.sync.buffer.batch-timeout-ms", 100),
                num("dcp.sync.buffer.write-threads", 4),
                num("dcp.sync.buffer.commit-interval-ms", 5000),
                num("dcp.sync.buffer.flush-wait-ms", 15000),
                num("dcp.sync.buffer.drain-per-queue", 200));
        LogicalTableReader tableReader = new LogicalTableReader(metaJdbc);
        this.syncConsumer = new SyncConsumer(targetDb.daoOf("target_db"), tableReader, dltService,
                batchWriteService,
                num("dcp.sync.demo.table-count", 50),
                str("dcp.sync.demo.table-prefix", "t_order"));
        batchWriteService.init();
        log.info("dcp-synclink 组件装配完成: kafka={} 目标库={}", kafkaBootstrap,
                str("dcp.sync.target.url", ""));
    }

    public String kafkaBootstrap() {
        return kafkaBootstrap;
    }

    public SyncConsumer syncConsumer() {
        return syncConsumer;
    }

    public BatchWriteService batchWriteService() {
        return batchWriteService;
    }

    public ExecutorEnvironment environment() {
        return env;
    }

    @Override
    public void close() {
        batchWriteService.shutdown();
        log.info("dcp-synclink 组件已关闭");
    }

    private String str(String key) {
        Map<String, String> m = env.env();
        return m == null ? null : m.get(key);
    }

    private String str(String key, String def) {
        String v = str(key);
        return v == null || v.isBlank() ? def : v;
    }

    private int num(String key, int def) {
        String v = str(key);
        try {
            return v == null ? def : Integer.parseInt(v.trim());
        } catch (NumberFormatException e) {
            return def;
        }
    }
}
