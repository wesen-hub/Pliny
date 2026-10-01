package com.dcp.scheduler.exec;

import javax.sql.DataSource;
import java.util.Map;

/**
 * 执行器环境契约(微服务架构方案 §8.1):宿主(dcp-worker)实现、业务 jar 使用。
 * 业务 jar 不新建连接池、不硬编码连接地址——一切外部资源经此注入,
 * 保证 dcp-scheduler-api 保持 JDK 零依赖(DataSource 为 javax.sql 标准类型)。
 */
public interface ExecutorEnvironment {

    /** Kafka bootstrap 地址(按 ns 隔离,支持不同集群)。 */
    String kafkaBootstrap(String ns);

    /** 元库数据源(只读/共享连接池,宿主统一管理生命周期)。 */
    DataSource metaDataSource();

    /** 通用环境变量(kv 透传,如对象存储端点、运行环境标识)。 */
    Map<String, String> env();
}
