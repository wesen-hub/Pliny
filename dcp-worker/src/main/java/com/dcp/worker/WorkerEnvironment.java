package com.dcp.worker;

import com.dcp.scheduler.exec.ExecutorEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.Map;

/**
 * worker 执行环境(方案 §8.1 契约的宿主实现):业务 jar 经 Factory.init(env) 拿连 接与配置。
 *
 * <pre>
 *   kafkaBootstrap(ns)      : dcp.worker.kafka.{ns}(sync 平台栈 9092 / indexer+outlet 出口栈 19092)
 *   metaDataSource()        : 元库连接(业务 jar 不新建连接池,复用本 DataSource)
 *   env()                   : dcp.worker.env 全透传(部署者按 ns 业务 jar 的键约定配置:
 *                             dcp.sync.* / dcp.indexer.* / dcp.outlet.*)
 * </pre>
 *
 * <p>kafka/env 用 {@link ConfigurationProperties} 绑定(dcp.worker.kafka.* / dcp.worker.env.* 是 yml 嵌套 map,
 * 无法用 {@code @Value("#{${dcp.worker.kafka:{}}}")} SpEL 占位符解析 —— flatten 后的 Environment 中不存在
 * dcp.worker.kafka 这个复合键, SpEL 注入恒为 null)。
 */
@Component
@ConfigurationProperties(prefix = "dcp.worker")
public class WorkerEnvironment implements ExecutorEnvironment {

    private static final Logger log = LoggerFactory.getLogger(WorkerEnvironment.class);

    @Value("${dcp.worker.meta.jdbc-url:}")
    private String metaUrl;
    @Value("${dcp.worker.meta.username:}")
    private String metaUser;
    @Value("${dcp.worker.meta.password:}")
    private String metaPassword;

    /** dcp.worker.kafka.{ns} -> bootstrap(绑定自 yml 嵌套 map 或环境变量 DCP_WORKER_KAFKA_*) */
    private Map<String, String> kafka = new HashMap<>();

    /** dcp.worker.env.* 全透传业务 jar(绑定自 yml 嵌套 map) */
    private Map<String, String> env = new HashMap<>();

    private volatile DataSource metaDataSource;

    public Map<String, String> getKafka() {
        return kafka;
    }

    public void setKafka(Map<String, String> kafka) {
        this.kafka = kafka;
    }

    public Map<String, String> getEnv() {
        return env;
    }

    public void setEnv(Map<String, String> env) {
        this.env = env;
    }

    @Override
    public String kafkaBootstrap(String ns) {
        Map<String, String> k = kafka == null ? Map.of() : kafka;
        String v = k.getOrDefault(ns, k.get("default"));
        if (v == null || v.isBlank()) {
            log.warn("[worker] 未配置 ns={} 的 kafka bootstrap(dcp.worker.kafka.{})", ns, ns);
        }
        return v;
    }

    @Override
    public DataSource metaDataSource() {
        if (metaDataSource == null) {
            synchronized (this) {
                if (metaDataSource == null) {
                    if (metaUrl == null || metaUrl.isBlank()) {
                        log.warn("[worker] 未配置 dcp.worker.meta.jdbc-url, metaDataSource 为 null(无元库需求的 ns 忽略)");
                        return null;
                    }
                    metaDataSource = new DriverManagerDataSource(metaUrl, metaUser, metaPassword);
                }
            }
        }
        return metaDataSource;
    }

    @Override
    public Map<String, String> env() {
        return new HashMap<>(env == null ? Map.of() : env);
    }
}
