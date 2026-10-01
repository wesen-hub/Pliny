package com.dcp.worker;

import com.dcp.scheduler.exec.ExecutorEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.Map;

/**
 * worker 执行环境(方案 §8.1 契约的宿主实现):业务 jar 经 Factory.init(env) 拿连接与配置。
 *
 * <pre>
 *   kafkaBootstrap(ns)      : dcp.worker.kafka.{ns}(sync 平台栈 9092 / indexer+outlet 出口栈 19092)
 *   metaDataSource()        : 元库连接(业务 jar 不新建连接池,复用本 DataSource)
 *   env()                   : dcp.worker.env 全透传(部署者按 ns 业务 jar 的键约定配置:
 *                             dcp.sync.* / dcp.indexer.* / dcp.outlet.*)
 * </pre>
 */
@Component
public class WorkerEnvironment implements ExecutorEnvironment {

    private static final Logger log = LoggerFactory.getLogger(WorkerEnvironment.class);

    @Value("${dcp.worker.meta.jdbc-url:}")
    private String metaUrl;
    @Value("${dcp.worker.meta.username:}")
    private String metaUser;
    @Value("${dcp.worker.meta.password:}")
    private String metaPassword;

    @Value("#{${dcp.worker.kafka:{}}}")
    private Map<String, String> kafka;

    @Value("#{${dcp.worker.env:{}}}")
    private Map<String, String> env;

    private volatile DataSource metaDataSource;

    @Override
    public String kafkaBootstrap(String ns) {
        String v = kafka.getOrDefault(ns, kafka.get("default"));
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
