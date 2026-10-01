package com.dcp.outlet.service.schedulerbridge;

import com.dcp.scheduler.exec.ExecutorEnvironment;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.Map;

/**
 * 锚点采样执行环境的过渡宿主实现(dcp-indexer-rt 业务 jar 的 ExecutorEnvironment)。
 *
 * <p>P0d 后由 dcp-worker 统一提供退役;键名与 jar 内 IndexerComponents 约定一致:
 * dcp.indexer.epoch / dcp.indexer.group-id / dcp.indexer.from-beginning,
 * kafkaBootstrap("indexer") 与 metaDataSource() 直接复用本进程 Spring 装配。
 */
@Component
public class IndexerExecutorEnvironment implements ExecutorEnvironment {

    private static final Logger log = LoggerFactory.getLogger(IndexerExecutorEnvironment.class);

    @Value("${outlet.kafka.bootstrap}")
    private String kafkaBootstrap;

    @Value("${outlet.epoch:1}")
    private int epoch;

    @Value("${outlet.indexer.group-id:dcp-outlet-indexer}")
    private String groupId;

    @Value("${outlet.indexer.from-beginning:false}")
    private boolean fromBeginning;

    private final DataSource metaDataSource;

    public IndexerExecutorEnvironment(@Qualifier("outletDataSource") DataSource metaDataSource) {
        this.metaDataSource = metaDataSource;
    }

    @PostConstruct
    public void init() {
        log.info("[scheduler] indexer 执行环境就绪: kafka={} epoch={} group={}",
                kafkaBootstrap, epoch, groupId);
    }

    public int epoch() { return epoch; }

    public String groupId() { return groupId; }

    public boolean fromBeginning() { return fromBeginning; }

    @Override
    public String kafkaBootstrap(String ns) {
        return kafkaBootstrap;
    }

    @Override
    public DataSource metaDataSource() {
        return metaDataSource;
    }

    @Override
    public Map<String, String> env() {
        Map<String, String> m = new HashMap<>();
        m.put("dcp.indexer.epoch", String.valueOf(epoch));
        m.put("dcp.indexer.group-id", groupId);
        m.put("dcp.indexer.from-beginning", String.valueOf(fromBeginning));
        return m;
    }
}
