package com.dcp.outlet.service.config;

import com.dcp.outlet.adapters.*;
import com.dcp.outlet.control.*;
import com.dcp.outlet.worker.DeadLetterSink;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;

/**
 * 装配:元数据库(two datasources 由 Spring 提供)→ 适配层 → 内核。
 *
 * <p>所有内核组件都不依赖 Spring;这里只是把它们接起来。
 */
@Configuration
public class OutletConfig {

    @Bean
    public DataSource outletDataSource(@Value("${outlet.meta.jdbc-url}") String url,
                                       @Value("${outlet.meta.username}") String user,
                                       @Value("${outlet.meta.password}") String password) {
        return new DriverManagerDataSource(url, user, password);
    }

    @Bean
    public StreamRegistry streamRegistry(DataSource ds) { return new JdbcStreamRegistry(ds); }

    @Bean
    public AnchorStore anchorStore(DataSource ds) { return new JdbcAnchorStore(ds); }

    @Bean
    public WatermarkStore watermarkStore(DataSource ds) { return new JdbcWatermarkStore(ds); }

    @Bean
    public TaskStore taskStore(DataSource ds) { return new JdbcTaskStore(ds); }

    @Bean
    public UnitStore unitStore(DataSource ds) { return new JdbcUnitStore(ds); }

    @Bean
    public AnchorIndexPolicy anchorIndexPolicy() { return AnchorIndexPolicy.defaults(); }

    @Bean
    public MonotonicityGuard monotonicityGuard() { return new MonotonicityGuard(); }

    @Bean
    public PartitionReader partitionReader(@Value("${outlet.kafka.bootstrap}") String bootstrap) {
        return new KafkaPartitionReader(bootstrap);
    }

    @Bean
    public StartOffsetService startOffsetService(AnchorStore anchors, PartitionReader reader,
                                                 AnchorIndexPolicy policy, MonotonicityGuard guard,
                                                 @Value("${outlet.start-offset.max-scan-rows:200000}") int maxScanRows) {
        return new StartOffsetService(anchors, reader, policy, guard, maxScanRows);
    }

    @Bean
    public UnitPlanner unitPlanner() { return new UnitPlanner(); }

    @Bean
    public LeaseManager leaseManager() { return new LeaseManager(); }

    @Bean
    public UnitStatusStore unitStatusStore(DataSource ds) { return new JdbcUnitStatusStore(ds); }

    @Bean
    public HeartbeatStore heartbeatStore(DataSource ds) { return new JdbcHeartbeatStore(ds); }

    @Bean
    public ProgressService progressService(PartitionReader reader, WatermarkStore watermarks) {
        return new ProgressService(reader, watermarks, System::currentTimeMillis);
    }

    @Bean
    public DeadLetterSink deadLetterSink(DataSource ds) { return new JdbcDeadLetterSink(ds); }

    @Bean
    public AlertSink alertSink() {
        // 生产实现:接平台告警通道 / Alertmanager;此处先落日志(平台只告警不补齐,D12)
        return alert -> org.slf4j.LoggerFactory.getLogger("outlet-alert").warn("{}", alert);
    }

    @Bean
    public Supervisor supervisor(TaskStore tasks, UnitStore units, UnitStatusStore statuses,
                                 LeaseManager leases, AlertSink alertSink) {
        return new Supervisor(tasks, units, statuses, leases, alertSink,
                30_000L, 300_000L, 60_000L, System::currentTimeMillis);
    }

    @Bean
    public KafkaAdminStreamScanner streamScanner(@Value("${outlet.kafka.bootstrap}") String bootstrap,
                                                 StreamRegistry registry,
                                                 @Value("${outlet.kafka.topic-prefix:dcp.cdc.}") String prefix) {
        return new KafkaAdminStreamScanner(bootstrap, registry, prefix);
    }
}
