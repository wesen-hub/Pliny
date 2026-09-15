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
    public com.dcp.outlet.adapters.JdbcContractViewSource contractViewSource(DataSource ds) {
        // 主体侧收窄端口:生产接入出口权限引擎(DataSecurityService);
        // 骨架实现为"不额外收窄",但**契约白名单仍生效** —— 接入引擎后即为完整的「契约 ∩ 主体」
        return new com.dcp.outlet.adapters.JdbcContractViewSource(
                ds, com.dcp.outlet.adapters.JdbcContractViewSource.SubjectColumnPolicy.permissive());
    }

    @Bean
    public ProgressService progressService(PartitionReader reader, WatermarkStore watermarks) {
        return new ProgressService(reader, watermarks, System::currentTimeMillis);
    }

    /** 死信出口:返回**具体类型**(TasksController 注入的是 JdbcDeadLetterSink,不是端口)。 */
    @Bean
    public JdbcDeadLetterSink deadLetterSink(DataSource ds) { return new JdbcDeadLetterSink(ds); }

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

    /* ------------------------------------------------ 建任务校验 / 交接 所需的端口(生产实现) */

    /** 契约摘要:建任务时拦"契约不存在 / 未生效 / 未覆盖流"。 */
    @Bean
    public ContractLookup contractLookup(DataSource ds) { return new JdbcContractLookup(ds); }

    /** 逻辑目录:建任务时拦"逻辑未注册"。 */
    @Bean
    public LogicCatalog logicCatalog(DataSource ds) { return new JdbcLogicCatalog(ds); }

    /** startPolicy=latest 的"最新 TSO" = 水位表(不猜时点;取不到=0)。 */
    @Bean
    public LatestTsoProvider latestTsoProvider(WatermarkStore watermarks) {
        return new WatermarkStoreLatestTso(watermarks);
    }

    /** 交接:把 CreateUnit 下发给计算面(含 contractRef —— 计算面据此拉契约视图)。 */
    @Bean
    public UnitDispatcher unitDispatcher(@Value("${outlet.worker.url}") String workerUrl) {
        return new HttpUnitDispatcher(workerUrl);
    }

    /**
     * 批阶段:目前是**拒统占位**。
     *
     * <p>要真接线需要三个生产适配器:changefeed start_ts(TiCDC API)、
     * TiDB GC safe point({@code SELECT @@tidb_gc_safe_point})、分区最新 TSO(水位表已具备),
     * 再组装 {@code SchedulerBatchRunner}(快照源走平台快照 API)。接线前 snapshot 任务会被判失败,
     * 而**不会带着错 T0 开跑**。
     */
    @Bean
    public BatchRunner batchRunner() { return new NotWiredBatchRunner(); }

    /** 注意 bean 名不能叫 `taskScheduler`:Spring Boot 的自动配置已占用那个名字(启动才会报冲突)。 */
    @Bean
    public TaskScheduler outletTaskScheduler(TaskStore tasks, UnitStore units, UnitDispatcher dispatcher,
                                            StartOffsetService startOffsets, LeaseManager leases,
                                            LatestTsoProvider latestTso, BatchRunner batchRunner) {
        return new TaskScheduler(tasks, units, dispatcher, startOffsets, leases, latestTso,
                batchRunner, System::currentTimeMillis, 30_000L);
    }

    @Bean
    public TaskKernelService taskKernelService(StreamRegistry streams, LogicCatalog logicCatalog,
                                               ContractLookup contracts, TaskStore tasks, UnitStore units,
                                               UnitPlanner planner, TaskScheduler outletTaskScheduler) {
        return new TaskKernelService(streams, logicCatalog, contracts, tasks, units, planner, outletTaskScheduler);
    }
}
