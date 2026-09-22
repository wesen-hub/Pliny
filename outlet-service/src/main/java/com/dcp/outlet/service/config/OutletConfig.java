package com.dcp.outlet.service.config;

import com.dcp.outlet.adapters.*;
import com.dcp.outlet.control.*;
import com.dcp.outlet.sdk.InMemoryLogicRegistry;
import com.dcp.outlet.sdk.LogicLoader;
import com.dcp.outlet.sdk.SdkLogger;
import com.dcp.outlet.worker.ClasspathLogicLoader;
import com.dcp.outlet.worker.CompositeLogicLoader;
import com.dcp.outlet.worker.DeadLetterSink;
import com.dcp.outlet.worker.SpiLogicLoader;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

import javax.sql.DataSource;
import java.nio.file.Path;

/**
 * 装配:元数据库(two datasources 由 Spring 提供)→ 适配层 → 内核。
 *
 * <p>所有内核组件都不依赖 Spring;这里只是把它们接起来。
 */
@Configuration
public class OutletConfig {

    /** 元数据库(v2 元数据表主库)。@Primary: 装配期存在两个 DataSource(元库/TiDB 只读)时默认注入元库。 */
    @Bean
    @Primary
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

    /** 逻辑目录:建任务时拦"逻辑未注册 / 已禁用"。 */
    @Bean
    public LogicCatalog logicCatalog(DataSource ds) { return new JdbcLogicCatalog(ds); }

    /** 逻辑管理服务:上传发布一次动作(七步)+ 列表 / 版本列表 / 启停 / 删除 / 下载。 */
    @Bean
    public com.dcp.outlet.service.logic.LogicService logicService(LogicCatalog logicCatalog, DataSource ds,
                                                                  @Value("${outlet.logic.repo}") String repoRoot,
                                                                  @Value("${outlet.logic.max-jar-bytes:52428800}") long maxJarBytes) {
        return new com.dcp.outlet.service.logic.LogicService(logicCatalog, ds, Path.of(repoRoot), maxJarBytes);
    }

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

    /* ------------------------------------------------ 批阶段(snapshot):T0 三适配器 + 真运行器 */

    /** TiDB 只读入口(查 GC safe point;与元库是两个不同实例)。 */
    @Bean
    public DataSource tidbDataSource(@Value("${outlet.tidb.jdbc-url}") String url,
                                     @Value("${outlet.tidb.username}") String user,
                                     @Value("${outlet.tidb.password}") String password) {
        return new DriverManagerDataSource(url, user, password);
    }

    /** R1:changefeed 起始 TSO(TiCDC OpenAPI v2)。 */
    @Bean
    public T0Selector.ChangefeedStartTs changefeedStartTs(
            @Value("${outlet.ticdc.base-url}") String baseUrl,
            @Value("${outlet.ticdc.changefeed-id}") String changefeedId) {
        return new TiCdcChangefeedStartTs(baseUrl, changefeedId);
    }

    /** R2:TiDB GC safe point。@Qualifier 强制绑定 TiDB 只读源(与元库同为 DataSource,类型注入会被 @Primary 元库抢走)。 */
    @Bean
    public T0Selector.GcSafePoint gcSafePoint(@Qualifier("tidbDataSource") DataSource tidbDs) {
        return new TiDbGcSafePoint(tidbDs);
    }

    /** R3:分区最新 TSO(水位表)。 */
    @Bean
    public T0Selector.LatestTs t0LatestTs(WatermarkStore watermarkStore,
                                          @Value("${outlet.epoch:1}") int epoch) {
        return new WatermarkStoreLatestTs(watermarkStore, epoch);
    }

    /** 批边界 T0 三规则。 */
    @Bean
    public T0Selector t0Selector(T0Selector.ChangefeedStartTs changefeedStartTs,
                                 T0Selector.GcSafePoint gcSafePoint,
                                 T0Selector.LatestTs t0LatestTs) {
        return new T0Selector(changefeedStartTs, gcSafePoint, t0LatestTs);
    }

    /** 快照源:平台既有快照 API(AS OF T0)。 */
    @Bean
    public SnapshotSource snapshotSource(@Value("${outlet.snapshot.base-url}") String baseUrl) {
        // 身份不再从配置来:每次读快照由调用方传任务契约的 consumer_id(批/增量一致性,见 SnapshotSource 接口注释)
        return new HttpSnapshotSource(baseUrl);
    }

    /** 逻辑加载:与计算面同构(jar 优先 + SPI + classpath),批/增量用同一份 logic。 */
    @Bean
    public LogicLoader logicLoader(@Value("${outlet.logic.repo}") String repoRoot) {
        return new CompositeLogicLoader(
                new JarLogicLoader(new FileLogicRepository(Path.of(repoRoot))),
                new SpiLogicLoader(),
                new ClasspathLogicLoader(new InMemoryLogicRegistry()));
    }

    /** SDK 日志门面(批阶段)。 */
    @Bean
    public SdkLogger sdkLogger() { return new Slf4jSdkLogger("outlet-batch"); }

    /**
     * 批阶段:执行体选择(设计 §4.3.1)+ 委派。
     * - local = 进程内(SchedulerBatchRunner,开发期)
     * - container / spark = 尚未接线(占位,等任务模型桥 + 执行器快照源)
     * - 选择由 outlet.batch.executor + 估算行数决定(BatchExecutorPolicy)
     */
    @Bean
    public BatchRunner batchRunner(TaskStore taskStore, StreamRegistry streamRegistry,
                                   SnapshotSource snapshotSource, T0Selector t0Selector,
                                   LogicLoader logicLoader, SdkLogger sdkLogger,
                                   ContractLookup contracts,
                                   @Value("${outlet.batch.executor:local}") String executorMode,
                                   @Value("${outlet.spark.submit-bin:spark-submit}") String sparkSubmitBin,
                                   @Value("${outlet.spark.executor-jar:}") String executorJar,
                                   @Value("${outlet.spark.catalog:demo}") String catalog,
                                   @Value("${outlet.spark.namespace:dcp}") String namespace,
                                   @Value("${outlet.snapshot.base-url:http://127.0.0.1:8081}") String snapshotBaseUrl,
                                   @Value("${outlet.spark.warehouse:}") String warehouse,
                                   @Value("${outlet.spark.catalog-type:}") String catalogType) {
        BatchRunner local = new SchedulerBatchRunner(taskStore, streamRegistry, snapshotSource,
                t0Selector, logicLoader, contracts, sdkLogger);
        BatchRunner spark = new SparkBatchRunner(taskStore, streamRegistry, t0Selector, sdkLogger,
                sparkSubmitBin, executorJar, catalog, namespace, snapshotBaseUrl, contracts,
                warehouse, catalogType);
        // 批容器执行体需拉起独立容器,依赖镜像/编排,先保留占位(置 BATCH_FAILED)
        BatchRunner container = (taskId, callback) -> {
            sdkLogger.error("批容器执行体未实现(需容器镜像 + 编排);任务 {} 置 BATCH_FAILED", taskId);
            callback.done(0L, false);
        };
        return new SelectingBatchRunner(local, container, spark, t0Selector, streamRegistry,
                taskStore, snapshotSource, contracts, executorMode, sdkLogger);
    }

    /** 方案 C:增量段交给 Flink(dcp-tagstream)。 */
    @Bean
    public ExternalIncrementalDispatcher externalIncrementalDispatcher(
            @Value("${outlet.kafka.bootstrap}") String kafkaBootstrap,
            @Value("${outlet.flink.base-url:}") String flinkBaseUrl,
            @Value("${outlet.flink.jar:}") String jarPath,
            @Value("${outlet.flink.kafka-bootstrap:}") String flinkKafkaBootstrap) {
        return new FlinkIncrementalDispatcher(kafkaBootstrap, flinkBaseUrl, jarPath, flinkKafkaBootstrap);
    }

    /** 注意 bean 名不能叫 `taskScheduler`:Spring Boot 的自动配置已占用那个名字(启动才会报冲突)。 */
    @Bean
    public TaskScheduler outletTaskScheduler(TaskStore tasks, UnitStore units, UnitDispatcher dispatcher,
                                            StartOffsetService startOffsets, LeaseManager leases,
                                            LatestTsoProvider latestTso, BatchRunner batchRunner,
                                            ExternalIncrementalDispatcher externalDispatcher) {
        return new TaskScheduler(tasks, units, dispatcher, startOffsets, leases, latestTso,
                batchRunner, externalDispatcher, System::currentTimeMillis, 30_000L);
    }

    @Bean
    public TaskKernelService taskKernelService(StreamRegistry streams, LogicCatalog logicCatalog,
                                               ContractLookup contracts, TaskStore tasks, UnitStore units,
                                               UnitPlanner planner, TaskScheduler outletTaskScheduler) {
        return new TaskKernelService(streams, logicCatalog, contracts, tasks, units, planner, outletTaskScheduler);
    }
}
