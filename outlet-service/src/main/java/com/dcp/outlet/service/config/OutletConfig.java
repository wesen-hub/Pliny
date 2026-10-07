package com.dcp.outlet.service.config;

import com.dcp.outlet.adapters.*;
import com.dcp.outlet.control.*;
import com.dcp.outlet.sdk.InMemoryLogicRegistry;
import com.dcp.outlet.sdk.LogicLoader;
import com.dcp.outlet.sdk.SdkLogger;
import com.dcp.outlet.service.logic.LogicArtifactStore;
import com.dcp.outlet.service.logic.MinioArtifactStore;
import com.dcp.outlet.service.logic.MinioLogicRepository;
import com.dcp.outlet.worker.ClasspathLogicLoader;
import com.dcp.outlet.worker.CompositeLogicLoader;
import com.dcp.outlet.worker.DeadLetterSink;
import com.dcp.outlet.worker.SpiLogicLoader;
import net.javacrumbs.shedlock.core.LockProvider;
import net.javacrumbs.shedlock.provider.jdbctemplate.JdbcTemplateLockProvider;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.JdbcTemplate;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;

import javax.sql.DataSource;
import java.net.URI;
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

    /** ShedLock 域级锁提供者(锁表 shedlock 在元库;DB 时钟判到期,文档 5.8)。 */
    @Bean
    public LockProvider lockProvider(DataSource ds) {
        return new JdbcTemplateLockProvider(JdbcTemplateLockProvider.Configuration.builder()
                .withJdbcTemplate(new JdbcTemplate(ds))
                .usingDbTime()
                .build());
    }

    @Bean
    public StreamRegistry streamRegistry(DataSource ds) { return new JdbcStreamRegistry(ds); }

    /** 批阶段终态物化存储(batch_job 行)——批执行体与 Supervisor 巡检共用同一实例。 */
    @Bean
    public BatchJobStore batchJobStore(DataSource ds) { return new BatchJobStore(ds); }

    @Bean
    public AnchorStore anchorStore(DataSource ds) { return new JdbcAnchorStore(ds); }

    @Bean
    public WatermarkStore watermarkStore(DataSource ds) { return new JdbcWatermarkStore(ds); }

    @Bean
    public TaskStore taskStore(DataSource ds) { return new JdbcTaskStore(ds); }

    /** 索引器分区污染死信(C1):chg_anchor_poison 表, 跨重启成立, 人工 DELETE 恢复采样。
     *  (锚点采样链已迁出 dcp-indexer-rt, AnchorIndexer/PoisonStore/MonotonicityGuard 装配随之迁出) */

    /** 分区读器(栅栏方案):查询分区水位与起点。 */
    @Bean
    public PartitionReader partitionReader(@Value("${outlet.kafka.bootstrap}") String bootstrap) {
        return new KafkaPartitionReader(bootstrap);
    }

    @Bean
    public StartOffsetService startOffsetService(AnchorStore anchors, PartitionReader reader) {
        // 栅栏方案:纯索引查询(lastFenceAtOrBefore), 短扫/时间索引/maxScanRows 已退役
        return new StartOffsetService(anchors, reader);
    }

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
    public Supervisor supervisor(TaskStore tasks, TaskFrameworkProbe framework,
                                 TaskScheduler outletTaskScheduler, HandoverFacts handoverFacts,
                                 AlertSink alertSink) {
        return new Supervisor(tasks, framework, outletTaskScheduler, handoverFacts, alertSink);
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

    /** M7: 逻辑制品仓库生产实现——MinIO 共享桶(PutObject 原子 + 桶版本化)。 */
    @Bean
    public LogicArtifactStore logicArtifactStore(S3Client logicS3Client,
                                                 @Value("${outlet.logic.s3.bucket}") String bucket) {
        return new MinioArtifactStore(logicS3Client, bucket);
    }

    /** M7: 逻辑制品的 S3 客户端(path-style, MinIO 专用; 与 backend 侧 S3 用途隔离)。 */
    @Bean
    public S3Client logicS3Client(@Value("${outlet.logic.s3.endpoint}") String endpoint,
                                  @Value("${outlet.logic.s3.region}") String region,
                                  @Value("${outlet.logic.s3.access-key}") String accessKey,
                                  @Value("${outlet.logic.s3.secret-key}") String secretKey) {
        return S3Client.builder()
                .endpointOverride(URI.create(endpoint))
                .region(Region.of(region))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(accessKey, secretKey)))
                .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build())
                .build();
    }

    /** 逻辑管理服务:上传发布一次动作(七步)+ 列表 / 版本列表 / 启停 / 删除 / 下载。 */
    @Bean
    public com.dcp.outlet.service.logic.LogicService logicService(LogicCatalog logicCatalog, DataSource ds,
                                                                  LogicArtifactStore logicArtifactStore,
                                                                  @Value("${outlet.logic.max-jar-bytes:52428800}") long maxJarBytes) {
        return new com.dcp.outlet.service.logic.LogicService(logicCatalog, ds, logicArtifactStore, maxJarBytes);
    }

    /** startPolicy=latest 的"最新 TSO" = 水位表(不猜时点;取不到=0)。 */
    @Bean
    public LatestTsoProvider latestTsoProvider(WatermarkStore watermarks) {
        return new WatermarkStoreLatestTso(watermarks);
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

    /** 批边界 T0 三规则(R2 带 GC 安全余量, 默认 2 分钟; 见 T0Selector 注释)。 */
    @Bean
    public T0Selector t0Selector(T0Selector.ChangefeedStartTs changefeedStartTs,
                                 T0Selector.GcSafePoint gcSafePoint,
                                 T0Selector.LatestTs t0LatestTs,
                                 @Value("${outlet.t0-gc-margin-ms:120000}") long gcSafeMarginMs) {
        return new T0Selector(changefeedStartTs, gcSafePoint, t0LatestTs, gcSafeMarginMs);
    }

    /** 快照源:平台既有快照 API(AS OF T0)。 */
    @Bean
    public SnapshotSource snapshotSource(@Value("${outlet.snapshot.base-url}") String baseUrl) {
        // 身份不再从配置来:每次读快照由调用方传任务契约的 consumer_id(批/增量一致性,见 SnapshotSource 接口注释)
        return new HttpSnapshotSource(baseUrl);
    }

    /** 逻辑加载:与计算面同构(jar 优先 + SPI + classpath),批/增量用同一份 logic。
     *  M7: jar 优先链改从 MinIO 共享桶拉取(本地 sha256 内容寻址缓存), 多实例同源。 */
    @Bean
    public LogicLoader logicLoader(S3Client logicS3Client,
                                   @Value("${outlet.logic.s3.bucket}") String bucket,
                                   @Value("${outlet.logic.cache-dir}") String cacheDir) {
        return new CompositeLogicLoader(
                new JarLogicLoader(new MinioLogicRepository(logicS3Client, bucket, Path.of(cacheDir))),
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
                                   ContractLookup contracts, BatchJobStore batchJobStore,
                                   @Value("${outlet.batch.executor:local}") String executorMode,
                                   @Value("${outlet.spark.submit-bin:spark-submit}") String sparkSubmitBin,
                                   @Value("${outlet.spark.executor-jar:}") String executorJar,
                                   @Value("${outlet.spark.catalog:demo}") String catalog,
                                   @Value("${outlet.spark.namespace:dcp}") String namespace,
                                   @Value("${outlet.snapshot.base-url:http://127.0.0.1:8081}") String snapshotBaseUrl,
                                   @Value("${outlet.spark.warehouse:}") String warehouse,
                                   @Value("${outlet.spark.catalog-type:}") String catalogType,
                                   @Value("${outlet.epoch:1}") int epoch) {
        BatchRunner local = new SchedulerBatchRunner(taskStore, streamRegistry, snapshotSource,
                t0Selector, logicLoader, contracts, batchJobStore, sdkLogger);
        BatchRunner spark = new SparkBatchRunner(taskStore, streamRegistry, t0Selector, sdkLogger,
                sparkSubmitBin, executorJar, catalog, namespace, snapshotBaseUrl, contracts,
                warehouse, catalogType, epoch);
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
            @Value("${outlet.flink.kafka-bootstrap:}") String flinkKafkaBootstrap,
            @Value("${outlet.epoch:1}") int epoch) {
        return new FlinkIncrementalDispatcher(kafkaBootstrap, flinkBaseUrl, jarPath, flinkKafkaBootstrap, epoch);
    }

    /** 注意 bean 名不能叫 `taskScheduler`:Spring Boot 的自动配置已占用那个名字(启动才会报冲突)。 */
    @Bean
    public TaskScheduler outletTaskScheduler(TaskStore tasks, StreamRegistry streams,
                                            StartOffsetService startOffsets,
                                            LatestTsoProvider latestTso, BatchRunner batchRunner,
                                            ExternalIncrementalDispatcher externalDispatcher,
                                            com.dcp.outlet.service.schedulerbridge.FrameworkTaskSpecSink frameworkSink,
                                            SdkLogger sdkLogger, HandoverFacts handoverFacts,
                                            @Value("${outlet.epoch:1}") int epoch) {
        // 任务级下发的唯一出口 = 框架 TaskSpec(彻底版):归属/判死/迁移/防双消费由框架统一负责
        return new TaskScheduler(tasks, streams, startOffsets, latestTso,
                batchRunner, externalDispatcher, frameworkSink, epoch, sdkLogger, handoverFacts);
    }

    @Bean
    public TaskKernelService taskKernelService(StreamRegistry streams, LogicCatalog logicCatalog,
                                               ContractLookup contracts, TaskStore tasks,
                                               TaskScheduler outletTaskScheduler) {
        return new TaskKernelService(streams, logicCatalog, contracts, tasks, outletTaskScheduler);
    }
}
