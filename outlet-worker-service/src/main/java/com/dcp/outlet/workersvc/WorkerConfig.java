package com.dcp.outlet.workersvc;

import com.dcp.outlet.adapters.*;
import com.dcp.outlet.sdk.*;
import com.dcp.outlet.worker.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 计算面装配:逻辑加载 → 契约(视图拉取 + 裁剪)→ 单元运行器(真 Kafka 消费源 + 控制面心跳续约)。
 *
 * <p>契约这条链是**安全链路**,装配有三条硬要求:
 * <ol>
 *   <li>裁剪器必须是**真裁剪**:`DefaultContractFilter` + 行条件求值器 + 脱敏,不得用 `permissive()`;</li>
 *   <li>视图来自控制面(`HttpContractProviderFactory`,30s 刷新),单元没带 `contractRef` → **拒绝消费**;</li>
 *   <li>拉不到且无缓存 → **拒绝消费**(fail-closed),绝不退回"全放行"。</li>
 * </ol>
 */
@Configuration
public class WorkerConfig {

    @Bean
    public LogicRegistry logicRegistry() {
        LogicRegistry registry = new InMemoryLogicRegistry();
        // 逻辑注册有三种方式(详细设计 §4):① 写在 worker 内由 @Component 注册;
        //   ② 部署期 SPI;③ 逻辑仓库 jar —— 三条路径在下面的 CompositeLogicLoader 里按序生效
        return registry;
    }

    /** 逻辑来源(按序尝试):③ 逻辑仓库 jar → ② 部署期 SPI → ① 同 classpath。 */
    @Bean
    public LogicLoader logicLoader(LogicRegistry registry,
                                   @Value("${outlet.logic.repo:./logic-repo}") String repoRoot) {
        return new CompositeLogicLoader(
                new JarLogicLoader(new FileLogicRepository(Path.of(repoRoot))),
                new SpiLogicLoader(),
                new ClasspathLogicLoader(registry));
    }

    @Bean
    public Heartbeat heartbeat(@Value("${outlet.control.url}") String controlUrl) {
        return new HttpControlPlaneClient(controlUrl);
    }

    @Bean
    public LeaseGuard leaseGuard(@Value("${outlet.control.url}") String controlUrl) {
        return new HttpControlPlaneClient(controlUrl);
    }

    @Bean
    public WatermarkProvider watermarkProvider() {
        return new WatermarkProvider() {
            private final Map<Integer, Long> wm = new ConcurrentHashMap<>();
            @Override public void update(int partition, long watermarkTs) { wm.put(partition, watermarkTs); }
            @Override public long safeWatermark(int partition) { return wm.getOrDefault(partition, 0L); }
        };
    }

    /** 契约视图来源:按单元 `contractRef` 从控制面拉取(30s 刷新,失败沿用缓存,fail-closed)。 */
    @Bean
    public ContractProviderFactory contractProviderFactory(
            @Value("${outlet.control.url}") String controlUrl,
            @Value("${outlet.contract.refresh-ms:30000}") long refreshMillis,
            @Value("${outlet.contract.timeout-ms:3000}") long timeoutMillis) {
        return new HttpContractProviderFactory(controlUrl, Duration.ofMillis(timeoutMillis), refreshMillis);
    }

    @Bean
    public UnitRunner unitRunner(LogicLoader loader,
                                 @Value("${outlet.kafka.bootstrap}") String bootstrap,
                                 Heartbeat heartbeat, LeaseGuard leaseGuard,
                                 WatermarkProvider watermarks,
                                 ContractProviderFactory contractProviders) {
        return new UnitRunner(loader,
                new SdkEventDecoder(),
                // 契约裁剪:列白名单 → 行条件(用原始行判定)→ 脱敏;三处实现与平台引擎同源同语义
                new DefaultContractFilter(new ExpressionRowConditionEvaluator(), new PlatformMasker(), null),
                new KafkaRecordSource.Factory(bootstrap),
                leaseGuard, heartbeat, watermarks,
                new InMemoryDeadLetterSink(),                        // 进程内死信;重启即失(要落库需引 JdbcDeadLetterSink)
                new SimpleUnitMetrics(),
                id -> new Slf4jSdkLogger(id),
                contractProviders);
    }
}
