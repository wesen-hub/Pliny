package com.dcp.outlet.rt;

import com.dcp.outlet.sdk.InMemoryLogicRegistry;
import com.dcp.outlet.sdk.LogicLoader;
import com.dcp.scheduler.exec.ExecutorEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 出口计算面组件组装器:业务 jar 无 Spring 容器,这里把 UnitRunner 链手工接起来。
 *
 * <pre>
 *   组装链(与迁出前 WorkerSchedulerRuntime 构造同构):
 *   LogicLoader = JarLogicLoader(逻辑仓库) → SpiLogicLoader(部署期 SPI) → ClasspathLogicLoader
 *   → SdkEventDecoder + DefaultContractFilter(真裁剪:表达式求值 + 脱敏)
 *   → KafkaRecordSource.Factory(kafkaBootstrap) + 内存水位 + 内存死信 + 指标
 *   → UnitRunner(框架版, 租约/心跳已由统一调度框架收编)。
 *
 *   配置键(宿主 ExecutorEnvironment 提供):
 *   dcp.outlet.logic-repo           逻辑仓库目录(FileLogicRepository)
 *   dcp.outlet.control-url          契约控制面 URL(缺省回退 permissive, 生产必须配置)
 *   dcp.outlet.contract-refresh-ms  契约视图刷新周期(默认 30000)
 *   dcp.outlet.contract-timeout-ms  契约拉取超时(默认 3000)
 * </pre>
 */
public class OutletComponents {

    private static final Logger log = LoggerFactory.getLogger("outlet-rt");

    private final UnitRunner runner;
    private final SimpleUnitMetrics metrics;

    public OutletComponents(ExecutorEnvironment env) {
        Map<String, String> cfg = env.env();
        this.metrics = new SimpleUnitMetrics();

        // 逻辑来源(按序尝试):③ 逻辑仓库 jar → ② 部署期 SPI → ① 同 classpath
        String repoRoot = cfg.getOrDefault("dcp.outlet.logic-repo", "./logic-repo");
        LogicLoader loader = new CompositeLogicLoader(
                new JarLogicLoader(new FileLogicRepository(Path.of(repoRoot))),
                new SpiLogicLoader(),
                new ClasspathLogicLoader(new InMemoryLogicRegistry()));

        // 契约提供者:安全链路 fail-closed —— 控制面 URL 缺失时告警回退 permissive(仅自检用)
        String controlUrl = cfg.get("dcp.outlet.control-url");
        ContractProviderFactory contractProviders;
        if (controlUrl == null || controlUrl.isBlank()) {
            log.warn("[outlet-rt] 未配置 dcp.outlet.control-url, 契约提供者回退 permissive(生产必须配置)");
            contractProviders = ContractProviderFactory.permissive();
        } else {
            long refreshMs = num(cfg, "dcp.outlet.contract-refresh-ms", 30000);
            long timeoutMs = num(cfg, "dcp.outlet.contract-timeout-ms", 3000);
            contractProviders = new HttpContractProviderFactory(
                    controlUrl, Duration.ofMillis(timeoutMs), refreshMs);
        }

        // 内存水位(与迁出前 WorkerConfig#watermarkProvider 同构)
        WatermarkProvider watermarks = new WatermarkProvider() {
            private final Map<Integer, Long> wm = new ConcurrentHashMap<>();
            @Override public void update(int partition, long watermarkTs) { wm.put(partition, watermarkTs); }
            @Override public long safeWatermark(int partition) { return wm.getOrDefault(partition, 0L); }
        };

        this.runner = new UnitRunner(loader,
                new SdkEventDecoder(),
                new DefaultContractFilter(new ExpressionRowConditionEvaluator(), new PlatformMasker(), null),
                new KafkaRecordSource.Factory(env.kafkaBootstrap("outlet")),
                watermarks,
                new InMemoryDeadLetterSink(),
                metrics,
                Slf4jSdkLogger::new,
                contractProviders);
    }

    public UnitRunner runner() { return runner; }

    public SimpleUnitMetrics metrics() { return metrics; }

    private static long num(Map<String, String> cfg, String key, long def) {
        try {
            return Long.parseLong(cfg.getOrDefault(key, String.valueOf(def)).trim());
        } catch (NumberFormatException e) {
            log.warn("配置 {} 非法,回退默认 {}: {}", key, def, cfg.get(key));
            return def;
        }
    }
}
