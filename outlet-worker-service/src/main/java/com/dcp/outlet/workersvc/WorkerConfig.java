package com.dcp.outlet.workersvc;

import com.dcp.outlet.adapters.*;

import com.dcp.outlet.sdk.*;
import com.dcp.outlet.worker.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** 计算面装配:逻辑注册/加载 → 单元运行器(真 Kafka 消费源 + 控制面心跳续约)。 */
@Configuration
public class WorkerConfig {

    @Bean
    public LogicRegistry logicRegistry() {
        LogicRegistry registry = new InMemoryLogicRegistry();
        // 逻辑注册有三种方式(详细设计 §4):① 写在 worker 内由 @Component 注册;
        //   ② 部署期 SPI —— 由下面的 CompositeLogicLoader 扫描;③ 逻辑仓库 jar —— 后续增量接入
        return registry;
    }

    @Bean
    public LogicLoader logicLoader(LogicRegistry registry) {
        return new CompositeLogicLoader(new SpiLogicLoader(), new ClasspathLogicLoader(registry));
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

    @Bean
    public UnitRunner unitRunner(LogicLoader loader,
                                 @Value("${outlet.kafka.bootstrap}") String bootstrap,
                                 Heartbeat heartbeat, LeaseGuard leaseGuard,
                                 WatermarkProvider watermarks) {
        return new UnitRunner(loader,
                new SdkEventDecoder(),
                DefaultContractFilter.permissive(),                 // 契约引擎接入见后续增量
                new KafkaRecordSource.Factory(bootstrap),
                leaseGuard, heartbeat, watermarks,
                new InMemoryDeadLetterSink(),                        // 生产替换为 JdbcDeadLetterSink
                new SimpleUnitMetrics(),
                id -> new Slf4jSdkLogger(id));
    }
}
