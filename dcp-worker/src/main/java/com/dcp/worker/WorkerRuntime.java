package com.dcp.worker;

import com.dcp.scheduler.exec.ExecutorProvider;
import com.dcp.scheduler.model.KafkaConfig;
import com.dcp.scheduler.model.ScaleRules;
import com.dcp.scheduler.skeleton.InstanceConfig;
import com.dcp.scheduler.skeleton.InstanceSkeleton;
import com.dcp.scheduler.store.EtcdStore;
import com.dcp.scheduler.store.KeySpace;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;

/**
 * worker 运行时(方案 §8.2 装配流程的第 4-6 步):一镜像一骨架多 ns。
 *
 * <pre>
 *   白名单 ClassLoader 装载业务 jar → ServiceLoader 发现 Factory
 *   → Factory.init(WorkerEnvironment) → 注册 ExecutorProvider
 *   → InstanceSkeleton(ns=sync/indexer/outlet 多 ns 声明)启动 → resubmitAll 由框架自愈。
 *   ns 的 lag 探针 Kafka 地址(平台栈 9092 / 出口栈 19092)按 ns 写 kafkaConfig。
 * </pre>
 */
@Component
public class WorkerRuntime {

    private static final Logger log = LoggerFactory.getLogger(WorkerRuntime.class);

    @Value("${dcp.etcd.endpoints:http://localhost:2379}")
    private String etcd;
    @Value("${dcp.worker.instance-id:dcp-worker-1}")
    private String instanceId;
    /** 归属 ns 声明(sync/indexer/outlet,逗号分隔 ns:capacity:campaign)。 */
    @Value("${dcp.worker.namespaces:}")
    private String namespaces;

    private final BundleAssembler bundleAssembler;
    private final WorkerEnvironment environment;

    private EtcdStore store;
    private InstanceSkeleton skeleton;
    private final List<WhitelistClassLoader> bundleLoaders = new ArrayList<>();

    public WorkerRuntime(BundleAssembler bundleAssembler, WorkerEnvironment environment) {
        this.bundleAssembler = bundleAssembler;
        this.environment = environment;
    }

    @PostConstruct
    public void start() {
        ExecutorProvider provider = new ExecutorProvider(environment);
        for (WhitelistClassLoader cl : bundleAssembler.assemble()) {
            bundleLoaders.add(cl);
            provider.discoverSpi(cl);
        }
        store = EtcdStore.connect(etcd);
        InstanceConfig cfg = new InstanceConfig();
        cfg.id = instanceId;
        cfg.addr = localAddr();
        cfg.etcd = etcd;
        cfg.hooks = null;      // 一阶段:消费/采样任务只扩不缩
        for (String decl : namespaces.split(",")) {
            String d = decl.trim();
            if (d.isEmpty()) continue;
            String[] parts = d.split(":");
            String ns = parts[0].trim();
            int capacity = parts.length > 1 ? Integer.parseInt(parts[1].trim()) : 8;
            boolean campaign = parts.length <= 2 || Boolean.parseBoolean(parts[2].trim());
            cfg.ns(ns, capacity, campaign);
        }
        skeleton = new InstanceSkeleton(cfg, store, provider);
        skeleton.start();
        // 每 ns:扩缩规则(只扩不缩)+ lag 探针 Kafka 地址(平台栈/出口栈不同)
        for (InstanceConfig.NsDecl ns : cfg.namespaces) {
            ScaleRules rules = new ScaleRules();
            rules.downEnabled = false;
            store.createIfAbsent(KeySpace.rules(ns.ns), rules, null);
            String bootstrap = environment.kafkaBootstrap(ns.ns);
            if (bootstrap != null && !bootstrap.isBlank()) {
                KafkaConfig kc = new KafkaConfig();
                kc.bootstrapServers = bootstrap;
                store.put(KeySpace.kafkaConfig(ns.ns), kc);
            }
        }
        log.info("[worker] 骨架启动: instance={} ns={} etcd={} bundles={}",
                instanceId, cfg.namespaces.size(), etcd, bundleLoaders.size());
    }

    @PreDestroy
    public void stop() {
        if (skeleton != null) {
            skeleton.close();
            log.info("[worker] 骨架已停止: {}", instanceId);
        }
        for (WhitelistClassLoader cl : bundleLoaders) {
            try {
                cl.close();
            } catch (Exception e) {
                log.warn("[worker] 关闭业务 jar ClassLoader 失败: {}", e.getMessage());
            }
        }
        bundleLoaders.clear();
    }

    /** 框架托管是否生效(健康检查用)。 */
    public boolean isEnabled() {
        return skeleton != null;
    }

    /** 任务提交/注销的统一入口(REST 控制器复用)。 */
    public EtcdStore store() {
        return store;
    }

    private static String localAddr() {
        try {
            return InetAddress.getLocalHost().getHostAddress();
        } catch (Exception e) {
            return "127.0.0.1";
        }
    }
}
