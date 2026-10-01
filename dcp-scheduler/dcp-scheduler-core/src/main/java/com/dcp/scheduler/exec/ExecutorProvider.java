package com.dcp.scheduler.exec;

import com.dcp.scheduler.model.Enums;
import com.dcp.scheduler.model.TaskSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 执行器装载(总纲 §五):
 *   EMBEDDED:框架内置执行器,按名注册(启动期自检存在性);
 *   SPI:业务按接口打 jar,ServiceLoader 发现,按插件名实例化。
 * 提交时与分配时各校验一次装载可用性;缺失 → 拒绝创建/FAILED,不影响其他任务。
 *
 * <p>微服务架构方案 §8:支持业务 jar 独立 ClassLoader 装载(discoverSpi(ClassLoader)),
 * 插件名优先 @ExecutorPlugin.value()(替代实现类简单名);装配期经
 * Factory.init(ExecutorEnvironment) 注入宿主环境(连接地址/元库数据源)。
 */
public class ExecutorProvider {

    private static final Logger log = LoggerFactory.getLogger(ExecutorProvider.class);

    private final Map<String, PartitionExecutor.Factory> embedded = new ConcurrentHashMap<>();
    private final Map<String, PartitionExecutor.Factory> spiPlugins = new ConcurrentHashMap<>();
    private final ExecutorEnvironment environment;

    /** 已注入环境的工厂(init 幂等,每工厂一次)。 */
    private final Set<PartitionExecutor.Factory> initialized = ConcurrentHashMap.newKeySet();

    public ExecutorProvider() {
        this(null);
    }

    public ExecutorProvider(ExecutorEnvironment environment) {
        this.environment = environment;
        discoverSpi(Thread.currentThread().getContextClassLoader());
    }

    /** SPI 发现(可指定业务 jar 独立 ClassLoader,宿主装配器调用)。幂等:同名后注册者忽略。 */
    public synchronized void discoverSpi(ClassLoader classLoader) {
        if (classLoader == null) {
            return;
        }
        for (PartitionExecutor.Factory f : ServiceLoader.load(PartitionExecutor.Factory.class, classLoader)) {
            String name = pluginName(f);
            if (spiPlugins.putIfAbsent(name, f) == null) {
                log.info("发现 SPI 执行器插件: {} ({})", name, f.getClass().getName());
            }
        }
    }

    /** 插件名:优先 @ExecutorPlugin.value(),缺省实现类简单名(向后兼容)。 */
    private static String pluginName(PartitionExecutor.Factory f) {
        ExecutorPlugin annotation = f.getClass().getAnnotation(ExecutorPlugin.class);
        return annotation != null ? annotation.value() : f.getClass().getSimpleName();
    }

    /** 注册内置执行器(EMBEDDED)。 */
    public void registerEmbedded(String name, PartitionExecutor.Factory factory) {
        embedded.put(name, factory);
        log.info("注册内置执行器: {}", name);
    }

    /** 校验可装载(任务提交时调用)。 */
    public boolean checkLoadable(TaskSpec.HandlerRef ref) {
        return ref != null && ref.ref != null
                && (Enums.HandlerType.EMBEDDED.equals(ref.type)
                        ? embedded.containsKey(ref.ref)
                        : spiPlugins.containsKey(ref.ref));
    }

    /** 装载(分配后调用)。失败抛异常,由骨架记为 FAILED。装配期注入环境(每工厂一次)。 */
    public PartitionExecutor load(TaskSpec.HandlerRef ref) {
        PartitionExecutor.Factory f = Enums.HandlerType.EMBEDDED.equals(ref.type)
                ? embedded.get(ref.ref)
                : spiPlugins.get(ref.ref);
        if (f == null) {
            throw new IllegalStateException("执行器不可装载: " + ref.type + ":" + ref.ref);
        }
        if (environment != null && initialized.add(f)) {
            log.info("注入执行器环境: {}", ref.ref);
            f.init(environment);
        }
        return f.create();
    }
}
