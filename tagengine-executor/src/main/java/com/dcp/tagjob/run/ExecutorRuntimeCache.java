package com.dcp.tagjob.run;

import com.dcp.tagjob.drl.KieBaseFactory;
import com.dcp.tagjob.drl.KieBaseFactory.KieBuildResult;
import com.dcp.tagjob.plan.BuildPlan;
import com.dcp.tagjob.util.Log;
import org.kie.api.KieBase;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * executor 侧运行时缓存(设计文档 §12: Fact Java 源码 + DRL 文本 broadcast → 每个 executor 用
 * KieBaseFactory 本地重建 KieModule(KieBuilder 一并编译 Fact 源码与 DRL), 避免跨 JVM KieBase
 * 反序列化依赖动态类的坑; 每 executor 一次, 缓存复用)。
 *
 * 多 partition/多任务线程共享同一 JVM 内缓存: key = taskKey; 构建有锁保护。
 *
 * <p>不再有任何 TCCL/自定义 ClassLoader 黑客: Fact 类 = 从 build 结果 moduleClassLoader
 * (KieContainer.getClassLoader(), 与 KieBase 内部同源) Class.forName 加载; setter 反射表基于该类。
 */
public final class ExecutorRuntimeCache {

    private static final ConcurrentHashMap<String, ExecutorRuntime> CACHE = new ConcurrentHashMap<>();

    private ExecutorRuntimeCache() {
    }

    public static ExecutorRuntime get(BuildPlan plan) {
        return CACHE.computeIfAbsent(plan.taskKey, k -> build(plan));
    }

    private static ExecutorRuntime build(BuildPlan plan) {
        // 锁: 同 JVM 同 taskKey 并发首建时串行
        synchronized (ExecutorRuntimeCache.class) {
            ExecutorRuntime cached = CACHE.get(plan.taskKey);
            if (cached != null) return cached;
            long t0 = System.currentTimeMillis();

            // 本地重建 KieModule: Fact 源码 + 每资产 DRL 一并编译(Drools 标准姿势, 无需 TCCL)
            List<String> errs = new ArrayList<>();
            KieBuildResult res = KieBaseFactory.buildBase(
                    plan.assets, plan.factSpec.javaSource, plan.factSpec.className, errs);
            if (res == null) {
                throw new IllegalStateException("executor 本地 KieBase 重建失败:\n" + String.join("\n", errs));
            }
            ClassLoader moduleLoader = res.moduleClassLoader();
            KieBase base = res.kieBase();

            // Fact 类从 moduleClassLoader 加载(与 KieBase 内部同源, insert 的对象即 Drools 认识的类)
            Class<?> factClass;
            try {
                factClass = Class.forName(plan.factSpec.className, true, moduleLoader);
            } catch (ClassNotFoundException e) {
                throw new IllegalStateException("Fact 类加载失败(moduleClassLoader): " + plan.factSpec.className, e);
            }

            // setter 预解析(按 schema 类型表)
            Map<String, Method> setters = new LinkedHashMap<>();
            for (BuildPlan.FactField f : plan.factFields) {
                String cap = Character.toUpperCase(f.name.charAt(0)) + f.name.substring(1);
                try {
                    setters.put(f.name, factClass.getMethod("set" + cap, jType(f.kind)));
                } catch (NoSuchMethodException e) {
                    throw new IllegalStateException("Fact setter 解析失败: " + f.name, e);
                }
            }
            Log.info("[executor] 任务 " + plan.taskKey + " KieModule/KieBase 本地重建完成, 资产数=" + plan.assets.size()
                    + ", 耗时=" + (System.currentTimeMillis() - t0) + "ms");
            return new ExecutorRuntime(moduleLoader, factClass, base, setters);
        }
    }

    private static Class<?> jType(char kind) {
        return switch (kind) {
            case 'L' -> Long.class;
            case 'D' -> Double.class;
            case 'B' -> Boolean.class;
            case 'S' -> String.class;
            default -> throw new IllegalArgumentException("未知 Fact 字段类型: " + kind);
        };
    }

    /** executor 单 JVM 运行时: moduleClassLoader + Fact 类 + KieBase + setter 表(分区线程共享, 只读) */
    public static final class ExecutorRuntime {
        /** KieContainer.getClassLoader() —— 加载 KieModule 内编译类(Fact 等), 与 KieBase 内部同源 */
        public final ClassLoader loader;
        public final Class<?> factClass;
        public final KieBase base;
        public final Map<String, Method> setters;

        ExecutorRuntime(ClassLoader loader, Class<?> factClass, KieBase base,
                        Map<String, Method> setters) {
            this.loader = loader;
            this.factClass = factClass;
            this.base = base;
            this.setters = setters;
        }
    }
}
