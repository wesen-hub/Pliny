package com.dcp.outlet.rt;

import com.dcp.outlet.sdk.ConsumerLogic;
import com.dcp.outlet.sdk.ConsumerLogicProvider;
import com.dcp.outlet.sdk.LoadedLogic;
import com.dcp.outlet.sdk.LogicLoader;
import com.dcp.outlet.sdk.LogicNotFoundException;
import com.dcp.outlet.sdk.LogicRef;

import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

/**
 * **逻辑仓库 jar 加载**(方式③):定位 → **校验 sha256** → ClassLoader 加载 → 关闭时释放。
 *
 * <p>jar 内通过 {@code META-INF/services/com.dcp.outlet.sdk.ConsumerLogicProvider} 声明逻辑
 * (与部署期 SPI 同一契约),因此 jar 作者只需实现 SDK 接口、打上该文件。
 *
 * <p>父加载器传 SDK 的加载器:jar 不必自带 SDK,且保证 {@code ConsumerLogic} 类型是**同一个类**。
 *
 * <p><b>ClassLoader 缓存 + 引用计数(2026-10-15 落地)</b>:同一 {@code logicId@version} 的
 * ClassLoader **复用**——类元数据只加载一份(不再按分区叠加膨胀元空间);但每次 load 仍重新
 * ServiceLoader 扫描出**独立 logic 实例**(UnitRunner「一单元一实例」,状态隔离不变)。
 * 引用计数归零才真正 close ClassLoader 并移出缓存(共享 ClassLoader 不能"单元一停就关");
 * 缓存条目随归零即时清理,不残留泄漏源。
 */
public class JarLogicLoader implements LogicLoader {

    private static final Logger LOG = Logger.getLogger("outlet-logic-repo");

    private final LogicRepository repository;
    /** ClassLoader 缓存:key = logicId@version(LogicRef 自带 equals/hashCode)。 */
    private final Map<LogicRef, CacheEntry> cache = new HashMap<>();

    public JarLogicLoader(LogicRepository repository) { this.repository = repository; }

    @Override
    public LoadedLogic load(LogicRef ref) {
        LogicArtifact artifact = repository.locate(ref)
                .orElseThrow(() -> new LogicNotFoundException(ref));

        CacheEntry entry;
        synchronized (cache) {
            entry = cache.get(ref);
            if (entry == null) {
                verifyHash(artifact);          // 仅首次加载校验(缓存命中复用;版本不可变,同 ref 的 jar 不重载)
                try {
                    URLClassLoader cl = new URLClassLoader(
                            new URL[]{artifact.path().toUri().toURL()},
                            ConsumerLogic.class.getClassLoader());   // 父加载器 = SDK
                    entry = new CacheEntry(ref, cl);
                } catch (Exception e) {
                    throw new IllegalStateException("加载逻辑 jar 失败:" + artifact, e);
                }
                cache.put(ref, entry);
                LOG.info(() -> "cached classloader for " + ref + " <- " + artifact.path());
            }
            entry.refs.incrementAndGet();
        }

        // 复用缓存的 ClassLoader,但每次 new 独立 logic 实例(一单元一实例)
        ConsumerLogic logic = scan(entry.classLoader, ref);
        if (logic == null) {
            release(entry);   // 扫描未命中也要成对释放引用,不泄漏
            throw new LogicNotFoundException(ref);
        }
        return new JarLoadedLogic(ref, logic, entry);
    }

    /** 复用缓存 ClassLoader 重新 ServiceLoader 扫描 → 反射 new Provider → new logic 实例。 */
    private ConsumerLogic scan(URLClassLoader cl, LogicRef ref) {
        ServiceLoader<ConsumerLogicProvider> providers =
                ServiceLoader.load(ConsumerLogicProvider.class, cl);
        for (ConsumerLogicProvider p : providers) {
            for (ConsumerLogicProvider.ProviderEntry e : p.logics()) {
                if (e.ref().equals(ref)) {
                    return e.logic();
                }
            }
        }
        return null;
    }

    /** 引用计数 -1;归零才真正关闭 ClassLoader 并移出缓存(成对调用,见 {@link JarLoadedLogic#close})。 */
    private void release(CacheEntry entry) {
        synchronized (cache) {
            if (entry.refs.decrementAndGet() == 0) {
                cache.remove(entry.ref, entry);
                try {
                    entry.classLoader.close();
                } catch (Exception ignored) {
                    // 关闭失败不掩盖业务异常
                }
            }
        }
    }

    /** 哈希缺失 / 不符 = 制品不完整或被篡改 → **拒绝加载**(fail-closed,不带病运行)。 */
    private void verifyHash(LogicArtifact artifact) {
        String expected = artifact.sha256();
        if (expected == null || expected.isBlank()) {
            throw new IllegalStateException("未提供 sha256,拒绝加载(fail-closed): " + artifact
                    + "(发布流程必须写 sidecar;存量缺 sidecar 的 jar 需补写哈希)");
        }
        if (!Files.isRegularFile(artifact.path())) {
            throw new LogicNotFoundException(LogicRef.of(artifact.logicId(), artifact.version()));
        }
        String actual = FileLogicRepository.sha256(artifact.path());
        if (!actual.equalsIgnoreCase(expected)) {
            throw new IllegalStateException("逻辑 jar 哈希不匹配(拒绝加载):" + artifact
                    + " expected=" + expected + " actual=" + actual);
        }
    }

    /** 缓存的 ClassLoader + 引用计数。 */
    private static final class CacheEntry {
        final LogicRef ref;
        final URLClassLoader classLoader;
        final AtomicInteger refs = new AtomicInteger();

        CacheEntry(LogicRef ref, URLClassLoader classLoader) {
            this.ref = ref;
            this.classLoader = classLoader;
        }
    }

    /** jar 来源的加载结果:close() 释放引用(归零才关 ClassLoader);logic 实例每单元独立。 */
    private final class JarLoadedLogic implements LoadedLogic {
        private final LogicRef ref;
        private final ConsumerLogic logic;
        private final CacheEntry entry;

        JarLoadedLogic(LogicRef ref, ConsumerLogic logic, CacheEntry entry) {
            this.ref = ref; this.logic = logic; this.entry = entry;
        }

        @Override public LogicRef ref() { return ref; }
        @Override public ConsumerLogic logic() { return logic; }

        @Override
        public void close() {
            release(entry);   // decRef;归零才 close ClassLoader(共享时不能单元一停就关)
        }
    }
}
