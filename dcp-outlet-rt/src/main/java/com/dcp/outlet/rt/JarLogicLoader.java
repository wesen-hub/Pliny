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
import java.util.ServiceLoader;
import java.util.logging.Logger;

/**
 * **逻辑仓库 jar 加载**(方式③):定位 → **校验 sha256** → 独立 ClassLoader 加载 → 关闭时释放。
 *
 * <p>jar 内通过 {@code META-INF/services/com.dcp.outlet.sdk.ConsumerLogicProvider} 声明逻辑
 * (与部署期 SPI 同一契约),因此 jar 作者只需实现 SDK 接口、打上该文件。
 *
 * <p>父加载器传 SDK 的加载器:jar 不必自带 SDK,且保证 {@code ConsumerLogic} 类型是**同一个类**。
 */
public class JarLogicLoader implements LogicLoader {

    private static final Logger LOG = Logger.getLogger("outlet-logic-repo");

    private final LogicRepository repository;

    public JarLogicLoader(LogicRepository repository) { this.repository = repository; }

    @Override
    public LoadedLogic load(LogicRef ref) {
        LogicArtifact artifact = repository.locate(ref)
                .orElseThrow(() -> new LogicNotFoundException(ref));

        verifyHash(artifact);

        try {
            URLClassLoader cl = new URLClassLoader(
                    new URL[]{artifact.path().toUri().toURL()},
                    ConsumerLogic.class.getClassLoader());        // 父加载器 = SDK

            ServiceLoader<ConsumerLogicProvider> providers =
                    ServiceLoader.load(ConsumerLogicProvider.class, cl);
            for (ConsumerLogicProvider p : providers) {
                for (ConsumerLogicProvider.ProviderEntry e : p.logics()) {
                    if (e.ref().equals(ref)) {
                        LOG.info(() -> "loaded logic from repo: " + ref + " ← " + artifact.path());
                        return new JarLoadedLogic(ref, e.logic(), cl);
                    }
                }
            }
            cl.close();
            throw new LogicNotFoundException(ref);
        } catch (LogicNotFoundException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException("加载逻辑 jar 失败:" + artifact, e);
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

    /** jar 来源的加载结果:**close() 必须关闭 ClassLoader**(否则类加载器与文件句柄泄漏)。 */
    private static final class JarLoadedLogic implements LoadedLogic {
        private final LogicRef ref;
        private final ConsumerLogic logic;
        private final URLClassLoader classLoader;

        JarLoadedLogic(LogicRef ref, ConsumerLogic logic, URLClassLoader classLoader) {
            this.ref = ref; this.logic = logic; this.classLoader = classLoader;
        }

        @Override public LogicRef ref() { return ref; }
        @Override public ConsumerLogic logic() { return logic; }

        @Override
        public void close() {
            try {
                classLoader.close();
            } catch (Exception ignored) {
                // 关闭失败不掩盖业务异常
            }
        }
    }
}
