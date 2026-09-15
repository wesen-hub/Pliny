package com.dcp.outlet.sdk;

import java.util.List;

/**
 * 部署期 SPI(D14):第三方扩展包通过 {@code META-INF/services/} 声明本接口的实现,
 * worker 启动时由 {@code LogicLoader} 自动 load 并注册。
 *
 * <pre>
 * src/main/resources/META-INF/services/com.dcp.outlet.sdk.ConsumerLogicProvider
 *   com.mycorp.es.EsSyncLogicProvider
 * </pre>
 */
public interface ConsumerLogicProvider {

    List<ProviderEntry> logics();

    /** SPI 条目:逻辑引用 + 实例。 */
    final class ProviderEntry {

        private final LogicRef ref;
        private final ConsumerLogic logic;

        private ProviderEntry(LogicRef ref, ConsumerLogic logic) {
            this.ref = ref;
            this.logic = logic;
        }

        public static ProviderEntry of(String logicId, String version, ConsumerLogic logic) {
            return new ProviderEntry(LogicRef.of(logicId, version), logic);
        }

        public LogicRef ref() { return ref; }

        public ConsumerLogic logic() { return logic; }
    }
}
