package com.dcp.outlet.rt;

import com.dcp.outlet.sdk.ConsumerLogicProvider;
import com.dcp.outlet.sdk.LoadedLogic;
import com.dcp.outlet.sdk.LogicLoader;
import com.dcp.outlet.sdk.LogicNotFoundException;
import com.dcp.outlet.sdk.LogicRef;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.ServiceLoader;

/**
 * 部署期 SPI 加载:第三方扩展包通过 {@code META-INF/services} 声明
 * {@link ConsumerLogicProvider};worker 启动时扫描一次并缓存。
 */
public class SpiLogicLoader implements LogicLoader {

    private final Map<LogicRef, ConsumerLogicProvider.ProviderEntry> entries = new LinkedHashMap<>();

    public SpiLogicLoader() {
        this(SpiLogicLoader.class.getClassLoader());
    }

    public SpiLogicLoader(ClassLoader classLoader) {
        ServiceLoader.load(ConsumerLogicProvider.class, classLoader).forEach(p ->
                p.logics().forEach(e -> entries.put(e.ref(), e)));
    }

    public int size() { return entries.size(); }

    @Override
    public LoadedLogic load(LogicRef ref) {
        ConsumerLogicProvider.ProviderEntry e = entries.get(ref);
        if (e == null) throw new LogicNotFoundException(ref);
        return new SimpleLoadedLogic(ref, e.logic());
    }
}
