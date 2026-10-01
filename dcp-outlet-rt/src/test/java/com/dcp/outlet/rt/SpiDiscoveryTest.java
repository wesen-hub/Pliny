package com.dcp.outlet.rt;

import com.dcp.scheduler.exec.ExecutorPlugin;
import com.dcp.scheduler.exec.PartitionExecutor;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * SPI 发现单测:宿主装载路径的编译期等价验证
 * (META-INF/services 声明 → ServiceLoader 发现 → @ExecutorPlugin 插件名)。
 */
class SpiDiscoveryTest {

    @Test
    void factoryDiscoverableViaServiceLoader() {
        List<PartitionExecutor.Factory> factories = new ArrayList<>();
        for (PartitionExecutor.Factory f : ServiceLoader.load(PartitionExecutor.Factory.class)) {
            factories.add(f);
        }
        assertFalse(factories.isEmpty(), "应经 SPI 声明发现至少一个 Factory");
        assertTrue(factories.stream().anyMatch(f -> f instanceof OutletFactory),
                "应发现 OutletFactory");
    }

    @Test
    void pluginNameIsOutletTask() {
        ExecutorPlugin ann = OutletFactory.class.getAnnotation(ExecutorPlugin.class);
        assertEquals("outlet-task", ann.value());
    }
}
