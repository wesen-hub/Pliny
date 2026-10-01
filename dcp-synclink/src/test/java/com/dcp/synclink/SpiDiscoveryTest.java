package com.dcp.synclink;

import com.dcp.scheduler.exec.ExecutorPlugin;
import com.dcp.scheduler.exec.PartitionExecutor;
import com.dcp.synclink.exec.SyncWriterFactory;
import org.junit.jupiter.api.Test;

import java.util.ServiceLoader;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * SPI 装载冒烟: ServiceLoader 能发现 SyncWriterFactory, @ExecutorPlugin 名为 sync-writer。
 */
class SpiDiscoveryTest {

    @Test
    void serviceLoaderDiscoversSyncWriterFactory() {
        ServiceLoader<PartitionExecutor.Factory> loader =
                ServiceLoader.load(PartitionExecutor.Factory.class,
                        SpiDiscoveryTest.class.getClassLoader());
        var found = loader.stream()
                .filter(p -> p.type() == SyncWriterFactory.class)
                .toList();
        assertTrue(!found.isEmpty(), "ServiceLoader 未发现 SyncWriterFactory(检查 META-INF/services 声明)");
        ExecutorPlugin annotation = SyncWriterFactory.class.getAnnotation(ExecutorPlugin.class);
        assertEquals("sync-writer", annotation.value());
    }
}
