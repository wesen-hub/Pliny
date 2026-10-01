package com.dcp.indexer.exec;

import com.dcp.indexer.core.IndexerComponents;
import com.dcp.scheduler.exec.ExecutorEnvironment;
import com.dcp.scheduler.exec.PartitionExecutor;
import com.dcp.scheduler.exec.ExecutorPlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * anchor-sampler 执行器工厂(一级 SPI):宿主 dcp-worker 经 META-INF/services 发现本工厂,
 * 装配期调用一次 {@link #init(ExecutorEnvironment)} 构建共享组件,每分区 {@link #create()}
 * 一次执行器(与迁出前 IndexerSchedulerRuntime 的 EMBEDDED 注册同构)。
 */
@ExecutorPlugin("anchor-sampler")
public class IndexerFactory implements PartitionExecutor.Factory {

    private static final Logger log = LoggerFactory.getLogger(IndexerFactory.class);

    private volatile IndexerComponents components;

    @Override
    public synchronized void init(ExecutorEnvironment env) {
        if (components == null) {
            components = new IndexerComponents(env);
            log.info("[anchor-sampler] 组件装配完成 epoch={} group={} kafka={}",
                    components.epoch(), components.groupId(), components.kafkaBootstrap());
        }
    }

    @Override
    public PartitionExecutor create() {
        IndexerComponents c = components;
        if (c == null) {
            throw new IllegalStateException("IndexerFactory 未初始化(Factory.init 未调用)");
        }
        return new AnchorSamplerExecutor(c.kafkaBootstrap(), c.indexer(), c.epoch(),
                c.groupId(), c.fromBeginning(), c.alert());
    }
}
