package com.dcp.synclink.exec;

import com.dcp.scheduler.exec.ExecutorEnvironment;
import com.dcp.scheduler.exec.ExecutorPlugin;
import com.dcp.scheduler.exec.PartitionExecutor;
import com.dcp.synclink.core.SynclinkComponents;
import lombok.extern.slf4j.Slf4j;

/**
 * sync-writer 执行器工厂(一级 SPI): 框架经 ServiceLoader 发现,
 * 装配期(init)组装写路径组件, 每分区 create 一个执行器实例。
 */
@Slf4j
@ExecutorPlugin("sync-writer")
public class SyncWriterFactory implements PartitionExecutor.Factory {

    private volatile SynclinkComponents components;

    @Override
    public synchronized void init(ExecutorEnvironment env) {
        if (components == null) {
            components = new SynclinkComponents(env);
        }
    }

    @Override
    public PartitionExecutor create() {
        SynclinkComponents c = components;
        if (c == null) {
            throw new IllegalStateException("SyncWriterFactory 未初始化(Factory.init 未调用)");
        }
        return new SyncWriterExecutor(c.kafkaBootstrap(), c.syncConsumer(), c.batchWriteService());
    }
}
