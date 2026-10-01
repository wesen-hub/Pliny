package com.dcp.outlet.rt;

import com.dcp.scheduler.exec.ExecutorEnvironment;
import com.dcp.scheduler.exec.ExecutorPlugin;
import com.dcp.scheduler.exec.PartitionExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * outlet-task 执行器工厂(一级 SPI):宿主 dcp-worker 经 META-INF/services 发现本工厂,
 * 装配期调用一次 {@link #init(ExecutorEnvironment)} 构建共享 UnitRunner,每分区
 * {@link #create()} 一次执行器(与迁出前 WorkerSchedulerRuntime 的 EMBEDDED 注册同构)。
 */
@ExecutorPlugin("outlet-task")
public class OutletFactory implements PartitionExecutor.Factory {

    private static final Logger log = LoggerFactory.getLogger(OutletFactory.class);

    private volatile OutletComponents components;

    @Override
    public synchronized void init(ExecutorEnvironment env) {
        if (components == null) {
            components = new OutletComponents(env);
            log.info("[outlet-task] 组件装配完成(UnitRunner 链已构建)");
        }
    }

    @Override
    public PartitionExecutor create() {
        OutletComponents c = components;
        if (c == null) {
            throw new IllegalStateException("OutletFactory 未初始化(Factory.init 未调用)");
        }
        return new PartitionTaskExecutor(c.runner(), c.metrics());
    }
}
