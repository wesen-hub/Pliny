package com.dcp.scheduler.exec;

import com.dcp.scheduler.model.ExecutorSnapshot;

import java.util.Map;

/**
 * 分区执行器契约(总纲 §五):业务侧实现,每分区一线程一实例。
 * 框架只管调度与生命周期;消费逻辑、位点管理、提交节奏全部由实现方自控
 * (内核拍板:框架不管理位点,只负责调度)。
 */
public interface PartitionExecutor {

    /** 启动:ctx 携带业务需要的一切。必须幂等且快速返回,长循环放后台线程。 */
    void start(ExecutorContext ctx);

    /** 停止:优雅退出。实现方负责 flush 与清理,幂等。 */
    void stop();

    /** 快照:四计数器(业务零指标概念)。框架 10s 拉一次写 etcd(TTL 30s)。 */
    ExecutorSnapshot metrics();

    /** 工厂:每分区线程一个实例(装载入口,EMBEDDED/SPI 共用)。 */
    interface Factory {
        /** 环境注入钩子(装配期调用一次,早于任何 create):业务 jar 在此缓存连接地址/数据源。
         *  default 实现保持向后兼容(老式工厂无需实现)。 */
        default void init(ExecutorEnvironment env) {
        }

        PartitionExecutor create();
    }

    /** 执行器上下文:框架把分配结果原样透传给执行器。 */
    final class ExecutorContext {
        public String ns;
        public String taskId;
        public String topic;
        public int partId;
        public String handover;
        public Map<String, String> bizMeta;
    }
}
