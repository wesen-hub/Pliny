package com.dcp.outlet.sdk;

import java.util.Map;

/**
 * 批级上下文(每批一个,平台构造并注入)。
 *
 * <p>单元级状态(连接、缓存)请放 {@code init()} 里持有的字段;本对象是**只读视图**。
 */
public interface ConsumerContext {

    String taskId();

    /** 消费组 = {@code task-{taskId}}(一任务一组)。 */
    String consumerGroup();

    Mode mode();

    /** 当前批次来自哪个分区。 */
    int partition();

    /**
     * 安全水位:语义是"≤ 此 TSO 的事件**已全部到齐**",**不是**"已处理到"。
     * 用于延迟 / 窗口聚合判断。
     */
    long safeWatermark();

    /** 滞后是否已小于"追平"阈值。 */
    boolean caughtUp();

    /** 同 {@link LogicInitContext#params()}。 */
    Map<String, Object> params();

    String logicId();

    String logicVersion();

    SdkLogger log();

    /**
     * 显式提交位点。
     *
     * <p><b>正常路径不要调用</b> —— {@code process()} 正常返回后平台会自动 {@code commitSync()}。
     * 仅在"异步 / 攒批"场景需要自己掌控提交时机时使用。
     */
    void commit();
}
