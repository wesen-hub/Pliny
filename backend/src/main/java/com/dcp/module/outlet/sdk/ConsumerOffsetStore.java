package com.dcp.module.outlet.sdk;

/**
 * 消费方自持位点(决策 1 / 设计 §6.8)。
 *
 * <p>两条铁律:
 * <ol>
 *   <li>位点在**消费方自己的库**,与业务表**同库**(才能与业务写同事务提交);</li>
 *   <li>平台侧只做观测,不作为交付依据。</li>
 * </ol>
 */
public interface ConsumerOffsetStore {

    /** 读取位点;无记录返回 null。数组为 {@code [epoch, tso]} */
    long[] load(String consumerId, String streamId);

    /** 写入/推进位点(调用方保证在业务写同一事务里执行) */
    void save(String consumerId, String streamId, int epoch, long positionTso);
}
