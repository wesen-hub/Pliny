package com.perm.admin.publisher;

/**
 * 策略变更事件投递通道（R-M3 第五步：事件广播，正式形态 Kafka 主通道）
 * 投递失败不阻断发布（发布原子性以持久化为准），仅降级为轮询兜底通道。
 */
public interface PolicyChangePublisher {

    /** 投递发布事件（异步，失败仅告警） */
    void publish(Publication publication);
}
