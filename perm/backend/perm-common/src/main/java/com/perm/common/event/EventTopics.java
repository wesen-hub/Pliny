package com.perm.common.event;

/**
 * 事件主题常量（PDP / admin / PEP 跨服务共用契约）
 * POLICY_CHANGED：管理面发布 → PDP 订阅即时同步（Kafka 主通道 + 版本轮询兜底）
 * AUDIT：PDP 审计义务投递 → admin 订阅落库（sync 直写 / async 积压冲刷）
 */
public final class EventTopics {

    /** 策略变更事件主题 */
    public static final String POLICY_CHANGED = "perm.policy.changed";

    /** 审计事件主题 */
    public static final String AUDIT = "perm.audit";

    private EventTopics() {
    }
}
