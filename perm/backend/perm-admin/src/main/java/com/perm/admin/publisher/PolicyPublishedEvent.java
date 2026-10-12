package com.perm.admin.publisher;

import org.springframework.context.ApplicationEvent;

/**
 * 策略发布事件（R-M3 第五步：事件广播）
 * 演示级：Spring 进程内事件，日志打印 + 测试断言用。
 * 正式形态：投递 Kafka 主题 perm.policy.changed，PDP 订阅后策略级定点失效（L3 镜像更新），
 * 事件仅触发失效不预编译，新版本首次命中时按需编译（预热策略除外）。
 */
public class PolicyPublishedEvent extends ApplicationEvent {

    private final Publication publication;

    public PolicyPublishedEvent(Object source, Publication publication) {
        super(source);
        this.publication = publication;
    }

    public Publication publication() {
        return publication;
    }
}
