package com.perm.admin.publisher;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Optional;

/**
 * 发布服务（R-M3 第五步事件广播 + 第六步确认存档）
 * 存档委托 PublicationHistory（内存/MySQL 双实现，perm.storage 切换）；
 * 广播双通道：Spring 进程内事件（测试/日志）+ Kafka 主题 perm.policy.changed（正式形态主通道，PDP 订阅定点失效）。
 */
@Component
public class PublicationService {

    private static final Logger log = LoggerFactory.getLogger(PublicationService.class);

    private final ApplicationEventPublisher eventPublisher;
    private final PublicationHistory history;
    private final Optional<PolicyChangePublisher> changePublisher;

    public PublicationService(ApplicationEventPublisher eventPublisher,
                              PublicationHistory history,
                              Optional<PolicyChangePublisher> changePublisher) {
        this.eventPublisher = eventPublisher;
        this.history = history;
        this.changePublisher = changePublisher;
    }

    /** 广播 + 存档（发布成功后调用） */
    public Publication announce(Publication publication) {
        history.add(publication);
        eventPublisher.publishEvent(new PolicyPublishedEvent(this, publication));
        changePublisher.ifPresent(p -> p.publish(publication));
        log.info("[发布] v{} {} policies={} by={} conflicts={}",
                publication.version(), publication.changeType(),
                publication.policyIds(), publication.operator(),
                publication.conflictSummary());
        return publication;
    }

    public List<Publication> history() {
        return history.all();
    }
}
