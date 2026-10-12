package com.perm.admin.publisher;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.perm.common.event.EventTopics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * Kafka 事件投递（正式形态主通道：主题 perm.policy.changed）
 * PDP 订阅后策略级定点失效（L3 镜像即时更新），版本比对轮询仅作兜底。
 * 投递失败不阻断发布（发布原子性以持久化为准）；producer max.block.ms 收敛快速失败，避免 Kafka 不可用时阻塞发布链路。
 */
@Component
@ConditionalOnProperty(name = "perm.kafka.enabled", havingValue = "true", matchIfMissing = true)
public class KafkaPolicyChangePublisher implements PolicyChangePublisher {

    private static final Logger log = LoggerFactory.getLogger(KafkaPolicyChangePublisher.class);

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper mapper = new ObjectMapper();

    public KafkaPolicyChangePublisher(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @Override
    public void publish(Publication publication) {
        try {
            Map<String, Object> payload = Map.of(
                    "version", publication.version(),
                    "changeType", publication.changeType(),
                    "policyIds", publication.policyIds(),
                    "operator", publication.operator() == null ? "" : publication.operator(),
                    "ts", publication.ts() == null ? System.currentTimeMillis() : publication.ts().toEpochMilli());
            kafkaTemplate.send(EventTopics.POLICY_CHANGED, String.valueOf(publication.version()), mapper.writeValueAsString(payload));
            log.info("[事件] 已投递 {}：v{} policies={}", EventTopics.POLICY_CHANGED, publication.version(), publication.policyIds());
        } catch (Exception e) {
            log.warn("[事件] Kafka 投递失败（不阻断发布，依赖版本轮询兜底）: {}", e.getMessage());
        }
    }
}
