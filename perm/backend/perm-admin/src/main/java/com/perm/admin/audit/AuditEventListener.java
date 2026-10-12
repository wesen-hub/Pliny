package com.perm.admin.audit;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.perm.common.event.EventTopics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * 审计事件订阅（R-A 消费端：PDP 投递 perm.audit → 落库 AuditStore）
 * 解析失败仅告警不中断消费（审计单条失败不阻塞后续消息）。
 */
@Component
@ConditionalOnProperty(name = "perm.kafka.enabled", havingValue = "true", matchIfMissing = true)
public class AuditEventListener {

    private static final Logger log = LoggerFactory.getLogger(AuditEventListener.class);

    private final AuditStore store;
    private final ObjectMapper mapper = new ObjectMapper();

    public AuditEventListener(AuditStore store) {
        this.store = store;
    }

    @KafkaListener(topics = EventTopics.AUDIT, groupId = "${spring.application.name}")
    public void onAudit(String message) {
        try {
            JsonNode node = mapper.readTree(message);
            List<String> policies = new ArrayList<>();
            node.path("matchedPolicies").forEach(p -> policies.add(p.asText()));
            store.save(new AuditRecord(
                    Instant.ofEpochMilli(node.path("ts").asLong(System.currentTimeMillis())),
                    node.path("userId").asText(""),
                    node.path("resourceId").asText(""),
                    node.path("action").asText(""),
                    node.path("decision").asText(""),
                    node.path("mode").asText("async"),
                    policies));
        } catch (Exception e) {
            log.warn("[审计] 消息解析失败（跳过本条）: {}", e.getMessage());
        }
    }
}
