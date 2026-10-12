package com.perm.pdp.sync;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.perm.common.event.EventTopics;
import com.perm.pdp.policy.PolicySnapshotLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * 策略变更事件监听（R-Sync 正式形态主通道：Kafka 事件驱动即时同步）
 * 收到 perm.policy.changed 事件 → 事件版本高于本地 → 立即触发一次快照同步（复用轮询通道的 sync 逻辑）。
 * 事件仅触发失效/同步，不预编译（新版本首次命中时按需编译，预热策略除外）。
 * 降级：Kafka 不可达时由 SnapshotSyncService 版本比对轮询兜底（R-Sync 兜底通道）。
 */
@Component
@ConditionalOnProperty(name = "perm.kafka.enabled", havingValue = "true", matchIfMissing = true)
public class PolicyChangeListener {

    private static final Logger log = LoggerFactory.getLogger(PolicyChangeListener.class);

    private final PolicySnapshotLoader snapshotLoader;
    private final SnapshotSyncService syncService;
    private final ObjectMapper mapper = new ObjectMapper();

    public PolicyChangeListener(PolicySnapshotLoader snapshotLoader, SnapshotSyncService syncService) {
        this.snapshotLoader = snapshotLoader;
        this.syncService = syncService;
    }

    @KafkaListener(topics = EventTopics.POLICY_CHANGED, groupId = "${spring.application.name}")
    public void onPolicyChanged(String message) {
        try {
            JsonNode node = mapper.readTree(message);
            int eventVersion = node.path("version").asInt(0);
            int localVersion = snapshotLoader.current().version();
            if (eventVersion <= localVersion) {
                log.debug("[事件] 版本无变化（事件 v{} ≤ 本地 v{}），忽略", eventVersion, localVersion);
                return;
            }
            log.info("[事件] 收到策略变更 v{} > 本地 v{}，即时触发快照同步", eventVersion, localVersion);
            syncService.sync();
        } catch (Exception e) {
            log.warn("[事件] 消息解析/同步失败（轮询通道兜底）: {}", e.getMessage());
        }
    }
}
