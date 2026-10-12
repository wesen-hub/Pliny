package com.perm.pdp.audit;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.perm.common.event.EventTopics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import jakarta.annotation.PreDestroy;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Kafka 审计记录器（perm.kafka.enabled=true 默认，正式形态主通道）
 * 义务语义（R-A）：
 *   sync  → 判定路径内同步投递（send + 确认），失败重试 1 次；仍失败仅告警并保留本地视图，
 *           不阻断判定（审计可用性不绑架判定可用性，由本地视图兜底排查）
 *   async → 入积压队列，后台单线程批量冲刷（每秒）；队列满时降级为同步直写不丢审计
 * 本地视图（recent/size）供 PDP 运维接口兜底查询；正式审计查询入口在 admin（落库）。
 */
@Component
@ConditionalOnProperty(name = "perm.kafka.enabled", havingValue = "true", matchIfMissing = true)
public class KafkaAuditRecorder implements AuditRecorder {

    private static final Logger log = LoggerFactory.getLogger(KafkaAuditRecorder.class);

    /** 积压容量：10k 条 ≈ 高并发下约 10 分钟缓冲 */
    private static final int BACKLOG_CAPACITY = 10_000;

    private final KafkaTemplate<String, String> kafka;
    private final ObjectMapper mapper = new ObjectMapper();
    private final ConcurrentLinkedQueue<Record> local = new ConcurrentLinkedQueue<>();
    private final LinkedBlockingQueue<Record> backlog = new LinkedBlockingQueue<>(BACKLOG_CAPACITY);
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final ScheduledExecutorService flusher = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "audit-flusher");
        t.setDaemon(true);
        return t;
    });

    public KafkaAuditRecorder(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafka = kafkaTemplate;
        flusher.scheduleWithFixedDelay(this::flushBacklog, 1, 1, TimeUnit.SECONDS);
    }

    @Override
    public void record(String userId, String resourceId, String action,
                       String decision, String mode, List<String> matchedPolicies) {
        Record r = new Record(Instant.now(), userId, resourceId, action, decision, mode, matchedPolicies);
        local.add(r);
        if ("sync".equalsIgnoreCase(mode)) {
            sendSync(r);
        } else if (!backlog.offer(r)) {
            // 积压满：降级同步直写（不静默丢弃审计）
            sendSync(r);
        }
    }

    @Override
    public List<Record> recent(int limit) {
        return local.stream().limit(limit).toList();
    }

    @Override
    public int size() {
        return local.size();
    }

    /** sync 义务：同步投递 + 确认，失败重试 1 次；仍失败告警降级（本地视图保留） */
    private void sendSync(Record r) {
        for (int attempt = 1; attempt <= 2; attempt++) {
            try {
                kafka.send(EventTopics.AUDIT, r.userId(), toJson(r)).get(2, TimeUnit.SECONDS);
                return;
            } catch (Exception e) {
                log.warn("[审计] sync 投递第 {} 次失败：user={} action={} err={}",
                        attempt, r.userId(), r.action(), e.getMessage());
            }
        }
        log.warn("[审计] sync 投递失败（不阻断判定，本地视图兜底）：user={} action={}", r.userId(), r.action());
    }

    /** async 义务：批量冲刷积压队列；失败条目及之后全部回队下次重试 */
    private void flushBacklog() {
        if (backlog.isEmpty()) {
            return;
        }
        List<Record> batch = new ArrayList<>(backlog.size());
        backlog.drainTo(batch);
        for (int i = 0; i < batch.size(); i++) {
            Record r = batch.get(i);
            try {
                kafka.send(EventTopics.AUDIT, r.userId(), toJson(r));
            } catch (Exception e) {
                log.warn("[审计] async 冲刷中断（{} 条回队重试）：{}", batch.size() - i, e.getMessage());
                for (int j = i; j < batch.size(); j++) {
                    if (!backlog.offer(batch.get(j))) {
                        log.warn("[审计] async 回队失败，审计丢失：user={} action={}", r.userId(), r.action());
                    }
                }
                return;
            }
        }
        log.debug("[审计] async 冲刷 {} 条", batch.size());
    }

    private String toJson(Record r) {
        try {
            return mapper.writeValueAsString(Map.of(
                    "ts", r.ts().toEpochMilli(),
                    "userId", r.userId(),
                    "resourceId", r.resourceId(),
                    "action", r.action(),
                    "decision", r.decision(),
                    "mode", r.mode(),
                    "matchedPolicies", r.matchedPolicies()));
        } catch (Exception e) {
            throw new IllegalStateException("审计序列化失败", e);
        }
    }

    @PreDestroy
    void shutdown() {
        running.set(false);
        flusher.shutdown();
        if (!backlog.isEmpty()) {
            log.info("[审计] 关闭前积压 {} 条，最后一次同步冲刷", backlog.size());
            flushBacklog();
        }
    }
}
