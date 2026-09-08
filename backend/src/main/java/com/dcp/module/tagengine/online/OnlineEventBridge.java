package com.dcp.module.tagengine.online;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

/**
 * 在线事件源自动接线(P1.2③; 设计 §4: 在线事件 = 单事件(Kafka/API)): 独立订阅 dcp.in.*(与数据面 sync 消费解耦),
 * 事件按 dataset(=topic 后缀) 路由到 published 的 online 任务 → OnlineTagService 求值置位。
 * 至此在线层与流层同源(同一 dcp.in 事件流), 生产不再依赖手工 HTTP 发事件(该 API 保留作调试/离线重放)。
 *
 * <p>信封与流一致: {op, seq, fields:{...}}(dcp.in 契约), 取 fields 映射。
 * 消费起点 latest(在线=实时置位, 不重放历史); 每任务独立求值, 失败只记日志不阻断后续。
 */
@Slf4j
@Component
public class OnlineEventBridge {

    private static final ObjectMapper OM = new ObjectMapper();
    private static final String PREFIX = "dcp.in.";
    private static final Pattern TOPIC_PATTERN = Pattern.compile("dcp\\.in\\..*");

    private final KafkaProperties kafkaProperties;
    private final JdbcTemplate metaJdbc;
    private final OnlineTagService onlineTagService;

    /** dataset -> online 任务 id 列表(30s 缓存; online 任务发布/删除后最迟 30s 生效) */
    private final Map<String, List<Long>> routeCache = new ConcurrentHashMap<>();
    private volatile long routeCacheAt = 0L;

    public OnlineEventBridge(KafkaProperties kafkaProperties, JdbcTemplate metaJdbc,
                             OnlineTagService onlineTagService) {
        this.kafkaProperties = kafkaProperties;
        this.metaJdbc = metaJdbc;
        this.onlineTagService = onlineTagService;
    }

    @PostConstruct
    public void start() {
        try {
            var props = kafkaProperties.buildConsumerProperties(null);
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
            props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
            DefaultKafkaConsumerFactory<String, String> factory = new DefaultKafkaConsumerFactory<>(props);
            ContainerProperties cp = new ContainerProperties(TOPIC_PATTERN);
            cp.setGroupId("dcp-tag-online");
            cp.setAckMode(ContainerProperties.AckMode.RECORD);
            cp.setMessageListener((MessageListener<String, String>) this::onRecord);
            ConcurrentMessageListenerContainer<String, String> container =
                    new ConcurrentMessageListenerContainer<>(factory, cp);
            container.setConcurrency(1);
            container.start();
            log.info("[tagengine] 在线事件源接线: 订阅 dcp.in.* group=dcp-tag-online (Kafka 自动派发 online 任务)");
        } catch (Exception e) {
            log.error("[tagengine] 在线事件源启动失败(online 退化为仅 HTTP 事件): {}", e.getMessage(), e);
        }
    }

    private void onRecord(ConsumerRecord<String, String> record) {
        String topic = record.topic();
        if (!topic.startsWith(PREFIX)) return;
        String dataset = topic.substring(PREFIX.length());
        if (dataset.isEmpty()) return;
        List<Long> taskIds = onlineTasksOf(dataset);
        if (taskIds.isEmpty()) return;
        JsonNode ev;
        try {
            ev = OM.readTree(record.value());
        } catch (Exception e) {
            return;   // 非 JSON 丢弃(与流一致)
        }
        JsonNode fields = ev.has("fields") ? ev.path("fields") : ev;
        if (fields == null || !fields.isObject()) return;
        Map<String, Object> fm = OM.convertValue(fields, LinkedHashMap.class);
        for (Long taskId : taskIds) {
            try {
                onlineTagService.handleEvent(taskId, fm, null);
            } catch (Exception e) {
                log.warn("[tagengine] 在线任务#{} 事件求值失败(dataset={}): {}", taskId, dataset, e.getMessage());
            }
        }
    }

    private List<Long> onlineTasksOf(String dataset) {
        long now = System.currentTimeMillis();
        if (now - routeCacheAt > 30_000L) {
            synchronized (routeCache) {
                if (now - routeCacheAt > 30_000L) {
                    Map<String, List<Long>> fresh = new HashMap<>();
                    List<Map<String, Object>> rows = metaJdbc.queryForList(
                            "SELECT id,dataset FROM tag_task WHERE mode='online' AND status='published'");
                    for (Map<String, Object> r : rows) {
                        String ds = String.valueOf(r.get("dataset"));
                        fresh.computeIfAbsent(ds, k -> new ArrayList<>())
                                .add(((Number) r.get("id")).longValue());
                    }
                    routeCache.clear();
                    routeCache.putAll(fresh);
                    routeCacheAt = now;
                }
            }
        }
        return routeCache.getOrDefault(dataset, List.of());
    }
}
