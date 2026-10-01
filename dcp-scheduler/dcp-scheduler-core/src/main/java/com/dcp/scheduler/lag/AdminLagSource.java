package com.dcp.scheduler.lag;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.clients.admin.AdminClientConfig;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * 内置 Kafka Admin 实现:listOffsets 查 topic 全分区 endOffset。
 * 框架对 Kafka 只读——不建消费者组、不提交位点,仅周期读水位。
 * <p>自愈:AdminClient 在 topic 删除重建等元数据剧变后可能进入坏状态(无限 METADATA 重试),
 * 请求失败时重建实例(下次调用重新 bootstrap)。
 */
public class AdminLagSource implements KafkaLagSource {

    private final Map<String, Object> cfg;
    private volatile AdminClient admin;

    public AdminLagSource(String bootstrapServers) {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        cfg.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 10_000);
        this.cfg = cfg;
    }

    private AdminClient admin() {
        if (admin == null) {
            synchronized (this) {
                if (admin == null) admin = AdminClient.create(cfg);
            }
        }
        return admin;
    }

    private synchronized void resetAdmin() {
        if (admin != null) {
            try {
                admin.close();
            } catch (Exception ignored) { /* 关闭失败也继续重建 */ }
            admin = null;
        }
    }

    @Override
    public Map<Integer, Long> endOffsets(String topic) throws Exception {
        try {
            return endOffsets0(topic);
        } catch (Exception e) {
            resetAdmin();   // 实例可能已坏(元数据剧变后无限重试),重建一次供下次调用自愈
            throw e;
        }
    }

    private Map<Integer, Long> endOffsets0(String topic) throws Exception {
        AdminClient admin = admin();
        // 分区集合来自 describeTopics(无缓存时多一跳,量小可接受)
        int partitions = admin.describeTopics(java.util.List.of(topic))
                .allTopicNames().get(10, TimeUnit.SECONDS)
                .get(topic).partitions().size();
        Map<TopicPartition, OffsetSpec> req = new HashMap<>();
        for (int p = 0; p < partitions; p++) {
            req.put(new TopicPartition(topic, p), OffsetSpec.latest());
        }
        ListOffsetsResult res = admin.listOffsets(req);
        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> resMap =
                res.all().get(10, TimeUnit.SECONDS);
        Map<Integer, Long> out = new HashMap<>();
        for (Map.Entry<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> e : resMap.entrySet()) {
            out.put(e.getKey().partition(), e.getValue().offset());
        }
        return out;
    }

    /** 进程退出/配置变更时释放连接。 */
    public void close() {
        resetAdmin();
    }
}
