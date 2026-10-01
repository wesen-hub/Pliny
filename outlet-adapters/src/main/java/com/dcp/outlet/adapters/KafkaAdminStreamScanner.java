package com.dcp.outlet.adapters;

import com.dcp.outlet.control.StreamInfo;
import com.dcp.outlet.control.StreamRegistry;
import org.apache.kafka.clients.admin.AdminClient;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * 新表感知:扫描 Kafka,**把 {@code dcp.cdc.{schema}.{table}} 形态的 topic 登记为"可消费的流"**。
 *
 * <p>链路:建表 → TiCDC 自动建该表 topic → 本扫描器感知 → 登记(含分区数)→ 建任务时按逻辑表名取用。
 * 只交易名而不是整表结构,因此无论多少表都是一次 listTopics。
 */
public class KafkaAdminStreamScanner {

    private final String bootstrap;
    private final StreamRegistry registry;
    private final String topicPrefix;      // 默认 dcp.cdc.

    public KafkaAdminStreamScanner(String bootstrap, StreamRegistry registry, String topicPrefix) {
        this.bootstrap = bootstrap;
        this.registry = registry;
        this.topicPrefix = topicPrefix;
    }

    /** 扫描一轮;返回新登记 / 更新的流的数量。 */
    public int scanOnce() {
        Properties p = new Properties();
        p.put("bootstrap.servers", bootstrap);
        p.put("request.timeout.ms", "15000");
        p.put("default.api.timeout.ms", "20000");   // 必须 ≥ request.timeout.ms
        try (AdminClient admin = AdminClient.create(p)) {
            List<String> topics = new ArrayList<>(admin.listTopics().names().get());
            int touched = 0;
            for (String topic : topics) {
                String stream = streamOf(topic);
                if (stream == null) continue;
                int partitions = admin.describeTopics(List.of(topic)).allTopicNames().get()
                        .get(topic).partitions().size();
                // 登记时带上该流所在 Kafka 地址(扫描器连接的就是该流的集群), 供任务指令透传
                registry.register(new StreamInfo(stream, topic, bootstrap, partitions));
                touched++;
            }
            return touched;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("scan interrupted", e);
        } catch (ExecutionException e) {
            throw new IllegalStateException("scan failed", e);
        }
    }

    /** {@code dcp.cdc.target_db.rt_sync_c} → {@code rt_sync_c};不匹配则 null(默认 topic 不算流)。 */
    String streamOf(String topic) {
        if (topic == null || !topic.startsWith(topicPrefix)) return null;
        String rest = topic.substring(topicPrefix.length());
        int dot = rest.indexOf('.');
        if (dot < 0) return null;                 // dcp.cdc.target_db:默认 topic(多表混投),不登记为流
        String table = rest.substring(dot + 1);
        return table.isEmpty() || table.contains(".") ? null : table;
    }
}
