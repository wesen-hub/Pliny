package com.dcp.tagstream;

import com.dcp.tagstream.config.StreamConfig;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * 打标流执行入口(P1; 设计 §4 流态): 消费 Kafka 事件 → Drools 求值(事实 schema=任务字段域)
 * → 命中双写: Redis 在线层 + MySQL tag_fact_view 投影。
 *
 * <p>用法: flink run -c com.dcp.tagstream.StreamTagJob dcp-tagstream.jar --job-json <file>
 * 退出: 常驻流作业; 依赖 flink-connector-kafka(已在 shade)与集群 kafka client(flink 自带)。
 */
public final class StreamTagJob {

    private static final ObjectMapper OM = new ObjectMapper();

    private StreamTagJob() {
    }

    public static void main(String[] args) throws Exception {
        String jobJson = null;
        String jobJsonB64 = null;
        for (int i = 0; i < args.length; i++) {
            if ("--job-json".equals(args[i]) && i + 1 < args.length) jobJson = args[++i];
            if ("--job-json-b64".equals(args[i]) && i + 1 < args.length) jobJsonB64 = args[++i];
        }
        StreamConfig cfg;
        try {
            if (jobJsonB64 != null) {
                cfg = OM.readValue(java.util.Base64.getDecoder().decode(jobJsonB64), StreamConfig.class);
            } else if (jobJson != null) {
                cfg = OM.readValue(Files.readString(Paths.get(jobJson), StandardCharsets.UTF_8), StreamConfig.class);
            } else {
                System.err.println("缺少 --job-json <file> 或 --job-json-b64 <base64>");
                System.exit(2);
                return;
            }
        } catch (Exception e) {
            System.err.println("job-json 解析失败: " + e.getMessage());
            System.exit(2);
            return;
        }
        if (cfg.kafka.topic == null || cfg.kafka.topic.isBlank()) {
            System.err.println("kafka.topic 必填");
            System.exit(2);
            return;
        }
        System.out.println("[tagstream] 启动 task=" + cfg.taskId + " v" + cfg.taskVer
                + " entityKey=" + cfg.entityKey + " topic=" + cfg.kafka.topic
                + " rules=" + cfg.rules.size() + " online=" + cfg.online.enabled
                + " factView=" + cfg.factView.enabled);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);   // dev 单 TM 4 slot; 事件量小, 保序

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(cfg.kafka.bootstrapServers)
                .setTopics(cfg.kafka.topic)
                .setGroupId(cfg.kafka.groupId == null || cfg.kafka.groupId.isBlank()
                        ? "dcp-tagstream-t" + cfg.taskId : cfg.kafka.groupId)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<StreamEvaluator.Hit> hits = env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "kafka-" + cfg.kafka.topic)
                .flatMap(new StreamEvaluator(cfg))
                .name("drools-eval-task" + cfg.taskId);

        if (cfg.online.enabled) {
            hits.addSink(new StreamSinks.RedisSink(cfg.online)).name("redis-online");
        }
        if (cfg.factView.enabled) {
            hits.addSink(new StreamSinks.ViewSink(cfg)).name("mysql-fact-view");
        }

        env.execute("dcp-tagstream-task" + cfg.taskId + "-v" + cfg.taskVer);
    }
}
