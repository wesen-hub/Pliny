package com.dcp.outlet.service.config;

import com.dcp.outlet.control.ExternalIncrementalDispatcher;
import com.dcp.outlet.control.TaskRecord;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * 方案 C 的 Flink 适配器:把 v2 任务的增量段交接给 Flink 作业(dcp-tagstream)。
 *
 * <p>控制面算好"每分区起点"后,由本适配器把任务 params(打标配置桥)+ 起点 组装成 tagstream job-json,
 * 再上传 jar 并提交到 Flink REST。
 *
 * <p>未配置 Flink 时只落 job-json(便于调试),不提交。
 */
@Component
public class FlinkIncrementalDispatcher implements ExternalIncrementalDispatcher {

    private static final ObjectMapper OM = new ObjectMapper();

    private final String kafkaBootstrap;
    private final String flinkBaseUrl;   // 空 = 只落 job-json 不提交
    private final String jarPath;

    public FlinkIncrementalDispatcher(@Value("${outlet.kafka.bootstrap}") String kafkaBootstrap,
                                      @Value("${outlet.flink.base-url:}") String flinkBaseUrl,
                                      @Value("${outlet.flink.jar:}") String jarPath) {
        this.kafkaBootstrap = kafkaBootstrap;
        this.flinkBaseUrl = flinkBaseUrl;
        this.jarPath = jarPath;
    }

    @Override
    public void dispatch(TaskRecord task, String topic, String stream, Map<Integer, Long> startOffsets) {
        String jobJson = buildJobJson(kafkaBootstrap, task, topic, stream, startOffsets);
        org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger("outlet-flink");
        log.info("Flink 增量段交接 task={} topic={} 起点分区={} jobJson={}",
                task.taskId(), topic, startOffsets == null ? 0 : startOffsets.size(), jobJson);
        // TODO(提交):上传 jar + 提交到 Flink REST,镜像后端 StreamJobService(ensureJar + submit)。
        // 未配 outlet.flink.base-url 时只落 job-json。
        if (flinkBaseUrl == null || flinkBaseUrl.isBlank()) {
            log.warn("未配置 outlet.flink.base-url → 只落 job-json,不提交 Flink");
        }
    }

    /** 组装 tagstream job-json(与 StreamConfig 同构)。公开以便离线自检。 */
    public static String buildJobJson(String kafkaBootstrap, TaskRecord task, String topic, String stream,
                                      Map<Integer, Long> startOffsets) {
        Map<String, Object> p = task.params();
        ObjectNode cfg = OM.createObjectNode();
        cfg.put("taskId", task.taskId());
        cfg.put("taskVer", task.epoch());
        cfg.put("entityKey", str(p.get("entityKey"), "id"));

        // datasetFields:params 里是字符串数组 → 包成 {name, kind}(kind 暂 S,数值类型需平台字段探测回填)
        ArrayNode ds = cfg.putArray("datasetFields");
        Object dsRaw = p.get("datasetFields");
        if (dsRaw instanceof JsonNode n && n.isArray()) {
            for (JsonNode f : n) {
                if (f.isTextual()) ds.addObject().put("name", f.asText()).put("kind", "S");
                else ds.add(f);
            }
        }

        ObjectNode kafka = cfg.putObject("kafka");
        kafka.put("bootstrapServers", kafkaBootstrap);
        kafka.put("topic", topic);
        kafka.put("groupId", "dcp-tagstream-t" + task.taskId());
        ObjectNode offs = kafka.putObject("partitionStartOffsets");
        if (startOffsets != null) startOffsets.forEach((k, v) -> offs.put(String.valueOf(k), v));

        passthrough(cfg, p, "bindings");
        passthrough(cfg, p, "rules");
        passthrough(cfg, p, "derived");
        if (p.get("tagIds") instanceof Map<?, ?> t) cfg.set("tagIds", OM.valueToTree(t));

        try {
            return OM.writeValueAsString(cfg);
        } catch (Exception e) {
            throw new IllegalStateException("job-json 序列化失败", e);
        }
    }

    private static void passthrough(ObjectNode cfg, Map<String, Object> p, String key) {
        if (p.get(key) instanceof JsonNode n) cfg.set(key, n);
    }

    private static String str(Object v, String def) {
        return v == null ? def : String.valueOf(v);
    }
}
