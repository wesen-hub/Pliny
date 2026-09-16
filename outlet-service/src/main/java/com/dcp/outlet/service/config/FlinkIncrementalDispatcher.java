package com.dcp.outlet.service.config;

import com.dcp.outlet.control.ExternalIncrementalDispatcher;
import com.dcp.outlet.control.TaskRecord;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.beans.factory.annotation.Value;

import java.util.Map;

/**
 * 方案 C 的 Flink 适配器:把 v2 任务的增量段交接给 Flink 作业(dcp-tagstream)。
 *
 * <p>控制面算好"每分区起点"后,由本适配器把任务 params(打标配置桥)+ 起点 组装成 tagstream job-json,
 * 再上传 jar 并提交到 Flink REST。
 *
 * <p>未配置 Flink 时只落 job-json(便于调试),不提交。装配见 OutletConfig.externalIncrementalDispatcher。
 */
public class FlinkIncrementalDispatcher implements ExternalIncrementalDispatcher {

    private static final ObjectMapper OM = new ObjectMapper();

    private final String kafkaBootstrap;
    private final String flinkBaseUrl;   // 空 = 只落 job-json 不提交
    private final String jarPath;

    public FlinkIncrementalDispatcher(@Value("${outlet.kafka.bootstrap}") String kafkaBootstrap,
                                      @Value("${outlet.flink.base-url:}") String flinkBaseUrl,
                                      @Value("${outlet.flink.jar:}") String jarPath,
                                      @Value("${outlet.flink.kafka-bootstrap:}") String flinkKafkaBootstrap) {
        // 下发给 Flink 作业的 Kafka 地址 = Flink 集群视角(容器内网/宿主可达地址);缺省回退控制面自身配置
        this.kafkaBootstrap = (flinkKafkaBootstrap == null || flinkKafkaBootstrap.isBlank())
                ? kafkaBootstrap : flinkKafkaBootstrap;
        this.flinkBaseUrl = flinkBaseUrl;
        this.jarPath = jarPath;
    }

    @Override
    public void dispatch(TaskRecord task, String topic, String stream, Map<Integer, Long> startOffsets) {
        String jobJson = buildJobJson(kafkaBootstrap, task, topic, stream, startOffsets);
        org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger("outlet-flink");
        log.info("Flink 增量段交接 task={} topic={} 起点分区={}",
                task.taskId(), topic, startOffsets == null ? 0 : startOffsets.size());
        if (flinkBaseUrl == null || flinkBaseUrl.isBlank()) {
            log.warn("未配置 outlet.flink.base-url → 只落 job-json,不提交 Flink");
            return;
        }
        try {
            String jarId = ensureJar(log);
            String jobId = submit(jarId, jobJson);
            log.info("Flink 作业已提交 task={} jobId={}", task.taskId(), jobId);
        } catch (Exception e) {
            log.error("Flink 作业提交失败 task=" + task.taskId(), e);
        }
    }

    /** 上传 dcp-tagstream.jar → 返回 jar id(镜像后端 StreamJobService.ensureJar)。 */
    private String ensureJar(org.slf4j.Logger log) throws Exception {
        if (jarPath == null || jarPath.isBlank()) {
            throw new IllegalStateException("未配置 outlet.flink.jar");
        }
        byte[] jar = java.nio.file.Files.readAllBytes(java.nio.file.Paths.get(jarPath));
        String name = "dcp-tagstream.jar";
        String boundary = "----dcp" + System.currentTimeMillis();
        byte[] head = ("--" + boundary + "\r\nContent-Disposition: form-data; name=\"jarfile\"; filename=\""
                + name + "\"\r\nContent-Type: application/java-archive\r\n\r\n").getBytes(java.nio.charset.StandardCharsets.UTF_8);
        byte[] tail = ("\r\n--" + boundary + "--\r\n").getBytes(java.nio.charset.StandardCharsets.UTF_8);
        java.io.ByteArrayOutputStream bos = new java.io.ByteArrayOutputStream();
        bos.write(head); bos.write(jar); bos.write(tail);
        var req = java.net.http.HttpRequest.newBuilder()
                .uri(java.net.URI.create(flinkBaseUrl + "/v1/jars/upload"))
                .header("Content-Type", "multipart/form-data; boundary=" + boundary)
                .POST(java.net.http.HttpRequest.BodyPublishers.ofByteArray(bos.toByteArray()))
                .build();
        var resp = java.net.http.HttpClient.newHttpClient()
                .send(req, java.net.http.HttpResponse.BodyHandlers.ofString());
        if (resp.statusCode() >= 300) {
            throw new IllegalStateException("jar 上传失败 http=" + resp.statusCode() + " " + resp.body());
        }
        JsonNode r = OM.readTree(resp.body());
        String fn = r.path("filename").asText(null);
        if (fn == null) throw new IllegalStateException("jar 上传响应无 filename: " + resp.body());
        String jarId = fn.contains("/") ? fn.substring(fn.lastIndexOf('/') + 1) : fn;
        return jarId;
    }

    /** 提交运行:POST /jars/{jarId}/run,programArgs 用 base64 传 job-json。 */
    private String submit(String jarId, String jobJson) throws Exception {
        String b64 = java.util.Base64.getEncoder().encodeToString(
                jobJson.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        String body = "{\"programArgs\":\"--job-json-b64 " + b64 + "\"}";
        var req = java.net.http.HttpRequest.newBuilder()
                .uri(java.net.URI.create(flinkBaseUrl + "/v1/jars/" + jarId + "/run"))
                .header("Content-Type", "application/json")
                .POST(java.net.http.HttpRequest.BodyPublishers.ofString(body))
                .build();
        var resp = java.net.http.HttpClient.newHttpClient()
                .send(req, java.net.http.HttpResponse.BodyHandlers.ofString());
        if (resp.statusCode() >= 300) {
            throw new IllegalStateException("Flink run 失败 http=" + resp.statusCode() + " " + resp.body());
        }
        JsonNode r = OM.readTree(resp.body());
        return r.path("jobid").asText(null);
    }

    /** 组装 tagstream job-json(与 StreamConfig 同构)。公开以便离线自检。 */
    public static String buildJobJson(String kafkaBootstrap, TaskRecord task, String topic, String stream,
                                      Map<Integer, Long> startOffsets) {
        Map<String, Object> p = task.params();
        ObjectNode cfg = OM.createObjectNode();
        // StreamConfig.taskId 是 long:优先取 params.taskId(平台数值任务 id);缺失才回退 v2 字符串 id
        Object platformId = p.get("taskId");
        if (platformId instanceof Number n) cfg.put("taskId", n.longValue());
        else cfg.put("taskId", task.taskId());
        cfg.put("taskVer", task.epoch());
        cfg.put("entityKey", str(p.get("entityKey"), "id"));

        // datasetFields:params 已回填 {name, kind}(v2TaskParams);JsonNode / Map / List 统一转
        Object dsRaw = p.get("datasetFields");
        if (dsRaw != null) cfg.set("datasetFields", OM.valueToTree(dsRaw));
        else cfg.putArray("datasetFields");

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
        Object v = p.get(key);
        if (v != null) cfg.set(key, OM.valueToTree(v));   // JsonNode / Map / List / 标量统一转
    }

    private static String str(Object v, String def) {
        return v == null ? def : String.valueOf(v);
    }
}
