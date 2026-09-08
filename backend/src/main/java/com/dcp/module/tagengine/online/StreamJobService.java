package com.dcp.module.tagengine.online;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClient;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 流作业平台化管理(P1.1 落地; 设计 §4 流态 + §9 P1):
 * published 的 stream 任务 → 生成 dcp-tagstream job-json → 上传 jar 到 Flink REST → run/cancel/status。
 *
 * <p>API(挂在 /api/tagengine/stream):
 *   POST /{taskId}/start    启动流作业(上传 jar(如需)+ flink run; 记录 jobId/状态)
 *   POST /{taskId}/stop     停止(flink cancel)
 *   GET  /{taskId}/status   Flink 作业状态(任务行 stream_status + job 实时态)
 *
 * <p>Flink REST: GET/POST http://{flinkHost}:{port}/v1/... (jars upload / jars/{id}/run / jobs/{id} cancel)
 * jar 来源: 宿主挂载 DCP_TAGSTREAM_JAR(compose volume: dcp-tagstream/target/dcp-tagstream.jar → 容器路径)
 */
@Slf4j
@Service
public class StreamJobService {

    private static final ObjectMapper OM = new ObjectMapper();

    private final JdbcTemplate metaJdbc;
    private final RestClient rest;

    @Value("${dcp.tagstream.flink-url:http://flink-jobmanager:8081}")
    private String flinkUrl;
    @Value("${dcp.tagstream.jar:/opt/dcp-tagstream.jar}")
    private String jarPath;
    @Value("${dcp.tagstream.kafka:dcp.in.}")
    private String kafkaPrefix;   // 事件 topic 前缀(与 ingest 一致: dcp.in.{dataset})
    @Value("${dcp.tagstream.jdbc-url:jdbc:mysql://mysql:3306/dcp?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Asia/Shanghai}")
    private String jdbcUrl;
    @Value("${dcp.tagstream.jdbc-user:root}")
    private String jdbcUser;
    @Value("${dcp.tagstream.jdbc-password:root123}")
    private String jdbcPassword;
    @Value("${dcp.tagstream.redis-host:dcp-redis}")
    private String redisHost;
    @Value("${dcp.tagstream.redis-port:6379}")
    private int redisPort;

    public StreamJobService(JdbcTemplate metaJdbc) {
        this.metaJdbc = metaJdbc;
        RestClient.Builder b = RestClient.builder();
        b.requestFactory(new JdkClientHttpRequestFactory());
        this.rest = b.build();
    }

    /* ═════════ 启动 / 停止 / 状态 ═════════ */

    public Map<String, Object> start(long taskId) {
        Map<String, Object> task = requireStreamPublished(taskId);
        // 已在跑 → 幂等返回
        String cur = str(task.get("stream_status"));
        String jobId = str(task.get("stream_job_id"));
        if ("RUNNING".equals(cur) && !jobId.isEmpty()) {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("taskId", taskId);
            m.put("jobId", jobId);
            m.put("status", "RUNNING");
            m.put("message", "作业已在运行");
            return m;
        }
        try {
            ObjectNode cfg = buildJobJson(task);
            String jarId = ensureJar();
            // flink run(上传后 jarId → POST /jars/{id}/run, programArgs 含 --job-json 内联(经 REST 不便传文件, 用 base64 入参)
            // 简化: job-json 写后端 runs 目录, programArgs 传文件路径需 Flink 可读 —— 改用 args 传 base64(作业端解析)
            String b64 = java.util.Base64.getEncoder().encodeToString(
                    OM.writeValueAsBytes(cfg));
            String body = rest.post().uri(flinkUrl + "/v1/jars/" + jarId + "/run")
                    .header("Content-Type", "application/json")
                    .body("{\"programArgs\":\"--job-json-b64 " + b64 + "\"}")
                    .retrieve().body(String.class);
            JsonNode resp = OM.readTree(body);
            String newJobId = resp.path("jobid").asText(null);
            if (newJobId == null || newJobId.isEmpty()) {
                throw new IllegalStateException("Flink run 未返回 jobid: " + body);
            }
            metaJdbc.update("UPDATE tag_task SET stream_job_id=?, stream_status='RUNNING' WHERE id=?", newJobId, taskId);
            log.info("[stream] task#{} 流作业启动 job={}", taskId, newJobId);
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("taskId", taskId);
            m.put("jobId", newJobId);
            m.put("status", "RUNNING");
            return m;
        } catch (Exception e) {
            metaJdbc.update("UPDATE tag_task SET stream_status='FAILED' WHERE id=?", taskId);
            throw new IllegalArgumentException("流作业启动失败: " + e.getMessage(), e);
        }
    }

    public Map<String, Object> stop(long taskId) {
        Map<String, Object> task = requireStreamPublished(taskId);
        String jobId = str(task.get("stream_job_id"));
        if (jobId.isEmpty()) {
            throw new IllegalArgumentException("任务无流作业记录(未启动过)");
        }
        try {
            rest.patch().uri(flinkUrl + "/v1/jobs/" + jobId).retrieve().toBodilessEntity();
            metaJdbc.update("UPDATE tag_task SET stream_status='CANCELED' WHERE id=?", taskId);
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("taskId", taskId);
            m.put("jobId", jobId);
            m.put("status", "CANCELED");
            return m;
        } catch (Exception e) {
            throw new IllegalArgumentException("流作业停止失败: " + e.getMessage(), e);
        }
    }

    public Map<String, Object> status(long taskId) {
        Map<String, Object> task = requireStreamPublished(taskId);
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("taskId", taskId);
        m.put("streamStatus", task.get("stream_status"));
        m.put("jobId", task.get("stream_job_id"));
        String jobId = str(task.get("stream_job_id"));
        if (!jobId.isEmpty()) {
            try {
                String body = rest.get().uri(flinkUrl + "/v1/jobs/" + jobId).retrieve().body(String.class);
                JsonNode j = OM.readTree(body);
                m.put("jobState", j.path("state").asText());
                m.put("flinkName", j.path("name").asText());
            } catch (Exception e) {
                m.put("jobState", "UNREACHABLE(" + e.getMessage() + ")");
            }
        }
        return m;
    }

    /* ═════════ 内部 ═════════ */

    private Map<String, Object> requireStreamPublished(long taskId) {
        List<Map<String, Object>> rows = metaJdbc.queryForList(
                "SELECT id,name,mode,dataset,entity_key,ver,status,snapshot_json,dataset_fields_json,stream_job_id,stream_status"
                        + " FROM tag_task WHERE id=?", taskId);
        if (rows.isEmpty()) throw new IllegalArgumentException("任务不存在 id=" + taskId);
        Map<String, Object> t = rows.get(0);
        if (!"stream".equals(t.get("mode"))) throw new IllegalArgumentException("非流任务(需 mode=stream): " + t.get("name"));
        if (!"published".equals(t.get("status"))) throw new IllegalArgumentException("任务未发布, 请先 publish");
        return t;
    }

    /** 生成 dcp-tagstream job-json(与手工 job 同构; 事件源 topic=dcp.in.{dataset}) */
    private ObjectNode buildJobJson(Map<String, Object> task) throws Exception {
        JsonNode snap = OM.readTree((String) task.get("snapshot_json"));
        ObjectNode cfg = OM.createObjectNode();
        cfg.put("taskId", ((Number) task.get("id")).longValue());
        cfg.put("taskVer", ((Number) task.get("ver")).intValue());
        cfg.put("entityKey", str(task.get("entity_key")));
        String dataset = str(task.get("dataset"));
        // datasetFields(任务行数组)
        ArrayNode ds = cfg.putArray("datasetFields");
        JsonNode rawDs = OM.readTree((String) task.get("dataset_fields_json"));
        if (rawDs != null && rawDs.isArray()) {
            for (JsonNode f : rawDs) ds.add(f.isTextual() ? f.asText() : f.path("name").asText());
        }
        ObjectNode kafka = cfg.putObject("kafka");
        kafka.put("bootstrapServers", "kafka:9092");
        kafka.put("topic", kafkaPrefix + dataset);
        kafka.put("groupId", "dcp-tagstream-t" + task.get("id"));
        ObjectNode online = cfg.putObject("online");
        online.put("enabled", true);
        online.put("redisHost", redisHost);
        online.put("redisPort", redisPort);
        online.put("ttlSeconds", 3600);
        ObjectNode fv = cfg.putObject("factView");
        fv.put("enabled", true);
        fv.put("jdbcUrl", jdbcUrl);
        fv.put("user", jdbcUser);
        fv.put("password", jdbcPassword);
        fv.put("table", "tag_fact_view");
        // rules: 快照 assets 闭包展开为纯 rule 列表(流作业 v1 平铺; flow 引用的规则递归收录)
        ArrayNode rulesArr = cfg.putArray("rules");
        java.util.Set<Long> seen = new java.util.LinkedHashSet<>();
        java.util.List<Long> order = new java.util.ArrayList<>();
        java.util.Map<Long, JsonNode> byId = new java.util.HashMap<>();
        for (JsonNode a : snap.path("assets")) byId.put(a.path("id").asLong(), a);
        for (JsonNode b : snap.path("bindings")) expandRule(b.path("id").asLong(), byId, order, seen);
        for (Long id : order) {
            JsonNode a = byId.get(id);
            if (a != null && "rule".equals(a.path("kind").asText())) rulesArr.add(a);
        }
        return cfg;
    }

    private void expandRule(long id, java.util.Map<Long, JsonNode> byId, java.util.List<Long> order,
                            java.util.Set<Long> seen) {
        if (!seen.add(id)) return;
        order.add(id);
        JsonNode a = byId.get(id);
        if (a != null && "flow".equals(a.path("kind").asText())) {
            for (JsonNode st : a.path("def").path("steps")) {
                expandRule(st.path("refId").asLong(), byId, order, seen);
            }
        }
    }

    /** 上传 jar(Flink web-upload 目录重启会清空, 同名复用不可靠 → 每次 start 重新上传, 返回 jarId) */
    private String ensureJar() throws Exception {
        byte[] jar = Files.readAllBytes(Paths.get(jarPath));
        String name = "dcp-tagstream.jar";
        String boundary = "----dcp" + System.currentTimeMillis();
        byte[] head = ("--" + boundary + "\r\nContent-Disposition: form-data; name=\"jarfile\"; filename=\"" + name + "\"\r\n"
                + "Content-Type: application/java-archive\r\n\r\n").getBytes(StandardCharsets.UTF_8);
        byte[] tail = ("\r\n--" + boundary + "--\r\n").getBytes(StandardCharsets.UTF_8);
        java.io.ByteArrayOutputStream bos = new java.io.ByteArrayOutputStream();
        bos.write(head);
        bos.write(jar);
        bos.write(tail);
        var req = java.net.http.HttpRequest.newBuilder()
                .uri(java.net.URI.create(flinkUrl + "/v1/jars/upload"))
                .header("Content-Type", "multipart/form-data; boundary=" + boundary)
                .POST(java.net.http.HttpRequest.BodyPublishers.ofByteArray(bos.toByteArray()))
                .build();
        java.net.http.HttpClient hc = java.net.http.HttpClient.newHttpClient();
        java.net.http.HttpResponse<String> resp = hc.send(req, java.net.http.HttpResponse.BodyHandlers.ofString());
        if (resp.statusCode() >= 300) {
            throw new IllegalStateException("jar 上传失败 http=" + resp.statusCode() + " " + resp.body());
        }
        JsonNode r = OM.readTree(resp.body());
        String filename = r.path("filename").asText(null);
        if (filename == null) throw new IllegalStateException("jar 上传响应无 filename: " + resp.body());
        // upload 返回完整 web 路径(如 /tmp/flink-web-xxx/flink-web-upload/{id}_dcp-tagstream.jar); run 需用 basename 作 jar id
        String jarId = filename.contains("/") ? filename.substring(filename.lastIndexOf('/') + 1) : filename;
        log.info("[stream] jar 已上传: {}", jarId);
        return jarId;
    }

    private static String str(Object o) {
        return o == null ? "" : String.valueOf(o);
    }
}
