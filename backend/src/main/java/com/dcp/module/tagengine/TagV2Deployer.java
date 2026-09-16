package com.dcp.module.tagengine;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 打标任务 → v2 消费底座任务的**登记桥**(方案 C 的入口)。
 *
 * <p>打标任务发布后,调用 {@link #deploy(long)} 把它登记成 v2 任务(consumer_task):
 * <ol>
 *   <li>流登记:触发控制面 {@code POST /api/outlet/streams/scan}(同步,带回真实分区数)</li>
 *   <li>logic 注册:{@code tag-engine@1.0.0} 占位注册(打标不走 v2 逻辑加载 —— 增量=Flink、批=Spark,只占位过六项校验)</li>
 *   <li>契约:permissive 默认契约 {@code ct-tag-{dataset}}(白名单=任务字段域;外部引擎任务不拉契约视图,内容不强制)</li>
 *   <li>建任务:{@code POST /api/outlet/tasks},incrementalEngine=external,params=打标配置桥</li>
 * </ol>
 */
@Component
public class TagV2Deployer {

    private static final Logger log = LoggerFactory.getLogger(TagV2Deployer.class);
    private static final ObjectMapper OM = new ObjectMapper();
    private static final String LOGIC_ID = "tag-engine";
    private static final String LOGIC_VERSION = "1.0.0";

    private final JdbcTemplate metaJdbc;
    private final TagEngineService engine;
    private final String controlUrl;

    public TagV2Deployer(JdbcTemplate metaJdbc, TagEngineService engine,
                         @Value("${dcp.outlet.control-url:http://127.0.0.1:8095}") String controlUrl) {
        this.metaJdbc = metaJdbc;
        this.engine = engine;
        this.controlUrl = controlUrl.endsWith("/") ? controlUrl.substring(0, controlUrl.length() - 1) : controlUrl;
    }

    /** 把打标任务登记成 v2 任务。返回 {v2TaskId, contractRef, response}。 */
    public Map<String, Object> deploy(long taskId) {
        Map<String, Object> params = engine.v2TaskParams(taskId);
        String dataset = String.valueOf(params.get("dataset"));
        String stream = dataset;
        String v2TaskId = "tag-task-" + taskId;
        String contractRef = "ct-tag-" + stream;

        // ① 流登记:控制面扫描(同步注册 CDC topic,带回真实分区数)
        try {
            httpPost(controlUrl + "/api/outlet/streams/scan", "");
        } catch (Exception e) {
            log.warn("[tag-v2] 流扫描失败(继续尝试建任务,建任务时会暴露流未登记): {}", e.getMessage());
        }

        // ② logic 占位注册
        metaJdbc.update("INSERT IGNORE INTO consumer_logic(logic_id, version, description) "
                        + "VALUES(?, ?, '打标引擎(增量=Flink/批=Spark,v2 不加载该 logic)')",
                LOGIC_ID, LOGIC_VERSION);

        // ②.5 平台消费方账号(批段读快照的平台身份:ChangeOutletService.context(consumerId) 要求账号存在且契约 active)
        metaJdbc.update("INSERT IGNORE INTO consumer(consumer_id, name, consumer_type, owner, status, channel) "
                        + "VALUES('tag-platform', '打标平台桥', 'tag', 'tag-platform', 'active', 'internal')");

        // ③ permissive 契约(白名单 = 任务字段域;外部引擎任务不拉契约视图,内容不强制)
        // consumer_id='tag-platform' 与 ②.5 的账号一致 —— 批段读快照的身份 = 契约 consumer_id(批/增量一致性)
        try {
            ObjectNode whitelist = OM.createObjectNode();
            ArrayNode cols = whitelist.putArray(stream);
            Object df = params.get("datasetFields");
            if (df instanceof JsonNode n && n.isArray()) {
                for (JsonNode f : n) {
                    String nm = f.isTextual() ? f.asText() : f.path("name").asText("");
                    if (!nm.isBlank()) cols.add(nm);
                }
            }
            metaJdbc.update("INSERT IGNORE INTO contract(contract_id, consumer_id, streams, column_whitelist, "
                            + "row_condition, max_level, mask_rules, channel, start_policy, status) "
                            + "VALUES(?, 'tag-platform', ?, ?, NULL, 3, '{}', 'internal', 'snapshot', 'active')",
                    contractRef, "[\"" + stream + "\"]", OM.writeValueAsString(whitelist));
        } catch (Exception e) {
            log.warn("[tag-v2] 契约预置失败(建任务时会暴露): {}", e.getMessage());
        }

        // ④ 建 v2 任务(external 引擎,批=Spark/容器,增量=Flink)
        String body = taskJson(v2TaskId, dataset, stream, contractRef, params);
        String resp = httpPost(controlUrl + "/api/outlet/tasks", body);
        log.info("[tag-v2] 打标任务 {} 登记为 v2 任务 {} (contract={})", taskId, v2TaskId, contractRef);
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("v2TaskId", v2TaskId);
        out.put("contractRef", contractRef);
        out.put("response", resp);
        return out;
    }

    private String taskJson(String v2TaskId, String dataset, String stream, String contractRef,
                            Map<String, Object> params) throws IllegalStateException {
        try {
            ObjectNode req = OM.createObjectNode();
            req.put("taskId", v2TaskId);
            req.put("name", "打标:" + dataset);
            req.put("consumerType", "tag");
            req.put("owner", "tag-platform");
            req.putArray("streams").add(stream);
            req.put("logicId", LOGIC_ID);
            req.put("logicVersion", LOGIC_VERSION);
            req.put("instances", 1);
            req.put("contractRef", contractRef);
            req.put("startPolicy", "snapshot");
            req.put("desiredState", "running");
            req.put("incrementalEngine", "external");
            req.set("params", OM.valueToTree(params));
            return OM.writeValueAsString(req);
        } catch (Exception e) {
            throw new IllegalStateException("构造 v2 建任务请求失败", e);
        }
    }

    private static String httpPost(String url, String body) {
        try {
            HttpRequest req = HttpRequest.newBuilder(URI.create(url))
                    .timeout(Duration.ofSeconds(15))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(body == null ? "" : body))
                    .build();
            HttpResponse<String> resp = HttpClient.newHttpClient().send(req, HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() / 100 != 2) {
                throw new IllegalStateException("HTTP " + resp.statusCode() + " " + resp.body());
            }
            return resp.body();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("HTTP 中断:" + url, e);
        } catch (Exception e) {
            throw new IllegalStateException("HTTP 失败:" + url, e);
        }
    }
}
