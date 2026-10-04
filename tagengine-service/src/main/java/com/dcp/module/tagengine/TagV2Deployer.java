package com.dcp.module.tagengine;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

import java.net.http.HttpClient;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 打标任务 → v2 消费底座任务的**登记桥**(方案 C 的入口)。
 *
 * <p>打标任务发布后,调用 {@link #deploy(long)} 把它登记成 v2 任务(consumer_task):
 * <ol>
 *   <li>流登记:触发控制面 {@code POST /api/outlet/streams/scan}(同步,带回真实分区数)</li>
 *   <li>logic 注册:{@code tag-engine@1.0.0}(dcp-tag-task; external 轨道仅占位过六项校验,
 *       unit 轨道由底座 JarLogicLoader 从 logic-repo/tag-engine/1.0.0.jar 加载执行)</li>
 *   <li>契约:permissive 默认契约 {@code ct-tag-{dataset}}(白名单=任务字段域;外部引擎任务不拉契约视图,内容不强制)</li>
 *   <li>建任务:{@code POST /api/outlet/tasks},incrementalEngine=开关(external=Flink 增量 / unit=底座 TagLogic),params=打标配置桥</li>
 * </ol>
 *
 * <p>双轨切流开关 {@code dcp.tagengine.incremental-engine}: external(默认, Flink 流执行) | unit(dcp-tag-task 底座执行)。
 */
@Component
public class TagV2Deployer {

    private static final Logger log = LoggerFactory.getLogger(TagV2Deployer.class);
    private static final ObjectMapper OM = new ObjectMapper();
    private static final String LOGIC_ID = "tag-engine";
    private static final String LOGIC_VERSION = "1.0.0";

    private final JdbcTemplate metaJdbc;
    private final TagEngineService engine;
    private final RestClient rest;
    private final String controlUrl;
    private final String incrementalEngine;

    public TagV2Deployer(JdbcTemplate metaJdbc, TagEngineService engine,
                         RestClient.Builder loadBalancedBuilder,
                         @Value("${dcp.outlet.control-url:http://dcp-outlet-control}") String controlUrl,
                         @Value("${dcp.tagengine.incremental-engine:external}") String incrementalEngine) {
        this.metaJdbc = metaJdbc;
        this.engine = engine;
        // 中危-4: 默认无超时, 控制面不可达时发布会无限挂起; 连接 5s / 读 15s
        HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
        JdkClientHttpRequestFactory factory = new JdkClientHttpRequestFactory(client);
        factory.setReadTimeout(Duration.ofSeconds(15));
        this.rest = loadBalancedBuilder.requestFactory(factory).build();
        this.controlUrl = controlUrl.endsWith("/") ? controlUrl.substring(0, controlUrl.length() - 1) : controlUrl;
        if (!"external".equals(incrementalEngine) && !"unit".equals(incrementalEngine)) {
            throw new IllegalArgumentException("dcp.tagengine.incremental-engine 须为 external|unit, 实际: " + incrementalEngine);
        }
        this.incrementalEngine = incrementalEngine;
    }

    /** 把打标任务登记成 v2 任务(幂等: 已登记 → 跳过)。返回 {v2TaskId, contractRef, created, skipped, response}。 */
    public Map<String, Object> deploy(long taskId) {
        String v2TaskId = taskIdOf(taskId);
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("v2TaskId", v2TaskId);
        if (exists(taskId)) {
            out.put("created", false);
            out.put("skipped", true);
            return out;
        }
        Map<String, Object> params = engine.v2TaskParams(taskId);
        String dataset = String.valueOf(params.get("dataset"));
        String stream = dataset;
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
        out.put("contractRef", contractRef);
        out.put("created", true);
        out.put("response", resp);
        return out;
    }

    public static String taskIdOf(long taskId) {
        return "tag-task-" + taskId;
    }

    /** v2 任务是否已登记(直接查元库 consumer_task, 不依赖控制面在线)。 */
    public boolean exists(long taskId) {
        try {
            Long n = metaJdbc.queryForObject("SELECT COUNT(*) FROM consumer_task WHERE task_id = ?",
                    Long.class, taskIdOf(taskId));
            return n != null && n > 0;
        } catch (Exception e) {
            return false;
        }
    }

    /** 停 v2 底座任务(保留登记; 删除/停用打标任务后由对账 tick 或调用方触发)。不存在/不可达 → 静默。 */
    public void stopTask(long taskId) {
        if (!exists(taskId)) {
            return;
        }
        try {
            httpPost(controlUrl + "/api/outlet/tasks/" + taskIdOf(taskId) + "/actions/stop", "");
            log.info("[tag-v2] v2 底座任务已停 {} (打标任务删除/停用)", taskIdOf(taskId));
        } catch (Exception e) {
            log.warn("[tag-v2] 停任务失败 task={}: {}", taskId, e.getMessage());
        }
    }

    private String taskJson(String v2TaskId, String dataset, String stream, String contractRef,
                            Map<String, Object> params) throws IllegalStateException {
        try {
            ObjectNode req = OM.createObjectNode();
            req.put("taskId", v2TaskId);
            req.put("name", "打标:" + dataset);
            req.put("consumerType", "tag");
            req.put("namespace", "tag");        // 底座多 ns 拆分: 打标任务归 tag ns
            req.put("owner", "tag-platform");
            req.putArray("streams").add(stream);
            req.put("logicId", LOGIC_ID);
            req.put("logicVersion", LOGIC_VERSION);
            req.put("instances", 1);
            req.put("contractRef", contractRef);
            req.put("startPolicy", "snapshot");
            req.put("desiredState", "running");
            req.put("incrementalEngine", incrementalEngine);   // external=Flink 增量 / unit=底座 TagLogic(dcp-tag-task)
            req.set("params", OM.valueToTree(params));
            return OM.writeValueAsString(req);
        } catch (Exception e) {
            throw new IllegalStateException("构造 v2 建任务请求失败", e);
        }
    }

    private String httpPost(String url, String body) {
        try {
            return rest.post().uri(url)
                    .header("Content-Type", "application/json")
                    .body(body == null ? "" : body)
                    .retrieve().body(String.class);
        } catch (Exception e) {
            throw new IllegalStateException("HTTP 失败:" + url, e);
        }
    }
}
