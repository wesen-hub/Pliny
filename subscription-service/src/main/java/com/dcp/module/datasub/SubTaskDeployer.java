package com.dcp.module.datasub;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 订阅推送底座任务**登记桥**(P3, 维度⑤ 订阅推送): 把生效 push 契约登记成 v2 底座任务
 * (consumer_task), 执行体 = dcp-outlet-rt 装载 dcp-sub-push jar(sub-push@1.0.0)。
 *
 * <p>档位语义(与讨论定案一致):
 * <ul>
 *   <li>实时/流式 → 底座化(本类登记, desiredState=stopped 只登记不激活);</li>
 *   <li>定时 → 保留 {@link PushScheduler} 轮询投递;</li>
 *   <li>manual/批量 → 保留手动 deliver(批量=全量语义)。</li>
 * </ul>
 *
 * <p>登记链路(与 TagV2Deployer/LakeTaskDeployer/IngestTaskDeployer 同模式):
 * <ol>
 *   <li>流登记:logical 契约触发控制面 {@code POST /api/outlet/streams/scan}(扫描器
 *       发现 TiCDC topic {@code dcp.cdc.{库}.{表}} 登记为流 {@code {表}}, 与湖增量共享
 *       出口栈 TiCDC 流); tag 契约手工登记 {@code dcp-tag-events} →
 *       {@code dcp.tag.events}(打标事件流, dcp-tag-task 发射, 带出口栈 kafka_bootstrap);</li>
 *   <li>logic 注册:{@code sub-push@1.0.0}(unit 轨道由底座 JarLogicLoader 从
 *       logic-repo/sub-push 加载执行);</li>
 *   <li>平台消费方账号 {@code sub-push-platform} + permissive 契约 {@code ct-sub-push-{id}}
 *       (白名单 = ["*"] 全放行 / 打标事件键, 全透传);</li>
 *   <li>建任务:{@code POST /api/outlet/tasks},startPolicy=latest(首轮全量衔接
 *       seqFloor/tsFloor 预留待定, 流式只追新),incrementalEngine=unit,
 *       params = 契约快照 + 目标(webhook url+secret / kafka topic+bootstrap) + 元库连接
 *       (tag 契约 exclude 剔除回查 tag_fact_view 用)。</li>
 * </ol>
 *
 * <p><b>只登记不激活</b>(部署方案阶段再统一激活执行栈): 建任务带 desiredState=stopped,
 * 内核对此不调度; 任务记录在册, 状态 STOPPED。
 */
@Slf4j
@Component
public class SubTaskDeployer {

    private static final ObjectMapper OM = new ObjectMapper();

    public static final String LOGIC_ID = "sub-push";
    public static final String LOGIC_VERSION = "1.0.0";
    public static final String CONSUMER_ID = "sub-push-platform";
    /** 打标事件流登记名(topic dcp.tag.events; dcp-tag-task tagEvents 段发射, 与底座运行时同栈) */
    public static final String TAG_EVENT_STREAM = "dcp-tag-events";
    public static final String TAG_EVENT_TOPIC = "dcp.tag.events";

    private final JdbcTemplate metaJdbc;
    private final String controlUrl;
    /** 底座容器内可达的元库地址(tag 契约 exclude 剔除回查 tag_fact_view) */
    private final String metaJdbcUrl;
    private final String metaUser;
    private final String metaPassword;
    /** kafka 推送通道地址(与现 deliver 同源 KAFKA_BOOTSTRAP_SERVERS: 平台栈) */
    private final String pushKafkaBootstrap;
    /** 打标事件流所在 Kafka 地址(出口栈; 与 tagengine-service dcp.tagstream.tag-events-kafka-bootstrap 同栈) */
    private final String tagEventsKafkaBootstrap;

    public SubTaskDeployer(JdbcTemplate metaJdbc,
                           @Value("${dcp.subscription.outlet-control-url:http://127.0.0.1:8095}") String controlUrl,
                           @Value("${dcp.subscription.meta-jdbc-url:}") String metaJdbcUrl,
                           @Value("${dcp.subscription.meta-user:root}") String metaUser,
                           @Value("${dcp.subscription.meta-password:}") String metaPassword,
                           @Value("${dcp.subscription.push-kafka-bootstrap:dcp-kafka:9092}") String pushKafkaBootstrap,
                           @Value("${dcp.subscription.tag-events-kafka-bootstrap:outlet-kafka:19092}") String tagEventsKafkaBootstrap) {
        this.metaJdbc = metaJdbc;
        this.controlUrl = controlUrl.endsWith("/") ? controlUrl.substring(0, controlUrl.length() - 1) : controlUrl;
        this.metaJdbcUrl = metaJdbcUrl;
        this.metaUser = metaUser;
        this.metaPassword = metaPassword;
        this.pushKafkaBootstrap = pushKafkaBootstrap;
        this.tagEventsKafkaBootstrap = tagEventsKafkaBootstrap;
    }

    // ────────────────────────── 命名约定 ──────────────────────────

    public static String taskIdOf(long contractId) {
        return "sub-push-" + contractId;
    }

    public static String contractRefOf(long contractId) {
        return "ct-sub-push-" + contractId;
    }

    /** TiCDC 流登记名(扫描器从 topic {@code dcp.cdc.{库}.{表}} 派生的流名, 即 TiDB 目标表名) */
    public static String streamOf(String table) {
        return table;
    }

    /** 档位判定: 实时/流式 → 底座化; 定时/manual/批量 → 保留原交付链路(轮询/手动) */
    public static boolean baseRelevant(String freq) {
        return "实时".equals(freq) || "流式".equals(freq);
    }

    // ────────────────────────── 登记(幂等) ──────────────────────────

    /**
     * 登记契约的订阅推送底座任务(幂等: 任务已存在 → 跳过, 不改变现有状态)。
     * 失败不抛(登记属机会型动作, 调用方按 error 字段决策)。
     *
     * @return {taskId, contractRef, created, skipped/status/error}
     */
    public Map<String, Object> deploy(long contractId) {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("taskId", taskIdOf(contractId));
        out.put("contractRef", contractRefOf(contractId));
        Map<String, Object> c = loadContract(contractId);
        if (c == null || !"push".equals(s(c.get("push"))) || !"active".equals(s(c.get("status")))) {
            out.put("created", false);
            out.put("error", "契约不存在或非生效 push 契约");
            return out;
        }
        String freq = s(c.get("freq"));
        if (!baseRelevant(freq)) {
            out.put("created", false);
            out.put("skipped", true);   // 定时档走 PushScheduler 轮询; manual/批量走手动 deliver
            return out;
        }
        if (exists(contractId)) {
            out.put("created", false);
            return out;
        }
        try {
            JsonNode view = (JsonNode) c.get("view");
            boolean isLogical = "logical".equals(s(c.get("view_type")));

            // ① 流登记:logical 走控制面扫描(TiCDC topic dcp.cdc.{库}.{表} 自动登记为流 {表});
            //    tag 事件流不在扫描范围, 手工登记(带出口栈 kafka_bootstrap)
            List<String> streams = new ArrayList<>();
            if (isLogical) {
                for (JsonNode tn : view.path("tables")) {
                    String t = tn.asText("");
                    if (t.isBlank()) continue;
                    streams.add(streamOf(t));
                }
                if (!streams.isEmpty()) {
                    try {
                        httpPost(controlUrl + "/api/outlet/streams/scan", "");
                    } catch (Exception e) {
                        log.warn("[sub-task] 流扫描失败(继续尝试建任务,建任务时会暴露流未登记) contractId={}: {}",
                                contractId, e.getMessage());
                    }
                }
            } else {
                registerTagEventStream();
                streams.add(TAG_EVENT_STREAM);
            }
            if (streams.isEmpty()) {
                out.put("created", false);
                out.put("error", "视图未引用任何表");
                return out;
            }

            // ② logic 注册
            metaJdbc.update("INSERT IGNORE INTO consumer_logic(logic_id, version, description, status) "
                            + "VALUES(?, ?, '订阅推送底座任务(出口栈 TiCDC 流 dcp.cdc.* 行数据/打标事件流 → 圈选过滤 → webhook/kafka 推送)', 'active')",
                    LOGIC_ID, LOGIC_VERSION);

            // ③ 平台消费方账号
            metaJdbc.update("INSERT IGNORE INTO consumer(consumer_id, name, consumer_type, owner, status, channel) "
                            + "VALUES(?, '订阅推送平台桥', 'subscribe', ?, 'active', 'internal')",
                    CONSUMER_ID, CONSUMER_ID);

            // ④ permissive 契约(logical: 白名单 ["*"] 全放行 —— 订阅推送要整行(业务列 + 写者列 _s 前缀/seq)透传;
            //    tag: 白名单 = 打标事件键, 全透传; 行条件/脱敏不设)
            try {
                ObjectNode whitelist = OM.createObjectNode();
                for (String stream : streams) {
                    ArrayNode cols = whitelist.putArray(stream);
                    if (isLogical) {
                        cols.add("*");
                    } else {
                        for (String k : List.of("taskId", "entity", "tagPath", "tagDictId", "tagValue", "ts")) {
                            cols.add(k);
                        }
                    }
                }
                metaJdbc.update("INSERT IGNORE INTO contract(contract_id, consumer_id, streams, column_whitelist, "
                                + "row_condition, max_level, mask_rules, channel, start_policy, status) "
                                + "VALUES(?, ?, ?, ?, NULL, 3, '{}', 'internal', 'snapshot', 'active')",
                        contractRefOf(contractId), CONSUMER_ID,
                        OM.writeValueAsString(streams), OM.writeValueAsString(whitelist));
            } catch (Exception e) {
                log.warn("[sub-task] 契约预置失败(建任务时会暴露) contractId={}: {}", contractId, e.getMessage());
            }

            // ⑤ 建任务(登记态: desiredState=stopped → 内核只登记不调度)
            String resp = httpPost(controlUrl + "/api/outlet/tasks", taskJson(contractId, c, isLogical, streams));
            out.put("created", true);
            out.put("status", "STOPPED");
            out.put("response", resp);
            log.info("[sub-task] 订阅推送底座任务已登记 {} (contract={}, 只登记不激活)",
                    taskIdOf(contractId), contractRefOf(contractId));
        } catch (Exception e) {
            log.warn("[sub-task] 任务登记失败 contractId={}: {}", contractId, e.getMessage());
            out.put("created", false);
            out.put("error", String.valueOf(e.getMessage()));
        }
        return out;
    }

    /** 任务是否已登记(直接查元库 consumer_task, 不依赖控制面在线)。 */
    public boolean exists(long contractId) {
        try {
            Long n = metaJdbc.queryForObject("SELECT COUNT(*) FROM consumer_task WHERE task_id = ?",
                    Long.class, taskIdOf(contractId));
            return n != null && n > 0;
        } catch (Exception e) {
            return false;
        }
    }

    /** 停底座任务(保留登记, 部署方案阶段 resume 复活)。任务不存在/控制面不可达 → 静默。 */
    public void stopTask(long contractId) {
        if (!exists(contractId)) {
            return;
        }
        try {
            httpPost(controlUrl + "/api/outlet/tasks/" + taskIdOf(contractId) + "/actions/stop", "");
            log.info("[sub-task] 订阅推送底座任务已停 {} (契约停用)", taskIdOf(contractId));
        } catch (Exception e) {
            log.warn("[sub-task] 停任务失败 contractId={}: {}", contractId, e.getMessage());
        }
    }

    // ────────────────────────── 内部辅助 ──────────────────────────

    private static String s(Object o) {
        return o == null ? "" : String.valueOf(o);
    }

    /** 契约快照(approve 落库后读取: 含新签发 webhook_secret + view 快照) */
    private Map<String, Object> loadContract(long id) {
        try {
            List<Map<String, Object>> rows = metaJdbc.queryForList(
                    "SELECT id,name,subscriber,view_type,view_json,push,freq,target_type,target_url,topic,webhook_secret,status"
                            + " FROM data_contract WHERE id=?", id);
            if (rows.isEmpty()) return null;
            Map<String, Object> m = new LinkedHashMap<>(rows.get(0));
            try {
                m.put("view", OM.readTree((String) m.get("view_json")));
            } catch (Exception ignore) {
                m.put("view", OM.createObjectNode());
            }
            m.remove("view_json");
            return m;
        } catch (Exception e) {
            log.warn("[sub-task] 契约读取失败 id={}: {}", id, e.getMessage());
            return null;
        }
    }

    private void registerTagEventStream() {
        metaJdbc.update("INSERT IGNORE INTO chg_stream(stream, topic, kafka_bootstrap, partitions) VALUES(?, ?, ?, 1)",
                TAG_EVENT_STREAM, TAG_EVENT_TOPIC, tagEventsKafkaBootstrap);
    }

    private String taskJson(long id, Map<String, Object> c, boolean isLogical, List<String> streams) throws IllegalStateException {
        try {
            ObjectNode req = OM.createObjectNode();
            req.put("taskId", taskIdOf(id));
            req.put("name", "订阅推送:" + s(c.get("name")));
            req.put("consumerType", "sub-push");
            req.put("namespace", "sub");         // 底座多 ns 拆分: 订阅推送任务归 sub ns
            req.put("owner", CONSUMER_ID);
            ArrayNode ss = req.putArray("streams");
            streams.forEach(ss::add);
            req.put("logicId", LOGIC_ID);
            req.put("logicVersion", LOGIC_VERSION);
            req.put("instances", 1);
            req.put("contractRef", contractRefOf(id));
            req.put("startPolicy", "latest");       // 首轮全量衔接(seqFloor/tsFloor)待定, 流式只追新; 锚点缺失 fallback earliest
            req.put("desiredState", "stopped");     // 只登记不激活(部署方案阶段 resume)
            req.put("incrementalEngine", "unit");
            req.set("params", OM.valueToTree(pushParamsOf(id, c)));
            return OM.writeValueAsString(req);
        } catch (Exception e) {
            throw new IllegalStateException("构造 v2 建任务请求失败", e);
        }
    }

    /** params 组装(与 dcp-sub-push PushParams 同源同键; 提出来供单测验证形状) */
    Map<String, Object> pushParamsOf(long id, Map<String, Object> c) {
        Map<String, Object> params = new LinkedHashMap<>();
        JsonNode view = (JsonNode) c.get("view");
        params.put("contractId", id);
        params.put("contractName", s(c.get("name")));
        params.put("subscriber", s(c.get("subscriber")));
        params.put("viewType", s(c.get("view_type")));
        params.put("view", view);
        String tt = s(c.get("target_type"));
        params.put("targetType", tt);
        if ("webhook".equals(tt)) {
            params.put("targetUrl", s(c.get("target_url")));
            params.put("webhookSecret", s(c.get("webhook_secret")));
        } else {
            String topic = s(c.get("topic"));
            if (topic.isBlank()) topic = "dcp.sub." + s(c.get("subscriber")) + "." + id;
            params.put("topic", topic);
            params.put("kafkaBootstrap", pushKafkaBootstrap);
        }
        // 元库连接(tag 契约 exclude 剔除回查 tag_fact_view; logical 契约无 exclude 时 PushLogic 忽略)
        if (metaJdbcUrl != null && !metaJdbcUrl.isBlank()) {
            params.put("jdbcUrl", metaJdbcUrl);
            params.put("user", metaUser);
            params.put("password", metaPassword);
        }
        return params;
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
