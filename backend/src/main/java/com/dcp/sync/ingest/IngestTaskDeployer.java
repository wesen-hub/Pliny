package com.dcp.sync.ingest;

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
 * 事件接入(入站)底座任务**登记桥**(P3):把逻辑表的入站消费登记成 v2 底座任务(consumer_task),
 * 执行体 = dcp-outlet-rt 装载 dcp-ingest-task jar(kafka-ingest@1.0.0)。
 *
 * <p>登记链路(与 tagengine-service TagV2Deployer 同模式):
 * <ol>
 *   <li>流登记:{@code chg_stream} 手工登记 {@code {lt}-in} → {@code dcp.in.{lt}}
 *       (扫描器只认 dcp.cdc.*, 入站流由本桥登记);</li>
 *   <li>logic 注册:{@code kafka-ingest@1.0.0}(unit 轨道由底座 JarLogicLoader 从
 *       logic-repo/kafka-ingest 加载执行);</li>
 *   <li>平台消费方账号 {@code ingest-platform} + permissive 契约 {@code ct-ingest-{lt}}
 *       (白名单 = op/submitter/ts/data 四键, 入站消息全透传);</li>
 *   <li>建任务:{@code POST /api/outlet/tasks},startPolicy=latest(入站无快照语义,
 *       锚点缺失 fallback earliest 不丢消息),incrementalEngine=unit,
 *       params = 底座容器视角的元库连接 + 平台栈 Kafka。</li>
 * </ol>
 *
 * <p><b>只登记不激活</b>(部署方案阶段再统一解决执行栈归属与解码器后 resume 复活):
 * 建任务带 desiredState=stopped,内核对此不调度(见 TaskKernelService);任务记录在册,
 * 状态 STOPPED,前端可见。停用/删除应用后无启用应用的表 → stop(保留登记,可复活)。
 */
@Component
public class IngestTaskDeployer {

    private static final Logger log = LoggerFactory.getLogger(IngestTaskDeployer.class);
    private static final ObjectMapper OM = new ObjectMapper();

    public static final String LOGIC_ID = "kafka-ingest";
    public static final String LOGIC_VERSION = "1.0.0";
    public static final String CONSUMER_ID = "ingest-platform";

    private final JdbcTemplate metaJdbc;
    private final String controlUrl;
    /** 底座容器内可达的元库地址(seq_counter/logical_table/dlt_message 所在,与 backend 同库不同视角) */
    private final String metaJdbcUrl;
    private final String metaUser;
    private final String metaPassword;
    /** 底座容器内可达的平台栈 Kafka(入站 topic dcp.in.{表} 与信封流 dcp.{表} 所在) */
    private final String kafkaBootstrap;

    public IngestTaskDeployer(JdbcTemplate metaJdbc,
                              @Value("${dcp.ingest.outlet-control-url:http://127.0.0.1:8095}") String controlUrl,
                              @Value("${dcp.ingest.meta-jdbc-url:}") String metaJdbcUrl,
                              @Value("${dcp.ingest.meta-user:root}") String metaUser,
                              @Value("${dcp.ingest.meta-password:}") String metaPassword,
                              @Value("${dcp.ingest.kafka-bootstrap:localhost:9092}") String kafkaBootstrap) {
        this.metaJdbc = metaJdbc;
        this.controlUrl = controlUrl.endsWith("/") ? controlUrl.substring(0, controlUrl.length() - 1) : controlUrl;
        this.metaJdbcUrl = metaJdbcUrl;
        this.metaUser = metaUser;
        this.metaPassword = metaPassword;
        this.kafkaBootstrap = kafkaBootstrap;
    }

    // ────────────────────────── 命名约定 ──────────────────────────

    public static String taskIdOf(String logicalTable) {
        return "ingest-" + logicalTable;
    }

    /** 入站流登记名(与扫描器登记的纯表名区分,避免主键冲突) */
    public static String streamOf(String logicalTable) {
        return logicalTable + "-in";
    }

    public static String contractOf(String logicalTable) {
        return "ct-ingest-" + logicalTable;
    }

    // ────────────────────────── 登记(幂等) ──────────────────────────

    /**
     * 登记逻辑表的入站底座任务(幂等:任务已存在 → 跳过,不改变现有状态)。
     * 失败不抛(登记属机会型动作,调用方按 error 字段决策)。
     *
     * @return {taskId, contractRef, created, status/error}
     */
    public Map<String, Object> deploy(String logicalTable, int partitions) {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("taskId", taskIdOf(logicalTable));
        out.put("contractRef", contractOf(logicalTable));
        if (exists(logicalTable)) {
            out.put("created", false);
            return out;
        }
        try {
            // ① 流登记(手工;扫描器只认 dcp.cdc.*, 入站流不在此列;带平台栈 kafka_bootstrap)
            metaJdbc.update("INSERT INTO chg_stream(stream, topic, kafka_bootstrap, partitions) VALUES(?, ?, ?, ?) "
                            + "ON DUPLICATE KEY UPDATE topic=VALUES(topic), kafka_bootstrap=VALUES(kafka_bootstrap), partitions=VALUES(partitions)",
                    streamOf(logicalTable), KafkaIngestEngine.IN_TOPIC_PREFIX + logicalTable, kafkaBootstrap, partitions);

            // ② logic 注册
            metaJdbc.update("INSERT IGNORE INTO consumer_logic(logic_id, version, description, status) "
                            + "VALUES(?, ?, '事件接入底座任务(dcp.in.{表} → 验证/领号/信封 → dcp.{表})', 'active')",
                    LOGIC_ID, LOGIC_VERSION);

            // ③ 平台消费方账号
            metaJdbc.update("INSERT IGNORE INTO consumer(consumer_id, name, consumer_type, owner, status, channel) "
                            + "VALUES(?, '事件接入平台桥', 'ingest', ?, 'active', 'internal')",
                    CONSUMER_ID, CONSUMER_ID);

            // ④ permissive 契约(白名单 = 入站消息四键, 全透传;行条件/脱敏不设)
            try {
                ObjectNode whitelist = OM.createObjectNode();
                ArrayNode cols = whitelist.putArray(streamOf(logicalTable));
                cols.add("op");
                cols.add("submitter");
                cols.add("ts");
                cols.add("data");
                metaJdbc.update("INSERT IGNORE INTO contract(contract_id, consumer_id, streams, column_whitelist, "
                                + "row_condition, max_level, mask_rules, channel, start_policy, status) "
                                + "VALUES(?, ?, ?, ?, NULL, 3, '{}', 'internal', 'snapshot', 'active')",
                        contractOf(logicalTable), CONSUMER_ID,
                        "[\"" + streamOf(logicalTable) + "\"]", OM.writeValueAsString(whitelist));
            } catch (Exception e) {
                log.warn("[ingest-task] 契约预置失败(建任务时会暴露) {}: {}", logicalTable, e.getMessage());
            }

            // ⑤ 建任务(登记态:desiredState=stopped → 内核只登记不调度)
            String resp = httpPost(controlUrl + "/api/outlet/tasks", taskJson(logicalTable));
            out.put("created", true);
            out.put("status", "STOPPED");
            out.put("response", resp);
            log.info("[ingest-task] 入站底座任务已登记 {} (contract={}, 只登记不激活)",
                    taskIdOf(logicalTable), contractOf(logicalTable));
        } catch (Exception e) {
            log.warn("[ingest-task] 任务登记失败 {}: {}", logicalTable, e.getMessage());
            out.put("created", false);
            out.put("error", String.valueOf(e.getMessage()));
        }
        return out;
    }

    /** 任务是否已登记(直接查元库 consumer_task,不依赖控制面在线)。 */
    public boolean exists(String logicalTable) {
        try {
            Long n = metaJdbc.queryForObject("SELECT COUNT(*) FROM consumer_task WHERE task_id = ?",
                    Long.class, taskIdOf(logicalTable));
            return n != null && n > 0;
        } catch (Exception e) {
            return false;
        }
    }

    /** 停底座任务(保留登记,部署方案阶段 resume 复活)。任务不存在/控制面不可达 → 静默。 */
    public void stop(String logicalTable) {
        if (!exists(logicalTable)) {
            return;
        }
        try {
            httpPost(controlUrl + "/api/outlet/tasks/" + taskIdOf(logicalTable) + "/actions/stop", "");
            log.info("[ingest-task] 入站底座任务已停 {} (无启用应用)", taskIdOf(logicalTable));
        } catch (Exception e) {
            log.warn("[ingest-task] 停任务失败 {}: {}", logicalTable, e.getMessage());
        }
    }

    // ────────────────────────── 状态探针(前端指标适配) ──────────────────────────

    /** 底座任务状态视图(status 字段;控制面不可达 → UNKNOWN)。 */
    public String statusOf(String logicalTable) {
        try {
            String body = httpGet(controlUrl + "/api/outlet/tasks/" + taskIdOf(logicalTable));
            JsonNode n = OM.readTree(body);
            if (n.has("error")) {
                return "UNKNOWN";
            }
            return n.path("status").asText("UNKNOWN");
        } catch (Exception e) {
            return "UNKNOWN";
        }
    }

    /** 底座任务消费滞后(进度页 lagRows 求和;不可达/无进度 → -1)。 */
    public long lagOf(String logicalTable) {
        try {
            String body = httpGet(controlUrl + "/api/outlet/tasks/" + taskIdOf(logicalTable) + "/progress");
            JsonNode n = OM.readTree(body);
            if (!n.has("progress") || !n.get("progress").isArray()) {
                return -1;
            }
            long lag = 0;
            boolean any = false;
            for (JsonNode row : n.get("progress")) {
                long v = row.path("lagRows").asLong(-1);
                if (v >= 0) {
                    lag += v;
                    any = true;
                }
            }
            return any ? lag : -1;
        } catch (Exception e) {
            return -1;
        }
    }

    // ────────────────────────── 内部辅助 ──────────────────────────

    private String taskJson(String logicalTable) throws IllegalStateException {
        try {
            ObjectNode req = OM.createObjectNode();
            req.put("taskId", taskIdOf(logicalTable));
            req.put("name", "事件接入:" + logicalTable);
            req.put("consumerType", "ingest");
            req.put("namespace", "ingest");      // 底座多 ns 拆分: 入站任务归 ingest ns
            req.put("owner", CONSUMER_ID);
            req.putArray("streams").add(streamOf(logicalTable));
            req.put("logicId", LOGIC_ID);
            req.put("logicVersion", LOGIC_VERSION);
            req.put("contractRef", contractOf(logicalTable));
            req.put("startPolicy", "latest");       // 入站无快照语义;锚点缺失 fallback earliest 不丢消息
            req.put("desiredState", "stopped");     // 只登记不激活(部署方案阶段 resume)
            req.put("incrementalEngine", "unit");
            ObjectNode params = req.putObject("params");
            params.put("logicalTable", logicalTable);
            if (metaJdbcUrl != null && !metaJdbcUrl.isBlank()) {
                params.put("jdbcUrl", metaJdbcUrl);
                params.put("user", metaUser);
                params.put("password", metaPassword);
            }
            params.put("kafkaBootstrap", kafkaBootstrap);
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

    private static String httpGet(String url) {
        try {
            HttpRequest req = HttpRequest.newBuilder(URI.create(url))
                    .timeout(Duration.ofSeconds(10))
                    .GET()
                    .build();
            HttpResponse<String> resp = HttpClient.newHttpClient().send(req, HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() / 100 != 2) {
                throw new IllegalStateException("HTTP " + resp.statusCode());
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
