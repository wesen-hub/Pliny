package com.dcp.module.lakesync;

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
import java.util.List;
import java.util.Map;

/**
 * 湖增量底座任务**登记桥**(P3, 维度⑩ 数据湖仓):把逻辑表的湖增量消费登记成 v2 底座任务
 * (consumer_task), 执行体 = dcp-outlet-rt 装载 dcp-lake-task jar(lake-sync@1.0.0)。
 *
 * <p>登记链路(与 TagV2Deployer 同模式, 消费出口栈 TiCDC 流):
 * <ol>
 *   <li>流登记:触发控制面 {@code POST /api/outlet/streams/scan}(扫描器发现 TiCDC topic
 *       {@code dcp.cdc.{库}.{表}} 并登记为流 {@code {表}}, 带回真实分区数与出口栈地址);</li>
 *   <li>logic 注册:{@code lake-sync@1.0.0}(unit 轨道由底座 JarLogicLoader 从
 *       logic-repo/lake-sync 加载执行);</li>
 *   <li>平台消费方账号 {@code lake-platform} + permissive 契约 {@code ct-lake-{lt}}
 *       (白名单 = ["*"] 全放行: 湖增量要整行(业务列 + 写者列 _s 前缀/seq)透传);</li>
 *   <li>建任务:{@code POST /api/outlet/tasks},startPolicy=latest(湖表历史由批作业
 *       dcp-lakesync 打底, 增量只追新),incrementalEngine=unit,
 *       params = 元库连接 + entityKeyColumns + lake 段(credential 长驻形态:
 *       RESTCatalog 自动换 token; 与批作业短时 token 同源同键)。</li>
 * </ol>
 *
 * <p><b>只登记不激活</b>(部署方案阶段再统一激活执行栈):建任务带 desiredState=stopped,
 * 内核对此不调度(见 TaskKernelService);任务记录在册, 状态 STOPPED, 前端可见。
 */
@Component
public class LakeTaskDeployer {

    private static final Logger log = LoggerFactory.getLogger(LakeTaskDeployer.class);
    private static final ObjectMapper OM = new ObjectMapper();

    public static final String LOGIC_ID = "lake-sync";
    public static final String LOGIC_VERSION = "1.0.0";
    public static final String CONSUMER_ID = "lake-platform";

    private final JdbcTemplate metaJdbc;
    private final String controlUrl;
    /** 控制面调用客户端(GovernanceConfig @LoadBalanced: 服务名 → Nacos 轮询, IP → 直连降级) */
    private final RestClient rest;
    /** 底座容器内可达的元库地址(logical_table 业务键读 / dlt_message 落库) */
    private final String metaJdbcUrl;
    private final String metaUser;
    private final String metaPassword;
    /** lake 段(与 LakeSyncService 同源配置: catalog + S3; rest=RESTCatalog(Polaris) / hadoop=HadoopCatalog 直连 S3) */
    private final String catalogType;
    private final String catalogName;
    private final String catalogCredential;
    private final String catalogUri;
    private final String catalogJdbcUri;
    private final String catalogWarehouse;
    private final String catalogNamespace;
    private final String s3Endpoint;
    private final String s3AccessKey;
    private final String s3SecretKey;

    public LakeTaskDeployer(JdbcTemplate metaJdbc,
                            RestClient.Builder loadBalancedBuilder,
                            @Value("${dcp.lakesync.outlet-control-url:http://dcp-outlet-control}") String controlUrl,
                            @Value("${dcp.lakesync.meta-jdbc-url:}") String metaJdbcUrl,
                            @Value("${dcp.lakesync.meta-user:root}") String metaUser,
                            @Value("${dcp.lakesync.meta-password:}") String metaPassword,
                            @Value("${dcp.lakesync.catalog.type:rest}") String catalogType,
                            @Value("${dcp.lakesync.catalog.name:demo}") String catalogName,
                            @Value("${dcp.lakesync.catalog.credential:root:s3cr3t}") String catalogCredential,
                            @Value("${dcp.lakesync.catalog.uri:}") String catalogUri,
                            @Value("${dcp.lakesync.catalog.jdbc-uri:}") String catalogJdbcUri,
                            @Value("${dcp.lakesync.catalog.warehouse:demo}") String catalogWarehouse,
                            @Value("${dcp.lakesync.catalog.namespace:dcp}") String catalogNamespace,
                            @Value("${dcp.lakesync.catalog.s3.endpoint:http://dcp-minio:9000}") String s3Endpoint,
                            @Value("${dcp.lakesync.catalog.s3.access-key:minioadmin}") String s3AccessKey,
                            @Value("${dcp.lakesync.catalog.s3.secret-key:minioadmin123}") String s3SecretKey) {
        this.metaJdbc = metaJdbc;
        this.controlUrl = controlUrl.endsWith("/") ? controlUrl.substring(0, controlUrl.length() - 1) : controlUrl;
        // 中危-4: 默认无超时; 连接 5s / 读 15s 与 WorkerClient 同口径
        HttpClient httpClient = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
        JdkClientHttpRequestFactory factory = new JdkClientHttpRequestFactory(httpClient);
        factory.setReadTimeout(Duration.ofSeconds(15));
        this.rest = loadBalancedBuilder.baseUrl(this.controlUrl).requestFactory(factory).build();
        this.metaJdbcUrl = metaJdbcUrl;
        this.metaUser = metaUser;
        this.metaPassword = metaPassword;
        this.catalogType = catalogType;
        this.catalogName = catalogName;
        this.catalogCredential = catalogCredential;
        this.catalogUri = catalogUri;
        this.catalogJdbcUri = catalogJdbcUri;
        this.catalogWarehouse = catalogWarehouse;
        this.catalogNamespace = catalogNamespace;
        this.s3Endpoint = s3Endpoint;
        this.s3AccessKey = s3AccessKey;
        this.s3SecretKey = s3SecretKey;
    }

    // ────────────────────────── 命名约定 ──────────────────────────

    public static String taskIdOf(String logicalTable) {
        return "lake-inc-" + logicalTable;
    }

    /** TiCDC 流登记名(扫描器从 topic {@code dcp.cdc.{库}.{表}} 派生的流名, 即 TiDB 目标表名) */
    public static String streamOf(String logicalTable) {
        return logicalTable;
    }

    public static String contractOf(String logicalTable) {
        return "ct-lake-" + logicalTable;
    }

    // ────────────────────────── 登记(幂等) ──────────────────────────

    /**
     * 登记逻辑表的湖增量底座任务(幂等:任务已存在 → 跳过,不改变现有状态)。
     * 失败不抛(登记属机会型动作,调用方按 error 字段决策)。
     *
     * @param partitions 已废弃(旧手工登记 chg_stream 的参数; 流现由控制面扫描器登记)
     * @return {taskId, contractRef, created, status/error}
     */
    public Map<String, Object> deploy(String logicalTable, List<String> keyColumns, int partitions) {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("taskId", taskIdOf(logicalTable));
        out.put("contractRef", contractOf(logicalTable));
        if (exists(logicalTable)) {
            out.put("created", false);
            return out;
        }
        try {
            // ① 流登记:控制面扫描(TiCDC topic dcp.cdc.{库}.{表} 自动登记为流, 带回真实分区数与出口栈地址)
            try {
                httpPost("/api/outlet/streams/scan", "");
            } catch (Exception e) {
                log.warn("[lake-task] 流扫描失败(继续尝试建任务,建任务时会暴露流未登记) {}: {}",
                        logicalTable, e.getMessage());
            }

            // ② logic 注册
            metaJdbc.update("INSERT IGNORE INTO consumer_logic(logic_id, version, description, status) "
                            + "VALUES(?, ?, '湖增量底座任务(出口栈 TiCDC 流 dcp.cdc.* 行数据 → 写者 seq 裁判/字段级合并 → Iceberg 湖表覆盖写)', 'active')",
                    LOGIC_ID, LOGIC_VERSION);

            // ③ 平台消费方账号
            metaJdbc.update("INSERT IGNORE INTO consumer(consumer_id, name, consumer_type, owner, status, channel) "
                            + "VALUES(?, '湖增量平台桥', 'lake', ?, 'active', 'internal')",
                    CONSUMER_ID, CONSUMER_ID);

            // ④ permissive 契约(["*"] = 全放行: 湖增量要整行透传(业务列 + 写者列 _s*/seq 动态不可枚举);
            //    行条件/脱敏不设)
            try {
                ObjectNode whitelist = OM.createObjectNode();
                whitelist.putArray(streamOf(logicalTable)).add("*");
                metaJdbc.update("INSERT IGNORE INTO contract(contract_id, consumer_id, streams, column_whitelist, "
                                + "row_condition, max_level, mask_rules, channel, start_policy, status) "
                                + "VALUES(?, ?, ?, ?, NULL, 3, '{}', 'internal', 'snapshot', 'active')",
                        contractOf(logicalTable), CONSUMER_ID,
                        "[\"" + streamOf(logicalTable) + "\"]", OM.writeValueAsString(whitelist));
            } catch (Exception e) {
                log.warn("[lake-task] 契约预置失败(建任务时会暴露) {}: {}", logicalTable, e.getMessage());
            }

            // ⑤ 建任务(登记态:desiredState=stopped → 内核只登记不调度)
            String resp = httpPost("/api/outlet/tasks", taskJson(logicalTable, keyColumns));
            out.put("created", true);
            out.put("status", "STOPPED");
            out.put("response", resp);
            log.info("[lake-task] 湖增量底座任务已登记 {} (contract={}, 只登记不激活)",
                    taskIdOf(logicalTable), contractOf(logicalTable));
        } catch (Exception e) {
            log.warn("[lake-task] 任务登记失败 {}: {}", logicalTable, e.getMessage());
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
            httpPost("/api/outlet/tasks/" + taskIdOf(logicalTable) + "/actions/stop", "");
            log.info("[lake-task] 湖增量底座任务已停 {} (湖同步任务删除)", taskIdOf(logicalTable));
        } catch (Exception e) {
            log.warn("[lake-task] 停任务失败 {}: {}", logicalTable, e.getMessage());
        }
    }

    // ────────────────────────── 状态探针(前端指标适配) ──────────────────────────

    /** 底座任务状态视图(status 字段;控制面不可达 → UNKNOWN)。 */
    public String statusOf(String logicalTable) {
        try {
            String body = httpGet("/api/outlet/tasks/" + taskIdOf(logicalTable));
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
            String body = httpGet("/api/outlet/tasks/" + taskIdOf(logicalTable) + "/progress");
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

    private String taskJson(String logicalTable, List<String> keyColumns) throws IllegalStateException {
        try {
            ObjectNode req = OM.createObjectNode();
            req.put("taskId", taskIdOf(logicalTable));
            req.put("name", "湖增量:" + logicalTable);
            req.put("consumerType", "lake");
            req.put("namespace", "lake");        // 底座多 ns 拆分: 湖增量任务归 lake ns
            req.put("owner", CONSUMER_ID);
            req.putArray("streams").add(streamOf(logicalTable));
            req.put("logicId", LOGIC_ID);
            req.put("logicVersion", LOGIC_VERSION);
            req.put("instances", 1);
            req.put("contractRef", contractOf(logicalTable));
            req.put("startPolicy", "latest");       // 湖表历史由批作业打底, 增量只追新;锚点缺失 fallback earliest 不丢
            req.put("desiredState", "stopped");     // 只登记不激活(部署方案阶段 resume)
            req.put("incrementalEngine", "unit");
            ObjectNode params = req.putObject("params");
            params.put("logicalTable", logicalTable);
            if (keyColumns != null && !keyColumns.isEmpty()) {
                ArrayNode keys = params.putArray("entityKeyColumns");
                keyColumns.forEach(keys::add);
            }
            if (metaJdbcUrl != null && !metaJdbcUrl.isBlank()) {
                params.put("jdbcUrl", metaJdbcUrl);
                params.put("user", metaUser);
                params.put("password", metaPassword);
            }
            ObjectNode lake = params.putObject("lake");
            lake.put("catalog", catalogName);
            lake.put("namespace", catalogNamespace);
            lake.put("table", logicalTable);
            lake.put("type", catalogType);
            if ("hadoop".equalsIgnoreCase(catalogType)) {
                // HadoopCatalog 直连 S3(无 REST catalog 服务场景): warehouse = s3://基路径
                lake.put("warehouse", catalogWarehouse);
            } else if ("jdbc".equalsIgnoreCase(catalogType)) {
                // JDBC Catalog: 元数据存 RDS(免 Hadoop fs/s3 scheme 注册), 数据与 metadata 文件走 S3FileIO
                lake.put("uri", catalogJdbcUri);
                lake.put("warehouse", catalogWarehouse);
            } else {
                lake.put("uri", catalogUri);
                lake.put("warehouse", catalogWarehouse);
                // 长驻进程推荐形态: credential + scope → RESTCatalog 自动换 token(批作业用短时 token, 同源)
                lake.put("credential", catalogCredential);
                lake.put("scope", "PRINCIPAL_ROLE:ALL");
            }
            lake.put("ioImpl", "org.apache.iceberg.aws.s3.S3FileIO");
            ObjectNode s3 = lake.putObject("s3");
            s3.put("endpoint", s3Endpoint);
            s3.put("pathStyleAccess", "true");
            s3.put("accessKey", s3AccessKey);
            s3.put("secretKey", s3SecretKey);
            s3.put("region", "us-east-1");
            return OM.writeValueAsString(req);
        } catch (Exception e) {
            throw new IllegalStateException("构造 v2 建任务请求失败", e);
        }
    }

    private String httpPost(String path, String body) {
        try {
            return rest.post().uri(path)
                    .header("Content-Type", "application/json")
                    .body(body == null ? "" : body)
                    .retrieve()
                    .onStatus(s -> s.value() / 100 != 2,
                            (req2, resp2) -> {
                                throw new IllegalStateException("HTTP " + resp2.getStatusCode() + " " + resp2.getBody());
                            })
                    .body(String.class);
        } catch (IllegalStateException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException("HTTP 失败:" + path, e);
        }
    }

    private String httpGet(String path) {
        try {
            return rest.get().uri(path)
                    .retrieve()
                    .onStatus(s -> s.value() / 100 != 2,
                            (req2, resp2) -> {
                                throw new IllegalStateException("HTTP " + resp2.getStatusCode());
                            })
                    .body(String.class);
        } catch (IllegalStateException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException("HTTP 失败:" + path, e);
        }
    }
}
