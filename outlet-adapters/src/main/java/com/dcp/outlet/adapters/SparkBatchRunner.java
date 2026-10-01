package com.dcp.outlet.adapters;

import com.dcp.outlet.control.BatchRunner;
import com.dcp.outlet.control.ContractInfo;
import com.dcp.outlet.control.ContractLookup;
import com.dcp.outlet.control.StreamInfo;
import com.dcp.outlet.control.StreamRegistry;
import com.dcp.outlet.control.T0Selector;
import com.dcp.outlet.control.TaskRecord;
import com.dcp.outlet.control.TaskStore;
import com.dcp.outlet.sdk.SdkLogger;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.Map;
import java.util.Optional;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/**
 * 方案 C 的 Spark 批执行体:{@code BatchRunner} 实现 —— 把打标批阶段提交给 tagengine-executor。
 *
 * <p>控制面选中 Spark(估算行数 ≥100万,见 {@link BatchExecutorPolicy})后,本类:
 * 取任务 params(打标配置桥)→ 算 T0 → 组装 task-json(tagengine-executor 契约)→ spark-submit → 等退出码 → 回调。
 *
 * <p>注意:task-json 的 datasetFields 类型({@code type})与"读快照而非读湖"由执行器侧承接(见迁移设计 §5.3);
 * 本类只负责把已知信息打包下发。
 */
public class SparkBatchRunner implements BatchRunner {

    private static final ObjectMapper OM = new ObjectMapper();

    private final TaskStore tasks;
    private final StreamRegistry streams;
    private final T0Selector t0;
    private final SdkLogger log;
    private final String sparkSubmitBin;
    private final String executorJar;
    private final String catalog;
    private final String namespace;
    /** 平台快照 API 宿主(下发到 task-json 的 snapshot.baseUrl; 执行器读快照用) */
    private final String snapshotBaseUrl;
    /** 契约查询:任务 contractRef → consumer_id(批/增量身份一致性) */
    private final ContractLookup contracts;
    /** 执行器写湖目标(可空; 空则不写 warehouse/catalogType → 由 spark-submit 环境自备 catalog, 生产口径) */
    private final String warehouse;
    private final String catalogType;
    /** 全局规划代(锚点索引维度,统一由配置 outlet.epoch 管理;彻底版:任务级 epoch 已删) */
    private final int epoch;

    public SparkBatchRunner(TaskStore tasks, StreamRegistry streams, T0Selector t0, SdkLogger log,
                            String sparkSubmitBin, String executorJar, String catalog, String namespace,
                            String snapshotBaseUrl, ContractLookup contracts,
                            String warehouse, String catalogType, int epoch) {
        this.tasks = tasks;
        this.streams = streams;
        this.t0 = t0;
        this.log = log;
        this.sparkSubmitBin = sparkSubmitBin;
        this.executorJar = executorJar;
        this.catalog = catalog;
        this.namespace = namespace;
        this.snapshotBaseUrl = snapshotBaseUrl;
        this.contracts = contracts;
        this.warehouse = warehouse;
        this.catalogType = catalogType;
        this.epoch = epoch;
    }

    @Override
    public void runAsync(String taskId, BatchCallback callback) {
        Thread t = new Thread(() -> {
            try {
                TaskRecord task = tasks.byId(taskId);
                if (task == null || task.streams().isEmpty()) {
                    callback.done(0L, false);
                    return;
                }
                Optional<StreamInfo> si = streams.get(task.streams().get(0));
                if (si.isEmpty()) {
                    callback.done(0L, false);
                    return;
                }
                java.util.Optional<String> identity = SchedulerBatchRunner.resolveConsumer(task, contracts);
                if (identity.isEmpty()) {
                    log.error("Spark 批阶段失败:任务契约身份不可用(contractRef={}) —— 契约缺失/未生效/无 consumer_id",
                            task.contractRef());
                    callback.done(0L, false);
                    return;
                }
                String consumerId = identity.get();
                T0Selector.Decision d = t0.select(si.get().topic(), 0, null);
                String json = buildTaskJson(task, si.get().stream(), si.get().topic(), d.tso(), epoch, catalog, namespace,
                        snapshotBaseUrl, consumerId, warehouse, catalogType);
                log.info("Spark 批阶段 task={} T0={} taskJson={}", taskId, d.tso(), json);

                // 提交:写 task-json → spark-submit → 等退出码
                if (executorJar == null || executorJar.isBlank()) {
                    log.warn("未配置 outlet.spark.executor-jar → 只落 task-json,不提交 Spark(任务置 BATCH_FAILED)");
                    callback.done(d.tso(), false);
                } else {
                    boolean ok = submit(json);
                    callback.done(d.tso(), ok);
                }
            } catch (Throwable e) {
                log.error("Spark 批阶段异常 task=" + taskId, e);
                callback.done(0L, false);
            }
        }, "outlet-spark-" + taskId);
        t.setDaemon(true);
        t.start();
    }

    /** 组装 task-json(tagengine-executor 的 TaskConfig 契约)。公开以便离线自检。 */
    public static String buildTaskJson(TaskRecord task, String stream, String topic, long t0, int epoch,
                                       String catalog, String namespace, String snapshotBaseUrl,
                                       String consumerId, String warehouse, String catalogType) {
        Map<String, Object> p = task.params();
        ObjectNode cfg = OM.createObjectNode();
        // 执行器 TaskConfig.taskId 是 long:优先取 params.taskId(平台数值任务 id);缺失才回退 v2 字符串 id
        Object platformId = p.get("taskId");
        if (platformId instanceof Number n) cfg.put("taskId", n.longValue());
        else cfg.put("taskId", task.taskId());
        cfg.put("taskVer", epoch);
        cfg.put("dataset", str(p.get("dataset"), stream));
        cfg.put("entityKey", str(p.get("entityKey"), "id"));
        cfg.put("t0", t0);

        ObjectNode iceberg = cfg.putObject("iceberg");
        iceberg.put("catalog", catalog);
        iceberg.put("namespace", namespace);
        iceberg.put("table", str(p.get("dataset"), stream));
        // 写湖目标(可空;空 = 由 spark-submit 环境自备 catalog;本地/联调场景下发展 warehouse)
        if (warehouse != null && !warehouse.isBlank()) iceberg.put("warehouse", warehouse);
        if (catalogType != null && !catalogType.isBlank()) iceberg.put("catalogType", catalogType);

        // tag_fact 事实层写目标:与数据集同 catalog/namespace(快照源模式下读不碰湖,但写仍需落湖)
        ObjectNode tagFact = cfg.putObject("tagFact");
        tagFact.put("catalog", catalog);
        tagFact.put("namespace", namespace);
        tagFact.put("table", "tag_fact");
        tagFact.put("partitionByTask", true);

        // 快照源(方案 C): 执行器据此读平台快照 API(AS OF T0)而非读湖;baseUrl 空 → 执行器回读湖模式。
        // consumerId = 任务契约身份(批/增量一致性), 由调用方解析 contractRef → consumer_id 后传入。
        if (snapshotBaseUrl != null && !snapshotBaseUrl.isBlank()) {
            ObjectNode snap = cfg.putObject("snapshot");
            snap.put("baseUrl", snapshotBaseUrl);
            snap.put("consumerId", consumerId == null ? "" : consumerId);
            snap.put("pageSize", 1000);
        }

        // datasetFields:params 里是 {name, kind}(S/L/D/B)→ 执行器 TaskConfig 契约为 {name, type}
        // 兼容两种形态:DB 读回是 Map/List,内存直传是 JsonNode —— 统一 valueToTree
        ArrayNode ds = cfg.putArray("datasetFields");
        JsonNode dsNode = p.get("datasetFields") == null ? null : OM.valueToTree(p.get("datasetFields"));
        if (dsNode != null && dsNode.isArray()) {
            for (JsonNode f : dsNode) {
                String nm, kind = "S";
                if (f.isTextual()) { nm = f.asText(); }
                else { nm = f.path("name").asText(""); kind = f.path("kind").asText("S"); }
                if (nm.isBlank()) continue;
                ds.addObject().put("name", nm).put("type", kindToType(kind));
            }
        }

        passthrough(cfg, p, "derived");
        passthrough(cfg, p, "bindings");
        passthrough(cfg, p, "rules");
        if (p.get("tagIds") instanceof Map<?, ?> m) cfg.set("tagIds", OM.valueToTree(m));

        try {
            return OM.writeValueAsString(cfg);
        } catch (Exception e) {
            throw new IllegalStateException("task-json 序列化失败", e);
        }
    }

    private boolean submit(String taskJson) {
        try {
            Path tmp = Files.createTempFile("dcp-tag-task-", ".json");
            Files.writeString(tmp, taskJson, java.nio.charset.StandardCharsets.UTF_8);
            List<String> cmd = List.of(sparkSubmitBin, "--class", "com.dcp.tagjob.TagBatchJob",
                    executorJar, "--task-json", tmp.toString());
            log.info("spark-submit: {}", String.join(" ", cmd));
            Process p = new ProcessBuilder(cmd).redirectErrorStream(true).start();
            int exit = p.waitFor();
            log.info("spark-submit 退出码={} taskJson={}", exit, tmp);
            return exit == 0;
        } catch (Exception e) {
            log.error("spark-submit 失败", e);
            return false;
        }
    }

    private static void passthrough(ObjectNode cfg, Map<String, Object> p, String key) {
        Object v = p.get(key);
        if (v != null) cfg.set(key, OM.valueToTree(v));   // JsonNode / Map / List / 标量统一转
    }

    /** kind(S/L/D/B)→ 执行器 type(Spark TaskConfig 契约为 long/double/boolean/string) */
    private static String kindToType(String kind) {
        return switch (kind == null ? "S" : kind) {
            case "L" -> "long";
            case "D" -> "double";
            case "B" -> "boolean";
            default -> "string";
        };
    }

    private static String str(Object v, String def) {
        return v == null ? def : String.valueOf(v);
    }
}
