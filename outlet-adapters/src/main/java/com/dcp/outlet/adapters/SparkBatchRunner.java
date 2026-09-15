package com.dcp.outlet.adapters;

import com.dcp.outlet.control.BatchRunner;
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

    public SparkBatchRunner(TaskStore tasks, StreamRegistry streams, T0Selector t0, SdkLogger log,
                            String sparkSubmitBin, String executorJar, String catalog, String namespace) {
        this.tasks = tasks;
        this.streams = streams;
        this.t0 = t0;
        this.log = log;
        this.sparkSubmitBin = sparkSubmitBin;
        this.executorJar = executorJar;
        this.catalog = catalog;
        this.namespace = namespace;
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
                T0Selector.Decision d = t0.select(si.get().topic(), 0, null);
                String json = buildTaskJson(task, si.get().stream(), si.get().topic(), d.tso(), catalog, namespace);
                log.info("Spark 批阶段 task={} T0={} taskJson={}", taskId, d.tso(), json);

                // TODO(提交):spark-submit --class com.dcp.tagjob.TagBatchJob <executorJar> --task-json <file>
                // 未配 executorJar 时只落 task-json,不提交(与 Flink 适配器同策略)
                if (executorJar == null || executorJar.isBlank()) {
                    log.warn("未配置 executorJar → 只落 task-json,不提交 Spark(任务置 BATCH_FAILED)");
                    callback.done(d.tso(), false);
                } else {
                    callback.done(d.tso(), false);   // 提交逻辑待接(需执行器 jar + Spark master)
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
    public static String buildTaskJson(TaskRecord task, String stream, String topic, long t0,
                                       String catalog, String namespace) {
        Map<String, Object> p = task.params();
        ObjectNode cfg = OM.createObjectNode();
        cfg.put("taskId", task.taskId());
        cfg.put("taskVer", task.epoch());
        cfg.put("dataset", str(p.get("dataset"), stream));
        cfg.put("entityKey", str(p.get("entityKey"), "id"));
        cfg.put("t0", t0);

        ObjectNode iceberg = cfg.putObject("iceberg");
        iceberg.put("catalog", catalog);
        iceberg.put("namespace", namespace);
        iceberg.put("table", str(p.get("dataset"), stream));

        // datasetFields:字符串数组 → {name, type}(type 暂 string,真实类型由执行器/平台探测回填)
        ArrayNode ds = cfg.putArray("datasetFields");
        Object dsRaw = p.get("datasetFields");
        if (dsRaw instanceof JsonNode n && n.isArray()) {
            for (JsonNode f : n) {
                if (f.isTextual()) ds.addObject().put("name", f.asText()).put("type", "string");
                else ds.add(f);
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

    private static void passthrough(ObjectNode cfg, Map<String, Object> p, String key) {
        if (p.get(key) instanceof JsonNode n) cfg.set(key, n);
    }

    private static String str(Object v, String def) {
        return v == null ? def : String.valueOf(v);
    }
}
