package com.dcp.tagjob;

import com.dcp.tagjob.config.CliOptions;
import com.dcp.tagjob.config.TaskConfig;
import com.dcp.tagjob.plan.AssetPlan;
import com.dcp.tagjob.plan.BuildPlan;
import com.dcp.tagjob.run.MatrixAccumulator;
import com.dcp.tagjob.run.PartitionEvaluator;
import com.dcp.tagjob.spark.DatasetAssembly;
import com.dcp.tagjob.spark.IncrementalSnapshot;
import com.dcp.tagjob.spark.TagFactWriter;
import com.dcp.tagjob.util.Log;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * 打标引擎 P0 批执行器入口(独立 Spark 工程 tagengine-executor, 设计文档 2026-09-08 §12 细化)。
 *
 * 执行流水线(与后端 tag_task/tag_rule 结构对齐, task-json 为平台落盘的任务执行配置):
 *  1. 装配: 读数据集(湖表 spark.table / 联调 spark.read 抽象) → datasetFields∩真实 schema 校验(缺失→失败)
 *     → 派生字段(纯 expr / 聚合 func+field+windowDays groupBy join 回) → schema 真相分类 → Fact 字段表
 *  2. 规则编译(driver): 动态 Fact 按 schema 生成 Java 源码; 可视化/决策表→DRL、手写 DRL 直跑;
 *     Fact 源码 + 每资产 DRL 一并入 KieModule 由 KieBuilder 编译, driver 侧建一次 KieBase 做编译校验
 *     (错误明细 → 任务失败); Fact 源码 + DRL 文本 broadcast
 *  3. 逐分区求值: 每 executor 用同一 KieBaseFactory 路径本地重建 KieModule/KieBase(缓存一次);
 *     每分区 StatefulKieSession; Fact 类自 moduleClassLoader 加载; 行 → Fact;
 *     平铺规则(数组序, halt 跳余)→ 规则流(Java 控制流, 终止仅当前流); 行级 TagSink
 *  4. 写 tag_fact: 当前值语义 = task_id 分区删旧 + append(Iceberg time travel 留史)
 *  5. 试跑: --limit 采样 + --matrix 命中矩阵(日志/JSON)
 *
 * 退出码: 0=成功; 1=执行失败(异常/编译错); 2=参数/配置/结构错误。
 */
public final class TagBatchJob {

    private static final ObjectMapper OM = new ObjectMapper();

    private TagBatchJob() {
    }

    public static void main(String[] args) {
        System.exit(run(args));
    }

    static int run(String[] args) {
        CliOptions opts;
        try {
            opts = CliOptions.parse(args);
        } catch (IllegalArgumentException e) {
            System.err.println(e.getMessage());
            System.err.println(CliOptions.usage());
            return 2;
        }
        TaskConfig cfg = loadConfig(opts);
        if (cfg == null) return 2;

        if (opts.matrix && opts.limit < 0) {
            opts.limit = 1000;
            Log.info("[main] 试跑模式(--matrix)未给 --limit, 默认采样 1000 行");
        }
        Log.info("════════ 打标批任务启动 task=" + cfg.taskId + " v" + cfg.taskVer + " dataset=" + cfg.dataset
                + " entityKey=" + cfg.entityKey + " master=" + opts.master + " matrix=" + opts.matrix + " ════════");

        SparkSession spark = null;
        try {
            spark = buildSpark(cfg, opts);
            spark.sparkContext().setLogLevel("WARN");

            // ① 事实装配
            List<String> warnings = new ArrayList<>();
            Dataset<Row> base = DatasetAssembly.assemble(spark, cfg, warnings);
            warnings.forEach(w -> Log.warn("[装配] " + w));
            // 增量模式: 只保留变更行并收集实体集/游标(墓碑行不参与求值; 空变更 → 无事可做直接成功退出)
            IncrementalSnapshot incr = new IncrementalSnapshot();
            base = DatasetAssembly.applyIncremental(base, cfg, incr);
            if (incr.active && !incr.hasChanged()) {
                Log.info("════════ 打标批任务完成 task=" + cfg.taskId + "(增量: 无变更, 游标已最新) 退出码 0 ════════");
                return 0;
            }
            if (opts.limit > 0) {
                base = base.limit((int) opts.limit);
                Log.info("[装配] 采样行数: " + opts.limit);
            }
            List<BuildPlan.FactField> factFields = DatasetAssembly.classify(base.schema(), warnings);
            warnings.forEach(w -> Log.warn("[schema] " + w));
            Log.info("[装配] Fact 字段表(schema 真相): " + factFields.stream().map(f -> f.name + ":" + f.kind).toList());

            // ② driver 编译: 计划装配 + 动态 Fact 编译 + KieBase 编译校验(失败抛 IllegalArgumentException)
            BuildPlan plan = PlanAssembler.assemble(cfg, cfg.entityKey, factFields);
            Log.info("[编译] 计划就绪: 资产数=" + plan.assets.size()
                    + " 平铺规则=" + plan.flatOrder.length + " 规则流=" + plan.flowOrder.length
                    + " Fact=" + plan.factSpec.className + " 源码=" + plan.factSpec.javaSource.length() + " 字符");

            // ③ 广播 + 求值
            JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
            MatrixAccumulator matrix = new MatrixAccumulator();
            spark.sparkContext().register(matrix, "tagMatrix-task" + cfg.taskId);
            Broadcast<BuildPlan> bc = jsc.broadcast(plan);
            Timestamp runTs = new Timestamp(System.currentTimeMillis());

            Dataset<Row> outRows = base.mapPartitions(
                    new PartitionEvaluator(bc, matrix, cfg.taskId, runTs, !opts.matrix),
                    Encoders.row(outputSchema()));
            long written = -1;
            if (opts.matrix) {
                outRows.count();   // 触发求值(试跑不产出行; 命中聚合进 accumulator)
                Log.info("[求值] 试跑完成(未写 tag_fact)");
            } else {
                Dataset<Row> dedup = outRows.dropDuplicates("entity", "tag");   // (entity,tag) 当前值去重(冲突后写覆盖口径见 README)
                written = dedup.count();
                if (incr.active) {
                    // 增量写: 只覆盖本任务中变更实体(含墓碑清旧行), 不动其他实体(§14.2)
                    TagFactWriter.writeIncremental(dedup, spark, cfg, cfg.taskId, incr);
                } else {
                    TagFactWriter.write(dedup, spark, cfg, cfg.taskId);
                }
                Log.info("[tag_fact] 写行数=" + written + (incr.active ? "(增量轮)" : ""));
            }

            // ④ run 摘要 + 命中矩阵 + 游标输出(增量)
            Map<String, Long> m = new TreeMap<>(matrix.value());
            String summary = summaryText(cfg, plan, m, written, opts.matrix);
            Log.info("[摘要] " + summary);
            if (incr.active && !opts.matrix) {
                writeCursorOut(incr, opts);
            }
            if (opts.matrixOut != null) {
                writeMatrixFile(cfg, plan, m, opts.matrixOut);
            }
            Log.info("════════ 打标批任务完成 task=" + cfg.taskId + " 退出码 0 ════════");
            return 0;
        } catch (IllegalArgumentException e) {
            Log.error("任务失败(参数/编译/结构): " + e.getMessage());
            return 2;
        } catch (Throwable t) {
            Log.error("任务失败(执行异常): " + t);
            t.printStackTrace(System.err);
            return 1;
        } finally {
            if (spark != null) {
                spark.stop();
                Log.info("[main] SparkSession 已停止");
            }
        }
    }

    /* ---------- 配置加载 / SparkSession ---------- */

    private static TaskConfig loadConfig(CliOptions opts) {
        try {
            String json = Files.readString(Paths.get(opts.taskJsonPath), StandardCharsets.UTF_8);
            TaskConfig cfg = OM.readValue(json, TaskConfig.class);
            if (cfg.taskId <= 0) throw new IllegalArgumentException("taskId 必须 > 0");
            if (cfg.dataset == null || cfg.dataset.isBlank()) throw new IllegalArgumentException("dataset 必填");
            if (cfg.entityKey == null || cfg.entityKey.isBlank()) throw new IllegalArgumentException("entityKey 必填");
            if (cfg.bindings == null || cfg.bindings.isEmpty()) throw new IllegalArgumentException("bindings 至少一条绑定");
            if (cfg.rules == null || cfg.rules.isEmpty()) throw new IllegalArgumentException("rules 快照为空(task-json 缺资产定义)");
            opts.applyIcebergOverrides(cfg);
            Log.info("[main] task-json 加载: " + new File(opts.taskJsonPath).getAbsolutePath());
            return cfg;
        } catch (IllegalArgumentException e) {
            Log.error("任务配置错误: " + e.getMessage());
            return null;
        } catch (Exception e) {
            Log.error("task-json 解析失败: " + e.getMessage());
            return null;
        }
    }

    private static SparkSession buildSpark(TaskConfig cfg, CliOptions opts) {
        SparkSession.Builder b = SparkSession.builder()
                .appName("tagengine-batch-task-" + cfg.taskId + "-v" + cfg.taskVer)
                .master(opts.master)
                .config("spark.ui.enabled", "false")
                .config("spark.sql.session.timeZone", "Asia/Shanghai");
        // Iceberg catalog 装配(仅当任务给出 warehouse 时由本作业自带; 平台环境一般由 spark-submit conf 装配 —— 接湖契约见 DatasetAssembly)
        if (cfg.iceberg.catalog != null && !cfg.iceberg.catalog.isBlank()
                && cfg.iceberg.warehouse != null && !cfg.iceberg.warehouse.isBlank()) {
            b.config("spark.sql.catalog." + cfg.iceberg.catalog, "org.apache.iceberg.spark.SparkCatalog")
                    .config("spark.sql.catalog." + cfg.iceberg.catalog + ".type",
                            cfg.iceberg.catalogType == null ? "hadoop" : cfg.iceberg.catalogType)
                    .config("spark.sql.catalog." + cfg.iceberg.catalog + ".warehouse", cfg.iceberg.warehouse);
            Log.info("[main] 自动装配 Iceberg catalog: " + cfg.iceberg.catalog + " warehouse=" + cfg.iceberg.warehouse);
        }
        return b.getOrCreate();
    }

    /* ---------- 输出 schema / 摘要 ---------- */

    private static StructType outputSchema() {
        return new StructType()
                .add("task_id", DataTypes.LongType, false)
                .add("entity", DataTypes.StringType, false)
                .add("tag", DataTypes.StringType, false)
                .add("value", DataTypes.StringType, true)
                .add("run_ts", DataTypes.TimestampType, false);
    }

    private static String summaryText(TaskConfig cfg, BuildPlan plan, Map<String, Long> m,
                                      long written, boolean matrix) {
        StringBuilder sb = new StringBuilder();
        sb.append("run 摘要 task=").append(cfg.taskId).append("v").append(cfg.taskVer)
                .append(" 总行数=").append(m.getOrDefault("rows", 0L))
                .append(" 实体键为空行=").append(m.getOrDefault("nullEntity", 0L));
        if (matrix) {
            sb.append(" (试跑: 未写 tag_fact)");
        } else {
            sb.append(" 写表行数=").append(written);
        }
        sb.append(" 每资产命中: [");
        boolean first = true;
        for (int i = 0; i < plan.assets.size(); i++) {
            AssetPlan p = plan.assets.get(i);
            long hit = m.getOrDefault("hit:" + i, 0L);
            long tag = m.getOrDefault("tag:" + i, 0L);
            if (hit == 0 && tag == 0) continue;
            sb.append(first ? "" : ", ").append(p.name).append("(").append(p.kind).append("#").append(p.id)
                    .append("): 命中行=").append(hit).append(" 标签行=").append(tag);
            first = false;
        }
        return sb.append(']').toString();
    }

    /** 游标输出(增量轮; 平台 exit0 后读文件回写 tag_task.cursor_json, 同 lakesync cursor-out 模式) */
    private static void writeCursorOut(IncrementalSnapshot incr, CliOptions opts) {
        if (opts.cursorOut == null || incr.seqColumn == null) {
            Log.info("[游标] 未配置 --cursor-out 或无量列, 游标推进=" + incr.maxSeq + "(seq=" + incr.seqColumn + ")");
            return;
        }
        try {
            ObjectNode o = OM.createObjectNode();
            o.put("seqColumn", incr.seqColumn);
            o.put("maxSeq", incr.maxSeq);
            Files.writeString(Paths.get(opts.cursorOut), OM.writerWithDefaultPrettyPrinter().writeValueAsString(o),
                    StandardCharsets.UTF_8);
            Log.info("[游标] 结果已写: " + opts.cursorOut + " = " + o);
        } catch (Exception e) {
            Log.error("[游标] 写文件失败: " + e.getMessage());
        }
    }

    private static void writeMatrixFile(TaskConfig cfg, BuildPlan plan, Map<String, Long> m, String path) {
        try {
            StringBuilder sb = new StringBuilder("{\n  \"taskId\": ").append(cfg.taskId)
                    .append(",\n  \"taskVer\": ").append(cfg.taskVer)
                    .append(",\n  \"totalRows\": ").append(m.getOrDefault("rows", 0L))
                    .append(",\n  \"nullEntityRows\": ").append(m.getOrDefault("nullEntity", 0L))
                    .append(",\n  \"assets\": [\n");
            for (int i = 0; i < plan.assets.size(); i++) {
                AssetPlan p = plan.assets.get(i);
                sb.append("    {\"idx\": ").append(i)
                        .append(", \"id\": ").append(p.id)
                        .append(", \"kind\": \"").append(p.kind)
                        .append("\", \"name\": \"").append(esc(p.name))
                        .append("\", \"ver\": ").append(p.ver)
                        .append(", \"hitRows\": ").append(m.getOrDefault("hit:" + i, 0L))
                        .append(", \"tagRows\": ").append(m.getOrDefault("tag:" + i, 0L))
                        .append("}").append(i == plan.assets.size() - 1 ? "\n" : ",\n");
            }
            sb.append("  ]\n}\n");
            Files.writeString(Paths.get(path), sb.toString(), StandardCharsets.UTF_8);
            Log.info("[matrix] 命中矩阵已写: " + path);
        } catch (Exception e) {
            Log.error("[matrix] 写矩阵文件失败: " + e.getMessage());
        }
    }

    private static String esc(String s) {
        return s.replace("\\", "\\\\").replace("\"", "\\\"");
    }
}
