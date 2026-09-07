package com.dcp.tagjob.config;

import com.fasterxml.jackson.databind.JsonNode;

/** 命令行/环境覆盖选项解析(平台触发用 --task-json; 联调可 --master/--limit/--matrix 等) */
public class CliOptions {
    /** task-json 文件路径(平台侧落盘的任务执行配置) */
    public String taskJsonPath;
    /** spark.master 覆盖, 默认 local[*] 用于联调 */
    public String master = "local[*]";
    /** 读数据集 iceberg 三元组覆盖 */
    public String icebergCatalog;
    public String icebergNamespace;
    public String icebergTable;
    /** tag_fact 写目标覆盖 */
    public String factCatalog;
    public String factNamespace;
    public String factTable;
    /** 采样行数(试跑) */
    public long limit = -1;
    /** 试跑模式: 采样求值 + 命中矩阵输出, 不写 tag_fact */
    public boolean matrix = false;
    /** 命中矩阵 JSON 输出文件(可空: 仅日志) */
    public String matrixOut;

    public static CliOptions parse(String[] args) {
        CliOptions o = new CliOptions();
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--task-json" -> o.taskJsonPath = req(args, ++i, "--task-json");
                case "--master" -> o.master = req(args, ++i, "--master");
                case "--iceberg-catalog" -> o.icebergCatalog = req(args, ++i, "--iceberg-catalog");
                case "--iceberg-namespace" -> o.icebergNamespace = req(args, ++i, "--iceberg-namespace");
                case "--iceberg-table" -> o.icebergTable = req(args, ++i, "--iceberg-table");
                case "--fact-catalog" -> o.factCatalog = req(args, ++i, "--fact-catalog");
                case "--fact-namespace" -> o.factNamespace = req(args, ++i, "--fact-namespace");
                case "--fact-table" -> o.factTable = req(args, ++i, "--fact-table");
                case "--limit" -> o.limit = Long.parseLong(req(args, ++i, "--limit"));
                case "--matrix" -> o.matrix = true;
                case "--matrix-out" -> o.matrixOut = req(args, ++i, "--matrix-out");
                default -> throw new IllegalArgumentException("未知参数: " + args[i]);
            }
        }
        if (o.taskJsonPath == null) {
            throw new IllegalArgumentException("缺少必填参数 --task-json <文件路径>(平台侧落盘的任务执行配置 JSON)");
        }
        /* 环境变量覆盖(设计: --master/--iceberg-* 环境变量覆盖 spark.master 默认 local[*]) */
        if (o.master == null) o.master = env("SPARK_MASTER", "local[*]");
        if (o.icebergCatalog == null) o.icebergCatalog = env("ICEBERG_CATALOG", null);
        if (o.icebergNamespace == null) o.icebergNamespace = env("ICEBERG_NAMESPACE", null);
        if (o.icebergTable == null) o.icebergTable = env("ICEBERG_TABLE", null);
        return o;
    }

    /** 覆盖应用到任务配置(读数据集三元组) */
    public void applyIcebergOverrides(TaskConfig cfg) {
        if (icebergCatalog != null) cfg.iceberg.catalog = icebergCatalog;
        if (icebergNamespace != null) cfg.iceberg.namespace = icebergNamespace;
        if (icebergTable != null) cfg.iceberg.table = icebergTable;
        if (factCatalog != null) cfg.tagFact.catalog = factCatalog;
        if (factNamespace != null) cfg.tagFact.namespace = factNamespace;
        if (factTable != null) cfg.tagFact.table = factTable;
    }

    private static String req(String[] args, int i, String flag) {
        if (i >= args.length) throw new IllegalArgumentException(flag + " 需要值");
        return args[i];
    }

    private static String env(String key, String dft) {
        String v = System.getenv(key);
        return (v == null || v.isBlank()) ? dft : v;
    }

    public static String usage() {
        return """
                用法: java -jar tagengine-executor.jar --task-json <path> [选项]
                  必填:
                    --task-json <path>        任务执行配置 JSON(平台落盘, 结构见 TaskConfig)
                  可选(联调/试跑):
                    --master <url>            spark.master, 默认 local[*](环境变量 SPARK_MASTER)
                    --iceberg-catalog/--iceberg-namespace/--iceberg-table  读数据集湖表覆盖
                    --fact-catalog/--fact-namespace/--fact-table           tag_fact 写目标覆盖
                    --limit N                 采样前 N 行(试跑)
                    --matrix                  试跑模式: 只求值出命中矩阵, 不写 tag_fact
                    --matrix-out <file>       命中矩阵 JSON 输出文件(可选, 默认仅日志)
                """;
    }

    /** 附加输入(供 JSON 解析后再次结构性校验用, 避免各调用点重复 import) */
    public static void requireTaskOk(JsonNode root) {
        // 占位: 详细校验在 TagBatchJob.loadConfig
    }
}
