package com.dcp.tagjob.spark;

import com.dcp.tagjob.config.TaskConfig;
import com.dcp.tagjob.plan.BuildPlan;
import com.dcp.tagjob.util.Log;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.BooleanType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DateType;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.DoubleType;
import org.apache.spark.sql.types.FloatType;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.LongType;
import org.apache.spark.sql.types.ShortType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.TimestampType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * 事实装配(driver 侧; 设计文档 §4/§12):
 *  读数据集(Iceberg 湖表/逻辑表名/DataSource 抽象, 见 readDataset) → datasetFields 与真实 schema 交集校验
 *  → 派生字段: (a) 纯表达式 expr(spark.sql expr()) (b) 聚合派生 {func,field,windowDays} 实体级 groupBy join 回
 *  → schema 真相分类 → Fact 字段表(字段类型真相 = 装配后 df.schema, 见 §12)。
 */
public final class DatasetAssembly {

    private DatasetAssembly() {
    }

    /** 装配主流程; 结构/字段错误抛 IllegalArgumentException(中文明细) */
    public static Dataset<Row> assemble(SparkSession spark, TaskConfig cfg, List<String> warnings) {
        Dataset<Row> df = readDataset(spark, cfg);

        // datasetFields 与真实 schema 交集: 声明缺失 → 任务失败(设计: 发布校验已做依赖域, 执行器按真实 schema 复查)
        Set<String> cols = new LinkedHashSet<>(Arrays.asList(df.columns()));
        List<String> missing = new ArrayList<>();
        for (TaskConfig.FieldDef f : cfg.datasetFields) {
            if (f.name == null || f.name.isEmpty()) continue;
            if (!cols.contains(f.name)) missing.add(f.name);
        }
        if (!missing.isEmpty()) {
            throw new IllegalArgumentException("datasetFields 声明字段不在真实表中(交集校验失败), 缺失: " + missing
                    + " —— 数据集逻辑表可能已变更, 请回后端刷新字段清单");
        }
        if (cfg.entityKey == null || cfg.entityKey.isBlank() || !cols.contains(cfg.entityKey)) {
            throw new IllegalArgumentException("实体键 entityKey 缺失或不在真实表中: " + cfg.entityKey);
        }

        // 派生字段(装配期; §4: 规则只声明字段依赖, 算在任务装配完成)
        for (TaskConfig.DerivedDef d : cfg.derived) {
            if (d.name == null || d.name.isBlank()) {
                throw new IllegalArgumentException("派生字段缺 name");
            }
            if (!isJavaIdentifier(d.name)) {
                throw new IllegalArgumentException("派生字段名须为合法标识符(动态 Fact 生成要求): " + d.name);
            }
            if (Arrays.asList(df.columns()).contains(d.name)) {
                throw new IllegalArgumentException("派生字段名与既有列冲突: " + d.name);
            }
            if (d.isAgg()) {
                df = aggDerived(spark, df, cfg.entityKey, d);
            } else if (d.isExpr()) {
                try {
                    df = df.withColumn(d.name, functions.expr(d.expr));
                } catch (RuntimeException e) {
                    // 表达式非法/引用列不存在: withColumn 惰性, 此处一般到 action 才抛; 捕获做中文化提示
                    throw new IllegalArgumentException("派生表达式失败: " + d.name + " = " + d.expr
                            + " —— " + cleanSparkMsg(e.getMessage()), e);
                }
            } else {
                throw new IllegalArgumentException("派生字段 " + d.name + " 缺定义: 须提供 expr(纯表达式) 或 func+field(聚合)");
            }
            Log.info("[装配] 派生字段完成: " + d.name);
        }
        return df;
    }

    /**
     * 读数据集: 湖表(与 Iceberg 对接用 spark.table/catalog 读取, §11④/§12)或联调 DataSource 抽象。
     * 不直接 import iceberg 类 —— 运行时 provider(iceberg-spark-runtime)由 spark-submit --packages/--jars 提供;
     * 接湖切换只发生在运行时 catalog 装配, 本模块无 iceberg API 依赖点(编译期已具备, 见 pom provided)。
     */
    public static Dataset<Row> readDataset(SparkSession spark, TaskConfig cfg) {
        String q = cfg.iceberg.isBlank() ? "" : cfg.iceberg.qualified();
        Log.info("[装配] 读数据集: iceberg=" + q + " dataset=" + cfg.dataset);
        if (!q.isEmpty() && tableExists(spark, q)) {
            return spark.table(q);
        }
        if (cfg.dataset != null && !cfg.dataset.isBlank() && tableExists(spark, cfg.dataset)) {
            return spark.table(cfg.dataset);
        }
        // DataSource 抽象兜底(联调: 湖表未落地/未注册 catalog 时允许 spark.read 直接读文件路径)
        if (!q.isEmpty() && (q.startsWith("/") || q.startsWith("file:") || q.startsWith("s3a:") || q.startsWith("hdfs:"))) {
            Log.warn("[装配] 表不存在, 按 DataSource 路径读(联调兜底): " + q);
            return spark.read().load(q);
        }
        throw new IllegalArgumentException("数据集不可读: iceberg=" + q + " dataset=" + cfg.dataset
                + "\n  —— 湖表需先落地并注册 catalog(spark-submit 装配 iceberg), 或本地联调将 iceberg.table 配为文件路径(spark.read 兜底)");
    }

    /** 聚合派生: 实体级 groupBy(func(field)) [可选: 先按 windowColumn >= now-windowDays 过滤] → join 回原表 */
    private static Dataset<Row> aggDerived(SparkSession spark, Dataset<Row> df, String entityKey,
                                           TaskConfig.DerivedDef d) {
        String func = d.func.toUpperCase();
        if (!Set.of("SUM", "COUNT", "AVG", "MIN", "MAX").contains(func)) {
            throw new IllegalArgumentException("聚合函数不支持: " + d.func + "(SUM/COUNT/AVG/MIN/MAX)");
        }
        List<String> cols = Arrays.asList(df.columns());
        if (!cols.contains(d.field)) {
            throw new IllegalArgumentException("聚合派生 " + d.name + " 引用字段不在表中: " + d.field);
        }
        Dataset<Row> src = df;
        if (d.windowDays > 0 && d.windowColumn != null && !d.windowColumn.isBlank()) {
            if (!cols.contains(d.windowColumn)) {
                Log.warn("[装配] 聚合派生 " + d.name + " 的 windowColumn=" + d.windowColumn + " 不在表中, 跳过时间窗过滤(全表聚合)");
            } else {
                // TODO(时间窗细化): 本期简化用 now-windowDays 过滤; 跨天/水印窗口回看 E2E 期细化(设计 §4 流式语义不同)
                src = src.filter(functions.col(d.windowColumn).geq(
                        functions.date_sub(functions.current_date(), d.windowDays)));
                Log.info("[装配] 聚合派生 " + d.name + " 时间窗过滤: " + d.windowColumn + " >= now-" + d.windowDays + "d");
            }
        }
        Column aggExpr = switch (func) {
            case "SUM" -> functions.sum(functions.col(d.field));
            case "COUNT" -> functions.count(functions.col(d.field));
            case "AVG" -> functions.avg(functions.col(d.field));
            case "MIN" -> functions.min(functions.col(d.field));
            default -> functions.max(functions.col(d.field));
        };
        String keyAlias = "__aggkey_" + d.name;
        String valAlias = "__aggval_" + d.name;
        Dataset<Row> g = src.groupBy(functions.col(entityKey).alias(keyAlias))
                .agg(aggExpr.alias(valAlias));
        return df.join(g, functions.col(entityKey).equalTo(functions.col(keyAlias)), "left")
                .drop(keyAlias)
                .withColumnRenamed(valAlias, d.name);
    }

    /** 装配后 schema 真相分类 → Fact 字段表(§12: 用 schema 实际类型, 不猜类型) */
    public static List<BuildPlan.FactField> classify(StructType schema, List<String> warnings) {
        List<BuildPlan.FactField> fields = new ArrayList<>();
        for (StructField sf : schema.fields()) {
            String name = sf.name();
            if (!isJavaIdentifier(name)) {
                warnings.add("列名非 Java 标识符, 不入 Fact(规则不可引用): " + name);
                continue;
            }
            char kind = kindOf(sf.dataType());
            if (kind == 0) {
                warnings.add("列类型不支持, 不入 Fact(规则不可引用): " + name + ":" + sf.dataType().typeName());
                continue;
            }
            fields.add(new BuildPlan.FactField(name, kind));
        }
        return fields;
    }

    /** Spark 类型 → Fact 值分类; 0 = 不支持 */
    public static char kindOf(DataType dt) {
        if (dt instanceof LongType || dt instanceof IntegerType || dt instanceof ShortType
                || dt instanceof org.apache.spark.sql.types.ByteType) {
            return 'L';
        }
        if (dt instanceof DoubleType || dt instanceof FloatType || dt instanceof DecimalType) {
            return 'D';   // decimal 精度降级 double(打标档位阈值场景够用; 精确口径 TODO)
        }
        if (dt instanceof BooleanType) return 'B';
        if (dt instanceof StringType) return 'S';
        if (dt instanceof DateType || dt instanceof TimestampType) return 'S';   // 序列化字符串(ISO 可比/含)
        return 0;   // binary/array/map/struct/null 等
    }

    public static boolean isJavaIdentifier(String name) {
        return name != null && !name.isEmpty() && name.matches("[A-Za-z_$][A-Za-z0-9_$]*");
    }

    public static boolean tableExists(SparkSession spark, String table) {
        try {
            return spark.catalog().tableExists(table);
        } catch (Exception e) {
            return false;
        }
    }

    private static String cleanSparkMsg(String m) {
        if (m == null) return "";
        String[] lines = m.split("\n");
        StringBuilder sb = new StringBuilder();
        for (String l : lines) {
            if (l.isBlank() || l.contains("== SQL ==") || l.contains("===") || l.contains("^^^")) continue;
            sb.append(l.trim()).append("; ");
        }
        String out = sb.toString();
        return out.length() > 500 ? out.substring(0, 500) + "…" : out;
    }
}
