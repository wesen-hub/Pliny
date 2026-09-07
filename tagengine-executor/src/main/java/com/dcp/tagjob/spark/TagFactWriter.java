package com.dcp.tagjob.spark;

import com.dcp.tagjob.config.TaskConfig;
import com.dcp.tagjob.util.Log;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * tag_fact 写层(设计文档 §6/§11⑦: 当前值语义 —— 按 task_id 分区, 每轮 = 删本任务旧分区 + append;
 * 历史保留靠 Iceberg time travel, 不做行级 valid_to)。
 *
 * 实现(§12 细化):
 *  表不存在 → writeTo.create(partitioned by task_id); 存在 → DELETE FROM … WHERE task_id=…(iceberg 支持)
 *  → append; 非事务湖/不支持 DELETE 时回退 全量过滤重写(只动本任务旧分区行的等价结果)。
 * 跨任务同 tag 冲突由后端发布校验拦截(§11⑧), 执行器按执行序后写覆盖并依赖分区隔离。
 */
public final class TagFactWriter {

    private TagFactWriter() {
    }

    /** 写 tag_fact; tagRows 列序: task_id, entity, tag, value, run_ts(由 PartitionEvaluator 输出) */
    public static void write(Dataset<Row> tagRows, SparkSession spark, TaskConfig cfg, long taskId) {
        if (cfg.tagFact.table == null || cfg.tagFact.table.isBlank()) {
            throw new IllegalArgumentException("未配置 tag_fact 写目标(tagFact.table)");
        }
        String tbl = cfg.tagFact.qualified();
        boolean partitioned = cfg.tagFact.partitionByTask;
        boolean exists = DatasetAssembly.tableExists(spark, tbl);
        boolean dataWritten = false;
        if (exists) {
            if (partitioned) {
                deleteOldPartition(spark, tbl, taskId);
            } else {
                Log.warn("[tag_fact] partitionByTask=false: 不做旧分区清理, 直接追加(当前值语义不保证, 请确认配置)");
            }
        } else {
            dataWritten = createTable(tagRows, spark, tbl, partitioned);
        }
        if (!dataWritten) {
            append(tagRows, spark, tbl);
        }
        Log.info("[tag_fact] 写入完成: " + tbl + " 行数=" + tagRows.count());
    }

    /** 建表并写初值; 返回 true 表示数据已随建表写入(无需再 append) */
    private static boolean createTable(Dataset<Row> rows, SparkSession spark, String tbl, boolean partitioned) {
        try {
            if (partitioned) {
                rows.writeTo(tbl).partitionedBy(org.apache.spark.sql.functions.col("task_id")).create();
            } else {
                rows.writeTo(tbl).create();
            }
            Log.info("[tag_fact] 表不存在, writeTo.create 建表+写初值完成: " + tbl + " partitionByTask=" + partitioned);
            return true;
        } catch (Exception e) {
            Log.warn("[tag_fact] writeTo.create 失败(" + e.getMessage() + "), 回退 SQL CTAS(USING PARQUET, 联调/非湖 catalog 兜底; 湖环境走 iceberg)");
        }
        // 回退: 通用 SQL 建分区表(iceberg 环境正常走上方 writeTo)
        String cols = "task_id BIGINT, entity STRING, tag STRING, value STRING, run_ts TIMESTAMP";
        String sql = partitioned
                ? "CREATE TABLE " + qid(tbl) + " (" + cols + ") USING PARQUET PARTITIONED BY (task_id)"
                : "CREATE TABLE " + qid(tbl) + " (" + cols + ") USING PARQUET";
        spark.sql(sql);
        return false;
    }

    /** 删本任务旧分区(iceberg DELETE; 不支持 → 全量过滤重写兜底) */
    private static void deleteOldPartition(SparkSession spark, String tbl, long taskId) {
        try {
            spark.sql("DELETE FROM " + qid(tbl) + " WHERE task_id = " + taskId);
            Log.info("[tag_fact] 已删除本任务旧分区(task_id=" + taskId + "), 历史留史依赖湖 time travel");
            return;
        } catch (Exception e) {
            Log.warn("[tag_fact] DELETE 语句不支持/失败(" + e.getMessage() + "), 回退过滤重写(保留其他任务分区)");
        }
        // 兜底: 全表过滤重写 = 旧数据(剔除本任务) —— 仅用于非事务表, 等价于删本任务旧分区
        Dataset<Row> rest = spark.table(tbl).filter("task_id != " + taskId);
        rest.write().mode("overwrite").insertInto(tbl);
        Log.warn("[tag_fact] 已按过滤重写完成(task_id=" + taskId + ")");
    }

    private static void append(Dataset<Row> rows, SparkSession spark, String tbl) {
        try {
            rows.writeTo(tbl).append();
        } catch (Exception e) {
            Log.warn("[tag_fact] writeTo.append 失败(" + e.getMessage() + "), 回退 insertInto(mode=append)");
            rows.write().mode("append").insertInto(tbl);
        }
    }

    /** 多段标识符 backtick 引用: demo.dcp.tag_fact → `demo`.`dcp`.`tag_fact` */
    private static String qid(String table) {
        return "`" + table.replace(".", "`.`") + "`";
    }
}
