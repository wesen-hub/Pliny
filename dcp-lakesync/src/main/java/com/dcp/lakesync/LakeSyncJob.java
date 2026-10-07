package com.dcp.lakesync;

import com.dcp.lakesync.config.LakeJobConfig;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.max;

/**
 * 湖同步周期批作业(设计文档 2026-09-08 §3 整行镜像/§4b):
 *  读数据面单表增量(任一源 _seq_{src} > 游标) → 湖表缺列补列(schema evolution)
 *  → MERGE INTO 湖 ON 业务键(整行覆盖: 数据面行=消费端合并后的当前权威态, 全列抄不回退)
 *  → 输出 cursor-result.json(各源本轮 max seq, 平台 exit0 后回写 logical_sync_progress)。
 *
 * 退出码: 0=成功; 1=执行异常; 2=参数/配置错误。
 */
public final class LakeSyncJob {

    private static final ObjectMapper OM = new ObjectMapper();

    private LakeSyncJob() {
    }

    public static void main(String[] args) {
        System.exit(run(args));
    }

    static int run(String[] args) {
        String jobJson = null;
        String cursorOut = null;
        String master = System.getenv().getOrDefault("SPARK_MASTER", "local[*]");
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--job-json" -> jobJson = req(args, ++i, "--job-json");
                case "--cursor-out" -> cursorOut = req(args, ++i, "--cursor-out");
                case "--master" -> master = req(args, ++i, "--master");
                default -> {
                    System.err.println("未知参数: " + args[i]);
                    return 2;
                }
            }
        }
        if (jobJson == null) {
            System.err.println("缺少必填参数 --job-json <file>");
            return 2;
        }
        LakeJobConfig cfg;
        try {
            cfg = LakeJobConfig.load(jobJson);
        } catch (IllegalArgumentException e) {
            System.err.println("任务配置错误: " + e.getMessage());
            return 2;
        }
        log("═══ 湖同步启动 logicalTable=" + cfg.logicalTable + " 数据面表=" + cfg.dataSource.table
                + " → 湖=" + cfg.lake.catalog + "." + cfg.lake.namespace + "." + cfg.lake.table
                + " 模式=" + (cfg.pullPredicate() == null ? "全量(首载)" : "增量") + " ═══");

        SparkSession spark = null;
        try {
            spark = buildSpark(cfg, master);
            spark.sparkContext().setLogLevel("WARN");

            // ① 读数据面增量(谓词下推; 全表 = 首载)
            Dataset<Row> staged = readSource(spark, cfg);
            long pulled = staged.count();
            log("[源] 拉取行数=" + pulled + (cfg.pullPredicate() == null ? "(首载全量)" : "(增量)"));

            // ② 湖: 缺列补列 → MERGE(整行镜像)
            String lakeId = qid(cfg.lake.catalog, cfg.lake.namespace, cfg.lake.table);
            if (!tableExists(spark, lakeId)) {
                log("[湖] 表不存在, 以源 schema 建表: " + lakeId);
                staged.writeTo(lakeId).create();
            } else {
                ensureColumns(spark, cfg, lakeId, staged.schema());
                mergeInto(spark, cfg, lakeId, staged);
            }
            spark.catalog().refreshTable(lakeId.replace("`", ""));
            long lakeRows = spark.table(lakeId).count();
            log("[湖] 本轮完成后总行数=" + lakeRows);

            // ③ 游标结果(平台 exit0 后回写)
            Map<String, Long> result = cursorResult(staged, cfg);
            if (cursorOut != null) {
                Files.writeString(Paths.get(cursorOut), OM.writeValueAsString(result), StandardCharsets.UTF_8);
                log("[游标] 结果已写: " + cursorOut + " = " + result);
            } else {
                log("[游标] " + result);
            }
            log("═══ 湖同步完成 logicalTable=" + cfg.logicalTable + " 拉取=" + pulled + " 湖行=" + lakeRows + " 退出码 0 ═══");
            return 0;
        } catch (IllegalArgumentException e) {
            log("任务失败(配置/结构): " + e.getMessage());
            return 2;
        } catch (Throwable t) {
            log("任务失败(执行异常): " + t);
            t.printStackTrace(System.err);
            return 1;
        } finally {
            if (spark != null) {
                spark.stop();
            }
        }
    }

    /* ---------- Spark/装配 ---------- */

    private static SparkSession buildSpark(LakeJobConfig cfg, String master) {
        LakeJobConfig.Lake L = cfg.lake;
        SparkSession.Builder b = SparkSession.builder()
                .appName("lakesync-" + cfg.logicalTable)
                .master(master)
                .config("spark.ui.enabled", "false")
                .config("spark.sql.session.timeZone", "Asia/Shanghai")
                .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions");
        String p = "spark.sql.catalog." + L.catalog;
        b.config(p, "org.apache.iceberg.spark.SparkCatalog")
                .config(p + ".type", L.type)
                .config(p + ".uri", L.uri);
        if (L.warehouse != null && !L.warehouse.isBlank()) b.config(p + ".warehouse", L.warehouse);
        if (L.token != null && !L.token.isBlank()) b.config(p + ".token", L.token);
        if (L.credential != null && !L.credential.isBlank()) {
            b.config(p + ".credential", L.credential);
            if (L.scope != null && !L.scope.isBlank()) b.config(p + ".scope", L.scope);
            if (L.oauth2ServerUri != null && !L.oauth2ServerUri.isBlank()) {
                b.config(p + ".oauth2-server-uri", L.oauth2ServerUri);
            }
            b.config(p + ".header.X-Iceberg-Access-Delegation", "vended-credentials");
        }
        if (L.ioImpl != null && !L.ioImpl.isBlank()) b.config(p + ".io-impl", L.ioImpl);
        if (L.s3 != null) {
            Map<String, String> map = Map.of(
                    "endpoint", "s3.endpoint",
                    "pathStyleAccess", "s3.path-style-access",
                    "accessKey", "s3.access-key-id",
                    "secretKey", "s3.secret-access-key",
                    // iceberg-aws 1.7.1 起 region 键从 s3.region 迁到 AwsClientProperties.CLIENT_REGION(client.region)
                    "region", "client.region");
            L.s3.forEach((k, v) -> {
                String prop = map.getOrDefault(k, "s3." + k);
                if (v != null) b.config(p + "." + prop, v);
            });
        }
        return b.getOrCreate();
    }

    /** 读数据面单表; 有谓词则子查询下推(真下推到 MySQL/TiDB), 否则全表(首载) */
    private static Dataset<Row> readSource(SparkSession spark, LakeJobConfig cfg) {
        Map<String, String> opts = new HashMap<>();
        opts.put("url", cfg.dataSource.url);
        opts.put("user", cfg.dataSource.user);
        opts.put("password", cfg.dataSource.password);
        opts.put("driver", "com.mysql.cj.jdbc.Driver");
        String pred = cfg.pullPredicate();
        if (pred == null) {
            opts.put("dbtable", "`" + cfg.dataSource.table + "`");
        } else {
            // 下推: 库内先过滤, 只把增量行拉回(避免全表到 Spark)
            opts.put("dbtable", "(SELECT * FROM `" + cfg.dataSource.table + "` WHERE " + pred + ") AS __lake_src");
        }
        Dataset<Row> df = spark.read().format("jdbc").options(opts).load();
        // 去掉数据面内部自增列(湖不需要), 其余整行镜像(业务键+字段+各源 seq+墓碑透传)
        return df.columns().length == 0 ? df : df.drop("_pk_id");
    }

    /** 湖表缺列 → ALTER ADD(schema evolution; 与消费端 ensureColumns 同向) */
    private static void ensureColumns(SparkSession spark, LakeJobConfig cfg, String lakeId, StructType stagedSchema) {
        Set<String> existing = new LinkedHashSet<>(Arrays.asList(spark.table(lakeId).columns()));
        for (StructField f : stagedSchema.fields()) {
            if (existing.contains(f.name())) continue;
            String sql = "ALTER TABLE " + lakeId + " ADD COLUMN `" + f.name() + "` "
                    + sparkTypeSql(f.dataType());
            log("[湖] schema evolution 补列: " + sql);
            spark.sql(sql);
        }
    }

    /** 整行镜像 MERGE: ON 业务键; MATCHED → UPDATE 全非键列; NOT MATCHED → INSERT */
    private static void mergeInto(SparkSession spark, LakeJobConfig cfg, String lakeId, Dataset<Row> staged) {
        String view = "lakesync_staged";
        staged.createOrReplaceTempView(view);
        List<String> allCols = Arrays.asList(staged.columns());
        Set<String> keys = new LinkedHashSet<>(cfg.entityKeyColumns);
        List<String> keyCols = allCols.stream().filter(keys::contains).toList();
        if (keyCols.isEmpty()) {
            throw new IllegalArgumentException("entityKeyColumns 在源表中不存在: " + cfg.entityKeyColumns
                    + " 实际列=" + allCols);
        }
        List<String> valueCols = allCols.stream().filter(c -> !keys.contains(c)).toList();
        String on = keyCols.stream().map(c -> "T.`" + c + "` = S.`" + c + "`")
                .collect(Collectors.joining(" AND "));
        String set = valueCols.stream().map(c -> "T.`" + c + "` = S.`" + c + "`")
                .collect(Collectors.joining(", "));
        String insCols = allCols.stream().map(c -> "`" + c + "`").collect(Collectors.joining(", "));
        String insVals = allCols.stream().map(c -> "S.`" + c + "`").collect(Collectors.joining(", "));
        String sql = "MERGE INTO " + lakeId + " AS T USING " + view + " AS S ON " + on
                + " WHEN MATCHED THEN UPDATE SET " + set
                + " WHEN NOT MATCHED THEN INSERT (" + insCols + ") VALUES (" + insVals + ")";
        log("[湖] 执行 MERGE(整行镜像)");
        spark.sql(sql);
        spark.catalog().dropTempView(view);
    }

    /** 游标结果: 每源 seq 列本轮 max(空结果集回退初值, 防误推进) */
    private static Map<String, Long> cursorResult(Dataset<Row> staged, LakeJobConfig cfg) {
        Map<String, Long> out = new LinkedHashMap<>();
        Set<String> cols = new LinkedHashSet<>(Arrays.asList(staged.columns()));
        for (LakeJobConfig.SeqColumn sc : cfg.seqColumns) {
            if (!cols.contains(sc.name)) {
                out.put(sc.name, sc.cursor);
                continue;
            }
            List<Row> rows = staged.select(max(col(sc.name)).alias("m")).collectAsList();
            Long m = rows.isEmpty() || rows.get(0).isNullAt(0) ? null : rows.get(0).getLong(0);
            out.put(sc.name, m == null ? Math.max(sc.cursor, 0) : Math.max(m, sc.cursor));
        }
        return out;
    }

    /* ---------- 工具 ---------- */

    private static boolean tableExists(SparkSession spark, String table) {
        try {
            return spark.catalog().tableExists(table.replace("`", ""));
        } catch (Exception e) {
            return false;
        }
    }

    private static String qid(String a, String b, String c) {
        return "`" + a + "`.`" + b + "`.`" + c + "`";
    }

    private static String sparkTypeSql(org.apache.spark.sql.types.DataType t) {
        return t.simpleString().toUpperCase(Locale.ROOT);
    }

    private static String req(String[] args, int i, String flag) {
        if (i >= args.length) throw new IllegalArgumentException(flag + " 需要值");
        return args[i];
    }

    private static void log(String s) {
        System.out.println("[lakesync] " + s);
    }
}
