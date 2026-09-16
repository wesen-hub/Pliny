package com.dcp.tagjob.spark;

import com.dcp.tagjob.config.TaskConfig;
import com.dcp.tagjob.util.Log;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.BooleanType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DoubleType;
import org.apache.spark.sql.types.LongType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * 快照源输入(方案 C): 执行器从**平台既有快照 API** 分页拉 AS OF T0 的行, 替换读湖。
 *
 * <p>为什么走平台 API 而不是自己拼 `AS OF TIMESTAMP`: 后者靠墙钟换算会引入毫秒级偏差,
 * 可能造成批/流之间的空洞;平台快照接口已精确支持 {@code boundaryTso} 与分页,
 * 且行已被契约裁列/脱敏(与增量侧同视角)。
 *
 * <p>接口: {@code GET {baseUrl}/api/chg/{stream}/snapshot?boundaryTso=T0&limit=&pageToken=&consumerId=}
 * 响应: ApiResponse 包装 {@code {data:{rows:[{...}], pageToken, done}}}。
 * 分页在 driver 侧完成, 攒齐后 {@code spark.createDataFrame} 构 DataFrame ——
 * 下游(字段域交集校验/派生/求值)与读湖路径完全同构。
 */
public final class SnapshotInput {

    private static final ObjectMapper OM = new ObjectMapper();
    /** 防御: 分页 token 异常时不死循环 */
    private static final int MAX_PAGES = 100_000;

    private SnapshotInput() {
    }

    /**
     * 快照源入口。约束(fail-closed, 不静默降级回读湖):
     * <ul>
     *   <li>task-json 必须带 {@code t0 > 0}(控制面 T0Selector 下发);</li>
     *   <li>与增量游标互斥 —— 批阶段快照一次性, 不携带 _seq 增量列。</li>
     * </ul>
     */
    public static Dataset<Row> read(SparkSession spark, TaskConfig cfg, List<String> warnings) {
        TaskConfig.SnapshotRef snap = cfg.snapshot;
        if (cfg.t0 <= 0) {
            throw new IllegalArgumentException("快照源需控制面下发 T0(task-json 缺 t0 或 t0<=0); 读湖模式请不要配 snapshot.baseUrl");
        }
        if (cfg.incremental.enabled()) {
            throw new IllegalArgumentException("快照源与增量游标(_seq)互斥: 批阶段快照为一次性 AS OF T0, 不携带增量列");
        }
        String base = snap.baseUrl.endsWith("/") ? snap.baseUrl.substring(0, snap.baseUrl.length() - 1) : snap.baseUrl;
        String stream = cfg.dataset;
        if (stream == null || stream.isBlank()) {
            throw new IllegalArgumentException("快照源需 dataset(流/逻辑表名)");
        }

        List<StructField> fields = schemaFields(cfg, warnings);
        StructType schema = DataTypes.createStructType(fields);

        List<Row> out = new ArrayList<>();
        String pageToken = null;
        int pages = 0;
        do {
            String url = base + "/api/chg/" + stream + "/snapshot?boundaryTso=" + cfg.t0
                    + "&limit=" + snap.pageSize
                    + (snap.consumerId == null || snap.consumerId.isBlank()
                            ? "" : "&consumerId=" + urlEncode(snap.consumerId))
                    + (pageToken == null ? "" : "&pageToken=" + urlEncode(pageToken));
            JsonNode root = getJson(url, snap.timeoutSeconds);
            JsonNode data = root.path("data");
            if (data.isMissingNode() || data.isNull()) data = root;   // 容错: 无 ApiResponse 包装时用顶层
            JsonNode rows = data.path("rows");
            if (!rows.isArray()) {
                throw new IllegalStateException("快照响应缺 rows 数组: " + url + " → " + root);
            }
            for (JsonNode r : rows) {
                out.add(toSparkRow(r, fields));
            }
            pageToken = textOf(data.path("pageToken"));
            if (pageToken == null || pageToken.isBlank() || "null".equals(pageToken)) {
                pageToken = textOf(data.path("nextPageToken"));       // 兼容旧字段名
            }
            if (data.path("done").asBoolean(false)) pageToken = null;
            pages++;
            if (pages > MAX_PAGES) {
                throw new IllegalStateException("快照分页异常:页数超过上限(" + MAX_PAGES + ")");
            }
        } while (pageToken != null && !pageToken.isBlank() && !"null".equals(pageToken));

        if (out.isEmpty()) {
            warnings.add("快照源返回 0 行(stream=" + stream + " t0=" + cfg.t0 + "): 批阶段将无事可做");
        }
        Log.info("[装配] 快照源: stream=" + stream + " t0=" + cfg.t0
                + " rows=" + out.size() + " pages=" + pages);
        return spark.createDataFrame(out, schema);
    }

    /* ---------- schema 与行转换 ---------- */

    /** datasetFields → StructType; 未知类型降级 string 并告警(不静默丢列) */
    private static List<StructField> schemaFields(TaskConfig cfg, List<String> warnings) {
        List<StructField> fields = new ArrayList<>();
        for (TaskConfig.FieldDef f : cfg.datasetFields) {
            if (f.name == null || f.name.isBlank()) continue;
            fields.add(DataTypes.createStructField(f.name, toSparkType(f.type, f.name, warnings), true));
        }
        if (fields.isEmpty()) {
            throw new IllegalArgumentException("快照源需 datasetFields(用于构造 schema); task-json 缺该段");
        }
        return fields;
    }

    private static DataType toSparkType(String type, String name, List<String> warnings) {
        if (type == null) type = "";
        return switch (type) {
            case "long" -> DataTypes.LongType;
            case "double" -> DataTypes.DoubleType;
            case "boolean" -> DataTypes.BooleanType;
            case "string" -> DataTypes.StringType;
            default -> {
                warnings.add("字段 " + name + " 类型未知(" + type + "), 按 string 处理");
                yield DataTypes.StringType;
            }
        };
    }

    /** JSON 行对象 → Spark Row(按声明类型强转; null 保 null) */
    private static Row toSparkRow(JsonNode r, List<StructField> fields) {
        Object[] vals = new Object[fields.size()];
        for (int i = 0; i < fields.size(); i++) {
            vals[i] = coerce(r.get(fields.get(i).name()), fields.get(i).dataType());
        }
        return RowFactory.create(vals);
    }

    private static Object coerce(JsonNode v, DataType t) {
        if (v == null || v.isNull() || v.isMissingNode()) return null;
        if (t instanceof LongType) {
            if (v.isNumber()) return v.longValue();
            try {
                return Long.parseLong(v.asText().trim());
            } catch (NumberFormatException e) {
                return v.asText();
            }
        }
        if (t instanceof DoubleType) {
            if (v.isNumber()) return v.doubleValue();
            try {
                return Double.parseDouble(v.asText().trim());
            } catch (NumberFormatException e) {
                return v.asText();
            }
        }
        if (t instanceof BooleanType) {
            if (v.isBoolean()) return v.booleanValue();
            return Boolean.parseBoolean(v.asText().trim());
        }
        return v.isValueNode() ? v.asText() : v.toString();
    }

    /* ---------- HTTP ---------- */

    private static JsonNode getJson(String url, int timeoutSeconds) {
        try {
            HttpRequest req = HttpRequest.newBuilder(URI.create(url))
                    .timeout(Duration.ofSeconds(timeoutSeconds))
                    .GET().build();
            HttpResponse<String> resp = HttpClient.newBuilder()
                    .connectTimeout(Duration.ofSeconds(timeoutSeconds))
                    .build()
                    .send(req, HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() / 100 != 2) {
                throw new IllegalStateException("HTTP " + resp.statusCode() + ": " + url);
            }
            return OM.readTree(resp.body());
        } catch (IllegalStateException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException("快照读取失败: " + url + " —— " + e.getMessage(), e);
        }
    }

    private static String textOf(JsonNode n) {
        if (n == null || n.isNull() || n.isMissingNode()) return null;
        return n.asText();
    }

    private static String urlEncode(String s) {
        return URLEncoder.encode(s, StandardCharsets.UTF_8);
    }
}
