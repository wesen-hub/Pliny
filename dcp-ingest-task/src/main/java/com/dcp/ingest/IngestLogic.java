package com.dcp.ingest;

import com.dcp.ingest.meta.JdbcMetaStore;
import com.dcp.ingest.meta.MetaStore;
import com.dcp.ingest.model.CdcRecord;
import com.dcp.ingest.model.Json;
import com.dcp.ingest.model.RouteUtil;
import com.dcp.ingest.send.EnvelopeSender;
import com.dcp.ingest.send.KafkaEnvelopeSender;
import com.dcp.outlet.sdk.Batch;
import com.dcp.outlet.sdk.ChangeRecord;
import com.dcp.outlet.sdk.ConsumerContext;
import com.dcp.outlet.sdk.ConsumerLogic;
import com.dcp.outlet.sdk.ConsumerLogicDef;
import com.dcp.outlet.sdk.LogicInitContext;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * 入站消费底座任务(@ConsumerLogicDef kafka-ingest@1.0.0)。
 *
 * <p>语义与切流前 KafkaIngestEngine.handleBatch 逐一对齐(2026-09-06 应用/凭证模型):
 * <ol>
 *   <li>消费 dcp.in.{逻辑表} 分区事件 —— after = 入站消息 JSON 透传
 *       ({@code {op, ts, submitter, data}}; 兼容 after=纯 data 形状, 缺省 op=INSERT / submitter=unknown);</li>
 *   <li>解析/验证(与 HTTP 预检同一套: op 归一 / 业务键 / 模型字段类型)失败 → 落 dlt_message(PENDING, 丢弃不阻断);</li>
 *   <li>按 submitter(appKey) 分组领 seq(seq_counter "kafka|{表}|{appKey}", 各应用独立可比序列)
 *       → 桶号 hash(业务键)%bucketCount → 分区(桶映射优先, 否则顺序映射) → CdcRecord 信封
 *       (taskId=k{appKey}, sourceDb=kafka, sourceTable=ingest, datasourceId=k) → 发 dcp.{逻辑表};</li>
 *   <li>领号/发送失败抛 → 底座不提交 offset 整批重投(at-least-once, 目标侧 seq 幂等)。</li>
 * </ol>
 */
@ConsumerLogicDef(logicId = "kafka-ingest", version = "1.0.0",
        description = "入站消费底座任务: 消费 dcp.in.{表} → 验证/领号/打信封 → 发 dcp.{表}(DLT 独立)")
public class IngestLogic implements ConsumerLogic {

    private static final Logger log = LoggerFactory.getLogger(IngestLogic.class);
    private static final ObjectMapper OM = new ObjectMapper();

    private IngestParams cfg;
    private MetaStore meta;
    private EnvelopeSender sender;
    private boolean closed;

    @Override
    public void init(LogicInitContext ctx) {
        this.cfg = OM.convertValue(ctx.params(), IngestParams.class);   // 失败抛
        if (cfg == null || cfg.logicalTable == null || cfg.logicalTable.isBlank()
                || cfg.jdbcUrl == null || cfg.jdbcUrl.isBlank()
                || cfg.kafkaBootstrap == null || cfg.kafkaBootstrap.isBlank()) {
            throw new IllegalStateException("[ingest-task] params 缺必填: logicalTable/jdbcUrl/kafkaBootstrap");
        }
        this.meta = newMetaStore(cfg);      // 连接失败抛
        this.sender = newSender(cfg);
        ctx.log().info("[ingest-task] ready table={} targetTopic={}", cfg.logicalTable, targetTopic());
    }

    /** 单测覆写点(LogicHarness 注入 fake 实现) */
    protected MetaStore newMetaStore(IngestParams p) {
        return new JdbcMetaStore(p.jdbcUrl, p.user, p.password);
    }

    /** 单测覆写点 */
    protected EnvelopeSender newSender(IngestParams p) {
        return new KafkaEnvelopeSender(p.kafkaBootstrap);
    }

    private String targetTopic() {
        if (cfg.targetTopic != null && !cfg.targetTopic.isBlank()) {
            return cfg.targetTopic;
        }
        return meta.loadLt(cfg.logicalTable).topic;
    }

    @Override
    public void process(Batch batch, ConsumerContext ctx) {
        if (batch.records().isEmpty()) {
            return;
        }
        String topic = targetTopic();   // 元库缺 topic 时信封目标未知, 直接失败(底座重试)
        if (topic == null || topic.isBlank()) {
            throw new IllegalStateException("[ingest-task] 逻辑表 " + cfg.logicalTable
                    + " 未配置 topic(目标信封流), 请先发布模型/配置逻辑表");
        }
        // ① 解析 + 验证(与 HTTP 预检同源), 按 appKey 分组; 失败落 DLT 丢弃
        Map<String, List<Object[]>> byApp = new LinkedHashMap<>();
        for (ChangeRecord r : batch.records()) {
            String appKey;
            String opStr;
            Map<String, Object> data;
            try {
                Map<String, Object> after = r.after();
                if (after == null) {
                    dlt(r, batch, "unknown", "入站消息缺 after(数据)");
                    continue;
                }
                Object sub = after.get("submitter");
                appKey = sub == null ? "unknown" : String.valueOf(sub);
                opStr = after.containsKey("op") && after.get("op") != null
                        ? String.valueOf(after.get("op")) : "INSERT";
                Object d = after.get("data");
                if (d instanceof Map) {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> dm = (Map<String, Object>) d;
                    data = new LinkedHashMap<>(dm);
                } else {
                    // 兼容 after=纯 data 形状(无 data 嵌套): 剥掉信封键后的剩余字段
                    data = new LinkedHashMap<>(after);
                    data.remove("op");
                    data.remove("ts");
                    data.remove("submitter");
                }
            } catch (Exception e) {
                dlt(r, batch, "unknown", "入站消息解析失败: " + e.getMessage());
                continue;
            }
            Analysis a = analyze(opStr, data);
            if (a.error != null) {
                dlt(r, batch, appKey, a.error);
                continue;
            }
            byApp.computeIfAbsent(appKey, k -> new ArrayList<>())
                    .add(new Object[]{a.businessKey, a.rowKind, a.fields});
        }

        // ② 按 appKey 分组: 领号 → 打信封 → 发 dcp.{逻辑表}
        MetaStore.LtConfig lt = meta.loadLt(cfg.logicalTable);
        int bucketCount = lt.bucketCount > 0 ? lt.bucketCount : 4096;
        for (Map.Entry<String, List<Object[]>> e : byApp.entrySet()) {
            String appKey = e.getKey();
            List<Object[]> recs = e.getValue();
            if (recs.isEmpty()) {
                continue;
            }
            long[] range = meta.allocateSeq("kafka|" + cfg.logicalTable + "|" + appKey, recs.size());
            long seq = range[0];
            String taskId = "k" + appKey.replaceAll("[^A-Za-z0-9_]", "_");
            for (Object[] rec : recs) {
                String businessKey = (String) rec[0];
                String rowKind = (String) rec[1];
                @SuppressWarnings("unchecked")
                Map<String, Object> fields = (Map<String, Object>) rec[2];
                int bucket = RouteUtil.bucket(businessKey, bucketCount);
                int partition = lt.partitionOf(bucket);
                CdcRecord cdc = CdcRecord.of(cfg.logicalTable, businessKey,
                        lt.keyColumns, rowKind, seq, fields);
                cdc.taskId = taskId;
                cdc.sourceDb = "kafka";
                cdc.sourceTable = "ingest";
                cdc.datasourceId = "k";
                sender.send(topic, partition, businessKey, Json.toJson(cdc));  // 失败抛 → 整批重投
                seq++;
            }
        }
    }

    /* ─────────────── 验证器(与 KafkaIngestEngine.analyze 同源) ─────────────── */

    /** 分析结果: error 非空 = 失败(DLT), 否则 {businessKey, rowKind, fields} */
    private static final class Analysis {
        final String businessKey;
        final String rowKind;
        final Map<String, Object> fields;
        final String error;

        Analysis(String businessKey, String rowKind, Map<String, Object> fields, String error) {
            this.businessKey = businessKey;
            this.rowKind = rowKind;
            this.fields = fields;
            this.error = error;
        }

        Analysis(String error) {
            this(null, null, null, error);
        }
    }

    private Analysis analyze(String op, Map<String, Object> data) {
        String normOp = normalizeOp(op);
        if (normOp == null) {
            return new Analysis("op 非法(仅 I/U/D 或 INSERT/UPDATE/DELETE): " + op);
        }
        if (data == null) {
            return new Analysis("data 不能为空(JSON 对象)");
        }
        MetaStore.LtConfig lt = meta.loadLt(cfg.logicalTable);
        List<String> keyCols = lt.keyColumns == null ? List.of() : lt.keyColumns;
        Map<String, String> columns = meta.loadColumns(cfg.logicalTable);
        StringBuilder key = new StringBuilder();
        for (String k : keyCols) {
            Object v = data.get(k);
            if (v == null || String.valueOf(v).isBlank()) {
                return new Analysis("业务键字段缺失或为空: " + k);
            }
            if (key.length() > 0) {
                key.append('|');
            }
            key.append(v);
        }
        String businessKey = key.length() == 0 ? null : key.toString();
        if (businessKey == null) {
            return new Analysis("逻辑表未配置业务键(key_columns 为空)");
        }
        Map<String, Object> fields = new LinkedHashMap<>();
        for (Map.Entry<String, Object> e2 : data.entrySet()) {
            String name = e2.getKey();
            String colType = columns.get(name);
            if (columns.isEmpty() || colType != null) {
                if (colType != null && isNumericType(colType) && e2.getValue() != null
                        && !String.valueOf(e2.getValue()).isBlank()) {
                    try {
                        new BigDecimal(String.valueOf(e2.getValue()));
                    } catch (NumberFormatException ex) {
                        return new Analysis("字段类型不可转换: " + name + "=" + e2.getValue()
                                + " (需要 " + colType + ")");
                    }
                }
                fields.put(name, e2.getValue() == null ? null : String.valueOf(e2.getValue()));
            }
        }
        return new Analysis(businessKey, normOp, fields, null);
    }

    private void dlt(ChangeRecord r, Batch batch, String appKey, String error) {
        meta.dlt(cfg.logicalTable, appKey, batch.stream(), r.partition(), r.offset(),
                String.valueOf(r.key()), payloadOf(r), error);
    }

    private static String payloadOf(ChangeRecord r) {
        try {
            return Json.toJson(r.after());
        } catch (Exception e) {
            return null;
        }
    }

    private static boolean isNumericType(String type) {
        String t = type == null ? "" : type.toLowerCase(Locale.ROOT);
        return t.contains("int") || t.contains("decimal") || t.contains("double")
                || t.contains("float") || t.contains("numeric");
    }

    private static String normalizeOp(String op) {
        if (op == null) {
            return null;
        }
        String u = op.trim().toUpperCase(Locale.ROOT);
        switch (u) {
            case "I":
            case "INSERT":
                return "INSERT";
            case "U":
            case "UPDATE":
                return "UPDATE";
            case "D":
            case "DELETE":
                return "DELETE";
            default:
                return null;
        }
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        if (sender != null) {
            try {
                sender.close();
            } catch (Exception ignore) {
            }
        }
        if (meta != null) {
            try {
                meta.close();
            } catch (Exception ignore) {
            }
        }
    }
}
