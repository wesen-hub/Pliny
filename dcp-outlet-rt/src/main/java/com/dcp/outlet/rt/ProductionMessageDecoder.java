package com.dcp.outlet.rt;

import com.dcp.outlet.sdk.Op;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 生产消息解码器(底座激活衔接项 1 的落地实现):按 topic 分发三类格式,取代联调用的
 * {@link SdkEventDecoder}(保留为文本格式回退通道)。
 *
 * <pre>
 * 分发规则(指令透传 topic 为准, 与 ns/Kafka 归属无关):
 *   1. {@code dcp.cdc.*} 前缀    → TiCDC 行数据(canal-json 变体, 出口栈 TiCDC changefeed 输出):
 *        {@code {ts, data[], old[], isDdl}}; TiCDC 已拆事件(每消息一行, 主键修改 = DELETE+INSERT);
 *        INSERT(data 非空 old 空)/UPDATE(data+old)/DELETE(data 空 old 非空, 需 EnableOldValue);
 *        op 以显式 {@code type} 字段为准(真实 TiCDC 带 type=INSERT/UPDATE/DELETE), 缺失回退 data/old 推断;
 *        tso = ts(TiDB TSO, 与批流衔接 T0 过滤/水位同域);
 *        行 = TiDB 目标表整行(含写者列 _s 前缀与行级 seq, 同步链路 AtomicUpsertDao 落库真实列; 数值字段
 *        TiCDC 序列化为字符串, 原样保留, 消费端按字符串兼容解析);
 *        主键 key() = pkNames 从行内提取(真实 TiCDC 带主键列名), 无 pkNames → 空 Map(行内直取);
 *   2. topic 以 {@code tag.events} 结尾 → 打标事件流(TagLogic 发射, 订阅推送 sub-push tag 契约消费):
 *        {@code {taskId, entity, tagPath, tagDictId, tagValue, ts(epoch millis)}};
 *        每消息一条事件 → INSERT 型 EVENT, after = 事件字段, tso = ts(millis, 事件时间域);
 *   3. {@code dcp.in.*} 前缀    → 入站消息(kafka-ingest 底座任务输入, backend 提交面产出):
 *        {@code {op, ts, submitter, data}}; after = 整条 JSON(ingest 逻辑自取 op/submitter/data);
 *        tso = ts 字段解析(epoch 数字或 ISO/日期文本, 失败 → 当前时间, 保证 >0 通过 T0 过滤);
 *   4. 其他(含旧链路 topic 缺失) → SdkEventDecoder 文本格式回退(内存/联调, WATERMARK|ts 等)。
 * </pre>
 *
 * <p>解析失败纪律:非法 JSON/形状不符 → {@code Kind.OTHER}(不进业务逻辑、照常推进位点)+告警,
 * 不抛异常 —— 与骨架主循环语义一致(OTHER 跳过不阻断; 抛异常会导致整个 unit FAILED, 一条脏数据
 * 打停一个消费单元不可接受)。业务层缺键/缺列类错误由各 Logic 自行落 DLT(如 lake-sync)。
 */
public class ProductionMessageDecoder implements MessageDecoder {

    private static final Logger log = LoggerFactory.getLogger(ProductionMessageDecoder.class);
    private static final ObjectMapper OM = new ObjectMapper();

    /** TiCDC 行数据 topic 前缀(出口栈 dcp.cdc.{库}.{逻辑表}) */
    public static final String CDC_TOPIC_PREFIX = "dcp.cdc.";
    /** 打标事件流 topic 后缀(缺省 dcp.tag.events, 自定义 topic 须以此后缀收尾) */
    public static final String TAG_EVENTS_SUFFIX = "tag.events";
    /** 入站消息 topic 前缀(平台栈 dcp.in.{逻辑表}, 提交面削峰队列) */
    public static final String IN_TOPIC_PREFIX = "dcp.in.";

    private final MessageDecoder fallback = new SdkEventDecoder();

    @Override
    public DecodedMessage decode(RawMessage raw) {
        String topic = raw.topic();
        if (topic != null && topic.startsWith(CDC_TOPIC_PREFIX)) {
            return decodeCdcRow(raw);
        }
        if (topic != null && topic.endsWith(TAG_EVENTS_SUFFIX)) {
            return decodeTagEvent(raw);
        }
        if (topic != null && topic.startsWith(IN_TOPIC_PREFIX)) {
            return decodeInbound(raw);
        }
        try {
            return fallback.decode(raw);
        } catch (Exception e) {
            // 文本回退仅限内存/联调, 生产脏数据不抛(与 TiCDC/打标分支纪律一致):
            // 单条解析失败 → OTHER 跳过, 不因一条脏数据打停整个 unit
            log.warn("[decoder] 文本格式解析失败(topic={} p{}@{}), 跳过: {}",
                    raw.topic(), raw.partition(), raw.offset(), e.getMessage());
            return DecodedMessage.other(DecodedMessage.Kind.OTHER, raw.partition(), raw.offset(), 0L);
        }
    }

    /* ─────────────── 入站消息 → after=整条 JSON 的 EVENT ─────────────── */

    private DecodedMessage decodeInbound(RawMessage raw) {
        try {
            JsonNode root = OM.readTree(raw.payload());
            if (root == null || !root.isObject()) {
                throw new IllegalArgumentException("非对象 JSON");
            }
            // data 嵌套对象须保持 Map 形状(ingest 逻辑直接解包 after.data), 整体 convertValue 保留嵌套;
            // toObject 会把嵌套对象压成 JSON 文本字符串, 不适用于此
            Map<String, Object> after = OM.convertValue(root,
                    new com.fasterxml.jackson.core.type.TypeReference<LinkedHashMap<String, Object>>() {});
            // tso: ts 字段(epoch 数字 / ISO / 日期文本)→ millis;缺失/解析失败 → 当前时间
            // (入站任务 startTso=0 的会话过滤边界, tso 必须 >0 才不被持续过滤)
            long tso = inboundTso(root.path("ts"));
            return DecodedMessage.event(raw.partition(), raw.offset(), tso, null, Op.INSERT,
                    Map.of(), null, after);
        } catch (Exception e) {
            log.warn("[decoder] 入站消息解析失败(topic={} p{}@{}), 跳过: {}",
                    raw.topic(), raw.partition(), raw.offset(), e.getMessage());
            return DecodedMessage.other(DecodedMessage.Kind.OTHER, raw.partition(), raw.offset(), 0L);
        }
    }

    /** 入站 ts → epoch millis: 数字(epoch) / ISO / "yyyy-MM-dd HH:mm:ss"; 不可解析 → 当前时间 */
    private static long inboundTso(JsonNode ts) {
        try {
            if (ts == null || ts.isNull() || ts.isMissingNode()) {
                return System.currentTimeMillis();
            }
            if (ts.isNumber()) {
                return ts.asLong();
            }
            String v = ts.asText();
            if (v == null || v.isBlank()) {
                return System.currentTimeMillis();
            }
            try {
                return Long.parseLong(v.trim());
            } catch (NumberFormatException ignore) {
                // 非纯数字 → 按日期文本解析
            }
            try {
                return java.time.Instant.parse(v.trim()).toEpochMilli();
            } catch (Exception ignore) {
                // 非 ISO_INSTANT → 继续尝试本地日期时间
            }
            try {
                return java.time.LocalDateTime.parse(v.trim())
                        .atZone(java.time.ZoneId.systemDefault()).toInstant().toEpochMilli();
            } catch (Exception ignore) {
                // 非 ISO_LOCAL_DATE_TIME → 继续尝试 yyyy-MM-dd HH:mm:ss
            }
            java.time.LocalDateTime dt = java.time.LocalDateTime.parse(v.trim(),
                    java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
            return dt.atZone(java.time.ZoneId.systemDefault()).toInstant().toEpochMilli();
        } catch (Exception e) {
            return System.currentTimeMillis();
        }
    }

    /* ─────────────── TiCDC canal-json 行数据 → EVENT ─────────────── */

    private DecodedMessage decodeCdcRow(RawMessage raw) {
        try {
            JsonNode root = OM.readTree(raw.payload());
            // tso 域: 开启 enable-tidb-extension 的 TiCDC 输出 _tidb.commitTs(TSO, 与批流衔接 T0/水位同域);
            // 顶层 ts 为毫秒(未开启扩展的旧变体, 或手工样例直接填 TSO), 无 _tidb 时原样回退
            long tso = commitTsOf(root);
            if (root.path("isDdl").asBoolean(false)) {
                return DecodedMessage.other(DecodedMessage.Kind.DDL, raw.partition(), raw.offset(), tso);
            }
            // TIDB_WATERMARK(enable-tidb-extension 每秒一条): 水位消息 → WATERMARK 推进 safeWatermark(TSO 域)
            if ("TIDB_WATERMARK".equalsIgnoreCase(root.path("type").asText(""))) {
                long wm = root.path("_tidb").path("watermarkTs").asLong(tso);
                return DecodedMessage.watermark(raw.partition(), raw.offset(), wm);
            }
            Map<String, Object> after = rowOf(root.path("data"));
            Map<String, Object> before = rowOf(root.path("old"));
            // op: 显式 type 字段优先(真实 TiCDC canal-json 带 type=INSERT/UPDATE/DELETE), 缺失回退 data/old 推断
            Op op = opOf(root.path("type").asText(""), after, before);
            if (op == null) {
                // data/old 双空(如空事务 resolved ts 事件) → OTHER 跳过
                return DecodedMessage.other(DecodedMessage.Kind.OTHER, raw.partition(), raw.offset(), tso);
            }
            // 主键: pkNames(真实 canal-json 带主键列名)从行内提取 → r.key(); 无 pkNames → 空 Map(行内直取)
            Map<String, Object> key = keyOf(root.path("pkNames"), after, before);
            String stream = root.path("table").asText(null);   // canal-json 自带目标表名(供观测, 不参与骨架 stream 裁决)
            return DecodedMessage.event(raw.partition(), raw.offset(), tso, stream, op,
                    key, before, after);
        } catch (Exception e) {
            log.warn("[decoder] TiCDC 行数据解析失败(topic={} p{}@{}), 跳过: {}",
                    raw.topic(), raw.partition(), raw.offset(), e.getMessage());
            return DecodedMessage.other(DecodedMessage.Kind.OTHER, raw.partition(), raw.offset(), 0L);
        }
    }

    /** commitTs 优先(官方键, enable-tidb-extension 时在 _tidb.commitTs; 缺失回退顶层 ts) */
    private static long commitTsOf(JsonNode root) {
        long cts = root.path("_tidb").path("commitTs").asLong(0L);
        return cts > 0 ? cts : root.path("ts").asLong(0L);
    }

    /** op 判定: type 字段优先(大小写不敏感), 缺失回退 data/old 推断; 两者皆不可判 → null */
    private static Op opOf(String type, Map<String, Object> after, Map<String, Object> before) {
        if (type != null && !type.isBlank()) {
            switch (type.toUpperCase()) {
                case "INSERT": return Op.INSERT;
                case "UPDATE": return Op.UPDATE;
                case "DELETE": return Op.DELETE;
                default: break;   // 未知 type → 回退推断
            }
        }
        if (after != null && before != null) {
            return Op.UPDATE;
        }
        if (after != null) {
            return Op.INSERT;
        }
        if (before != null) {
            return Op.DELETE;
        }
        return null;
    }

    /** 主键提取: pkNames 从 data 行(缺则 old 行)取键列值; 无 pkNames → 空 Map */
    private static Map<String, Object> keyOf(JsonNode pkNames, Map<String, Object> after, Map<String, Object> before) {
        if (pkNames == null || !pkNames.isArray() || pkNames.isEmpty()) {
            return Map.of();
        }
        Map<String, Object> row = after != null ? after : before;
        if (row == null) {
            return Map.of();
        }
        Map<String, Object> key = new LinkedHashMap<>();
        for (JsonNode pk : pkNames) {
            String col = pk.asText("");
            if (!col.isBlank() && row.containsKey(col)) {
                key.put(col, row.get(col));
            }
        }
        return key;
    }

    /** data/old 取首个元素(对象);空/非对象 → null。协议为单行消息, 多行时告警并只取首行(低危-3)。 */
    private Map<String, Object> rowOf(JsonNode arr) {
        if (arr == null || !arr.isArray() || arr.isEmpty() || !arr.get(0).isObject()) {
            return null;
        }
        if (arr.size() > 1) {
            log.warn("[decoder] data/old 含 {} 行, canal-json 单行协议只取首行, 其余被忽略(请核对上游是否误发批量)",
                    arr.size());
        }
        Map<String, Object> row = new LinkedHashMap<>();
        arr.get(0).fields().forEachRemaining(e -> row.put(e.getKey(), toObject(e.getValue())));
        return row;
    }

    /* ─────────────── 打标事件流 → INSERT 型 EVENT ─────────────── */

    private DecodedMessage decodeTagEvent(RawMessage raw) {
        try {
            JsonNode root = OM.readTree(raw.payload());
            if (root == null || !root.isObject()) {
                throw new IllegalArgumentException("非对象 JSON: " + root.getNodeType());
            }
            Map<String, Object> after = new LinkedHashMap<>();
            root.fields().forEachRemaining(e -> after.put(e.getKey(), toObject(e.getValue())));
            long ts = root.path("ts").asLong(0L);
            return DecodedMessage.event(raw.partition(), raw.offset(), ts, null, Op.INSERT,
                    Map.of(), null, after);
        } catch (Exception e) {
            log.warn("[decoder] 打标事件解析失败(topic={} p{}@{}), 跳过: {}",
                    raw.topic(), raw.partition(), raw.offset(), e.getMessage());
            return DecodedMessage.other(DecodedMessage.Kind.OTHER, raw.partition(), raw.offset(), 0L);
        }
    }

    /* ─────────────── 工具 ─────────────── */

    /** JsonNode → Java 标量(字符串/长整/浮点/布尔; 数组与嵌套对象序列化回 JSON 文本) */
    private static Object toObject(JsonNode n) {
        if (n == null || n.isNull()) {
            return null;
        }
        if (n.isTextual()) {
            return n.asText();
        }
        if (n.isIntegralNumber()) {
            return n.asLong();
        }
        if (n.isNumber()) {
            return n.asDouble();
        }
        if (n.isBoolean()) {
            return n.asBoolean();
        }
        return n.toString();
    }

    /* ─────────────── 便捷构造(测试/自检用) ─────────────── */

    /** TiCDC 行数据样例 payload(INSERT/UPDATE/DELETE, 与出口栈 dcp.cdc.* 流形状一致)。 */
    public static byte[] cdcPayload(long ts, String dataJson, String oldJson) {
        StringBuilder sb = new StringBuilder("{\"ts\":").append(ts);
        if (dataJson != null) {
            sb.append(",\"data\":[").append(dataJson).append(']');
        }
        if (oldJson != null) {
            sb.append(",\"old\":[").append(oldJson).append(']');
        }
        sb.append('}');
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }

    /** 打标事件 payload(与 dcp.tag.events 流形状一致)。 */
    public static byte[] tagEventPayload(long ts, String entity, String tagPath, String tagValue) {
        return ("{\"entity\":\"" + entity + "\",\"tagPath\":\"" + tagPath
                + "\",\"tagValue\":\"" + tagValue + "\",\"ts\":" + ts + "}")
                .getBytes(StandardCharsets.UTF_8);
    }
}
