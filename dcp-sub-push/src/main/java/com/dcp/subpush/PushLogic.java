package com.dcp.subpush;

import com.dcp.outlet.sdk.Batch;
import com.dcp.outlet.sdk.ChangeRecord;
import com.dcp.outlet.sdk.ConsumerContext;
import com.dcp.outlet.sdk.ConsumerLogic;
import com.dcp.outlet.sdk.ConsumerLogicDef;
import com.dcp.outlet.sdk.LogicInitContext;
import com.dcp.outlet.sdk.Op;
import com.dcp.subpush.channel.KafkaChannel;
import com.dcp.subpush.channel.PushChannel;
import com.dcp.subpush.channel.WebhookChannel;
import com.dcp.subpush.match.CondMatcher;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 订阅推送底座任务(@ConsumerLogicDef sub-push@1.0.0, 方案 P3 维度⑤ 数据订阅):
 * 每生效 push 契约一个任务, 事件驱动实时推送, 替代 subscription-service 轮询式
 * deliver(PushScheduler 定时档保留, manual 保留手动 deliver)。
 *
 * <p>流程(与切流前 DataSubService.deliver 增量段语义同构):
 * <ol>
 *   <li>logical 契约: 消费出口栈 TiCDC 流 dcp.cdc.* 行数据(一单元一流, batch.stream()
 *       即 TiDB 目标表名) —— 引用表过滤(view.tables 匹配流名) → DELETE 事件不推
 *       (与轮询语义一致: 订阅方只看"当前存在的行") → seqFloor 过滤(预留首轮全量衔接)
 *       → conds 内存求值(组内 AND/组间 AND) → 交付列裁剪(view.cols; merge 视图加
 *       source_table) → 行含 seq(TiDB 行级序列);</li>
 *   <li>tag 契约: 消费打标事件流(dcp.tag.events, TagLogic 发射) —— 圈选
 *       (tagPath/tagDictId ∈ view.tags, 可带 value 匹配) → exclude 剔除(查元库
 *       tag_fact_view 当前标签态) → tsFloor 过滤(预留) → 事件 {entity,tag,value,ts};</li>
 *   <li>整批匹配行组成 payload {@code {contract, mode:"stream", count, rows|events}}
 *       一次推送(与现状 deliver payload 同构); 无匹配不推送;</li>
 *   <li>推送失败抛 → 平台不提交→重试→死信(at-least-once, 与现状"失败重放同范围"一致)。</li>
 * </ol>
 *
 * <p>契约遵循(ConsumerLogic D26): 幂等(订阅方至少一次投递, 与现状一致);
 * 失败要抛(推送主链路); 不开线程池(单线程串行 process)。
 */
@ConsumerLogicDef(logicId = "sub-push", version = "1.0.0",
        description = "订阅推送底座任务: 消费出口栈 TiCDC 流行数据/打标事件流 → 圈选/裁剪 → webhook/kafka 实时推送")
public class PushLogic implements ConsumerLogic {

    private static final ObjectMapper OM = new ObjectMapper();

    private PushParams cfg;
    private PushChannel channel;
    private Excluder excluder;
    private boolean closed;

    @Override
    public void init(LogicInitContext ctx) {
        try {
            this.cfg = OM.convertValue(ctx.params(), PushParams.class);
        } catch (Exception e) {
            throw new IllegalStateException("[sub-push] params 解析失败(平台透传形状不符?): " + e.getMessage(), e);
        }
        if (cfg == null || cfg.viewType == null || cfg.view == null
                || !List.of("logical", "tag").contains(cfg.viewType)) {
            throw new IllegalStateException("[sub-push] params 缺必填: viewType(logical|tag)/view(契约视图快照)");
        }
        if (!List.of("webhook", "kafka").contains(cfg.targetType)) {
            throw new IllegalStateException("[sub-push] params.targetType 须为 webhook|kafka, 实际: " + cfg.targetType);
        }
        if ("webhook".equals(cfg.targetType) && (cfg.targetUrl == null || cfg.targetUrl.isBlank())) {
            throw new IllegalStateException("[sub-push] webhook 契约缺 targetUrl");
        }
        // 业务校验先行(视图形状/exclude 依赖), 外部通道后建(连接失败统一 fail-closed 包装)
        this.excluder = newExcluder();
        try {
            this.channel = newChannel(cfg);
        } catch (IllegalStateException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException("[sub-push] 推送通道初始化失败: " + e.getMessage(), e);
        }
        ctx.log().info("[sub-push] ready contract={} viewType={} target={}",
                cfg.contractId, cfg.viewType, cfg.targetType);
    }

    /** 推送通道工厂(单测可覆写注入 fake)。 */
    protected PushChannel newChannel(PushParams p) {
        return "webhook".equals(p.targetType) ? new WebhookChannel(p) : new KafkaChannel(p);
    }

    /** exclude 查询器工厂(单测可覆写注入 fake)。 */
    protected Excluder newExcluder() {
        return "tag".equals(cfg.viewType) ? new Excluder(cfg.view, cfg.jdbcUrl, cfg.user, cfg.password) : Excluder.none();
    }

    @Override
    public void process(Batch batch, ConsumerContext ctx) {
        if (batch.records().isEmpty()) {
            return;
        }
        List<Map<String, Object>> out = "logical".equals(cfg.viewType)
                ? collectLogical(batch) : collectTag(batch);
        if (out.isEmpty()) {
            return;
        }
        try {
            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("contract", cfg.contractName == null ? String.valueOf(cfg.contractId) : cfg.contractName);
            payload.put("mode", "stream");
            payload.put("count", out.size());
            payload.put("logical".equals(cfg.viewType) ? "rows" : "events", out);
            channel.send(OM.writeValueAsString(payload));
        } catch (Exception e) {
            // 底座契约: 失败要抛(平台据此不提交 → 重试 → 死信); 包装定位信息
            throw new IllegalStateException("[sub-push] contract=" + cfg.contractId + " stream=" + batch.stream()
                    + " partition=" + ctx.partition() + " 推送失败: " + e.getMessage(), e);
        }
    }

    /* ─────────────── logical 契约: TiCDC 行数据 → rows ─────────────── */

    private List<Map<String, Object>> collectLogical(Batch batch) {
        JsonNode view = cfg.view;
        List<String> tables = new ArrayList<>();
        for (JsonNode t : view.path("tables")) {
            String n = t.asText("");
            if (isIdent(n)) {
                tables.add(n);
            }
        }
        List<String> cols = new ArrayList<>();
        for (JsonNode c : view.path("cols")) {
            String n = c.asText("");
            if (isIdent(n)) {
                cols.add(n);
            }
        }
        boolean merged = tables.size() > 1 && "merge".equals(view.path("merge").asText("merge"));
        // 一单元一流: batch.stream() 即 TiDB 目标表名(扫描器从 dcp.cdc.{库}.{表} 派生);
        // 无法判定流名 → 非契约引用流(fail-closed, 与原信封表过滤一致)
        String stream = batch.stream();
        if (!tables.isEmpty() && (stream == null || stream.isBlank() || !tables.contains(stream))) {
            return new ArrayList<>();   // 非契约引用流
        }
        List<Map<String, Object>> rows = new ArrayList<>();
        for (ChangeRecord r : batch.records()) {
            if (r.op() == Op.DELETE) {
                continue;   // 删除事件不推(与轮询语义一致: 订阅方只看当前存在的行)
            }
            Map<String, Object> after = r.after();
            if (after == null || after.isEmpty()) {
                continue;
            }
            Long seq = asLong(after.get("seq"));
            if (cfg.seqFloor != null && seq != null && seq <= cfg.seqFloor) {
                continue;   // 预留: 首轮全量已交付过的窗口闭合下限
            }
            // 业务字段 = 行内剔除写者列(_s 前缀)与 seq 列(写者列是平台内部裁判列, 不进交付载荷)
            Map<String, Object> fields = new LinkedHashMap<>();
            for (Map.Entry<String, Object> e : after.entrySet()) {
                String k = e.getKey();
                if (k.startsWith("_s") || "seq".equals(k)) {
                    continue;
                }
                fields.put(k, e.getValue());
            }
            if (fields.isEmpty()) {
                continue;
            }
            if (!CondMatcher.match(view, fields)) {
                continue;
            }
            Map<String, Object> row = new LinkedHashMap<>();
            if (cols.isEmpty()) {
                row.putAll(fields);
            } else {
                for (String c : cols) {
                    if (fields.containsKey(c)) {
                        row.put(c, fields.get(c));
                    }
                }
            }
            if (merged) {
                row.put("source_table", stream);
            }
            if (seq != null) {
                row.put("seq", seq);
            }
            rows.add(row);
        }
        return rows;
    }

    /* ─────────────── tag 契约: 打标事件流 → events ─────────────── */

    private List<Map<String, Object>> collectTag(Batch batch) {
        JsonNode view = cfg.view;
        List<String[]> include = new ArrayList<>();   // {tagDictId, path, value}
        for (JsonNode t : view.path("tags")) {
            String tid = t.hasNonNull("tagDictId") ? t.path("tagDictId").asText() : null;
            String path = t.path("path").asText("");
            String value = t.path("value").asText("");
            if ((tid == null || tid.isBlank()) && path.isBlank()) {
                continue;
            }
            include.add(new String[]{tid, path, value});
        }
        if (include.isEmpty()) {
            return new ArrayList<>();
        }
        List<Map<String, Object>> events = new ArrayList<>();
        for (ChangeRecord r : batch.records()) {
            Map<String, Object> after = r.after();
            if (after == null) {
                continue;
            }
            long ts = asLong(after.get("ts")) == null ? 0L : asLong(after.get("ts"));
            if (cfg.tsFloor != null && ts <= cfg.tsFloor) {
                continue;   // 预留: 首轮全量已交付过的窗口闭合下限
            }
            String entity = str(after.get("entity"));
            String tagPath = str(after.get("tagPath"));
            String tagDictId = str(after.get("tagDictId"));
            String tagValue = after.get("tagValue") == null ? "" : str(after.get("tagValue"));
            if (entity.isBlank() || tagPath.isBlank()) {
                continue;   // 事件形状不完整(非法事件)跳过
            }
            if (!hitAny(include, tagDictId, tagPath, tagValue)) {
                continue;   // 不在圈选标签集
            }
            if (excluder.excluded(entity)) {
                continue;   // 持有 exclude 标签 → 剔除
            }
            Map<String, Object> ev = new LinkedHashMap<>();
            ev.put("entity", entity);
            ev.put("tag", tagPath);
            ev.put("value", tagValue);
            ev.put("ts", ts);
            events.add(ev);
        }
        return events;
    }

    /** 圈选命中: 任一规格(id 优先匹配, 兼容旧行按路径) + value(若指定)相等。 */
    private static boolean hitAny(List<String[]> include, String tagDictId, String tagPath, String tagValue) {
        for (String[] spec : include) {
            boolean hit;
            if (spec[0] != null && !spec[0].isBlank()) {
                hit = spec[0].equals(tagDictId)
                        || (tagDictId.isBlank() && spec[1].equals(tagPath));   // 旧行无 id 按路径兼容
            } else {
                hit = spec[1].equals(tagPath);
            }
            if (hit && (spec[2] == null || spec[2].isBlank() || spec[2].equals(tagValue))) {
                return true;
            }
        }
        return false;
    }

    /* ─────────────── 工具 ─────────────── */

    private static String str(Object o) {
        return o == null ? "" : String.valueOf(o);
    }

    private static Long asLong(Object o) {
        if (o instanceof Number n) {
            return n.longValue();
        }
        if (o instanceof String s && !s.isBlank()) {
            try {
                return Long.valueOf(s.trim());
            } catch (NumberFormatException ignore) {
                return null;
            }
        }
        return null;
    }

    private static boolean isIdent(String x) {
        return x != null && !x.isBlank() && x.matches("[A-Za-z0-9_]+");
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        if (excluder != null) {
            excluder.close();
        }
        if (channel != null) {
            try {
                channel.close();
            } catch (Exception ignore) {
            }
        }
    }
}
