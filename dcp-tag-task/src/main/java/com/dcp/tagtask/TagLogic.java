package com.dcp.tagtask;

import com.dcp.outlet.sdk.Batch;
import com.dcp.outlet.sdk.ChangeRecord;
import com.dcp.outlet.sdk.ConsumerContext;
import com.dcp.outlet.sdk.ConsumerLogic;
import com.dcp.outlet.sdk.ConsumerLogicDef;
import com.dcp.outlet.sdk.LogicInitContext;
import com.dcp.outlet.sdk.Op;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 打标消费底座任务(方案 N8): 底座形态的打标执行体, 替代 Flink 流执行(dcp-tagstream 双轨切流后退役)。
 *
 * <p>流程(与切流前 StreamTagJob 语义同构):
 * <ol>
 *   <li>init: 解析 params(发布快照 + sink 配置) → 编译 KieBase(失败抛, 平台判死信) →
 *       建 Redis/JDBC 连接 + 事件流发射器(按 enabled)</li>
 *   <li>process(每批, 单线程串行): 遍历变更记录 ——
 *       <ul>
 *         <li>INSERT/UPDATE/SNAPSHOT: after() 装配 Fact → 求值 → 命中多写
 *             (Redis tag:online:{entity} + tag_fact_view/tag_notify + Kafka 打标事件流
 *             dcp.tag.events, 订阅推送底座任务 dcp-sub-push 的数据源);</li>
 *         <li>DELETE: 清该实体在线标签(Redis del) + 删投影行;</li>
 *       </ul></li>
 *   <li>close: 释放连接(不销毁目标端数据)</li>
 * </ol>
 *
 * <p>契约遵循(ConsumerLogic D26):
 * 幂等(在线层/投影层均按 entity+tag 覆盖写, 平台至少一次投递安全);
 * 失败要抛(主链路 sink 失败抛异常 → 平台不提交 → 重试 → 死信; 审计 tag_notify 除外);
 * 不开线程池。
 */
@ConsumerLogicDef(logicId = "tag-engine", version = "1.0.0",
        description = "打标底座任务: 按发布快照求值, 多写 Redis 在线层 + MySQL 投影层 + Kafka 打标事件流(DELETE 清标签)")
public class TagLogic implements ConsumerLogic {

    private static final Logger log = LoggerFactory.getLogger(TagLogic.class);
    private static final ObjectMapper OM = new ObjectMapper();

    private TagParams cfg;
    private TagBundle bundle;
    private TagSinks.TagOnline online;
    private TagSinks.TagView view;
    private TagSinks.TagEvents events;
    private boolean closed;
    /** 验证用: 双写均未启用时收集命中(供集成验证反射断言; 生产双写启用时为空) */
    private final List<String[]> collected = new ArrayList<>();

    @Override
    public void init(LogicInitContext ctx) {
        try {
            this.cfg = OM.convertValue(ctx.params(), TagParams.class);
        } catch (Exception e) {
            throw new IllegalStateException("[tag-task] params 解析失败(平台透传形状不符?): " + e.getMessage(), e);
        }
        if (cfg.datasetFields == null || cfg.datasetFields.isEmpty()) {
            throw new IllegalStateException("[tag-task] params.datasetFields 缺失: 平台 v2TaskParams 未带字段域");
        }
        // 计划构建: DRL 生成失败/编译失败 → 抛, 平台判死信(不带病运行)
        this.bundle = new TagBundle(cfg);
        bundle.build();
        // 双写通道(按 enabled)
        if (cfg.online != null && cfg.online.enabled) {
            try {
                this.online = newOnlineSink(cfg);
            } catch (Exception e) {
                throw new IllegalStateException("[tag-task] Redis 在线层连接失败("
                        + cfg.online.redisHost + ":" + cfg.online.redisPort + "): " + e.getMessage(), e);
            }
        }
        if (cfg.factView != null && cfg.factView.enabled) {
            try {
                this.view = newViewSink(cfg);
            } catch (Exception e) {
                throw new IllegalStateException("[tag-task] 投影层连接失败: " + e.getMessage(), e);
            }
        }
        if (cfg.tagEvents != null && cfg.tagEvents.enabled) {
            try {
                this.events = newEventsSink(cfg);
            } catch (Exception e) {
                throw new IllegalStateException("[tag-task] 打标事件流发射器初始化失败: " + e.getMessage(), e);
            }
        }
        ctx.log().info("[tag-task] ready task={} entityKey={} dataset={} rules={} derived={} online={} factView={} tagEvents={} partitions={}",
                cfg.taskId, cfg.entityKey, cfg.dataset, cfg.rules.size(), cfg.derived.size(),
                online != null, view != null, events != null, ctx.partitions());
    }

    /** 在线层通道工厂(单测可覆写注入 fake) */
    protected TagSinks.TagOnline newOnlineSink(TagParams cfg) {
        return new TagSinks.OnlineSink(cfg);
    }

    /** 投影层通道工厂(单测可覆写注入 fake) */
    protected TagSinks.TagView newViewSink(TagParams cfg) throws Exception {
        return new TagSinks.ViewSink(cfg);
    }

    /** 打标事件流发射器工厂(单测可覆写注入 fake) */
    protected TagSinks.TagEvents newEventsSink(TagParams cfg) {
        return new TagSinks.KafkaTagEvents(cfg);
    }

    @Override
    public void process(Batch batch, ConsumerContext ctx) {
        for (ChangeRecord r : batch.records()) {
            try {
                handleRecord(r, batch);
            } catch (Exception e) {
                // 底座契约: 失败要抛(平台据此不提交 → 重试 → 死信); 包装定位信息
                throw new IllegalStateException("[tag-task] task=" + cfg.taskId + " stream=" + batch.stream()
                        + " op=" + r.op() + " partition=" + r.partition() + " offset=" + r.offset()
                        + " 处理失败: " + e.getMessage(), e);
            }
        }
        if (!ctx.caughtUp()) {
            ctx.log().debug("[tag-task] still catching up, safeWatermark={}", ctx.safeWatermark());
        }
    }

    /** 单条变更处理(幂等; 失败上抛) */
    private void handleRecord(ChangeRecord r, Batch batch) throws Exception {
        long now = System.currentTimeMillis();
        if (r.op() == Op.DELETE) {
            String entity = entityOf(r);
            if (entity == null) return;
            if (online != null) online.remove(entity);
            if (view != null) view.remove(entity);
            return;
        }
        Map<String, Object> fields = r.after();
        if (fields == null || fields.isEmpty()) return;
        String entity = entityOf(r);
        if (entity == null || entity.isEmpty()) {
            log.warn("[tag-task] task={} stream={} 记录缺 entityKey({}), 跳过 offset={}",
                    cfg.taskId, batch.stream(), cfg.entityKey, r.offset());
            return;
        }
        // 窗口派生(内存滚动窗口) → Fact 求值
        Map<String, Double> winVals = bundle.updateWindow(entity, now, fields);
        List<String[]> tags = bundle.eval(fields, winVals);
        if (tags.isEmpty()) return;
        Map<String, String> tm = new LinkedHashMap<>();
        for (String[] t : tags) tm.put(t[0], t[1] == null ? "" : t[1]);
        // 多写(失败上抛: 平台重试; 在线层/投影层/事件流均按 entity+tag 覆盖写/重发, 幂等)
        if (online != null) online.put(entity, tm);
        if (view != null) view.put(entity, tm, now);
        if (events != null) events.emit(entity, tm, now);
        if (online == null && view == null) collected.addAll(tags);
    }

    /** entity 取值: after() 优先(整行); 缺失回退 key()(主键); DELETE 再回退 before() */
    private String entityOf(ChangeRecord r) {
        Object v = null;
        if (r.after() != null) v = r.after().get(cfg.entityKey);
        if (v == null && r.key() != null) v = r.key().get(cfg.entityKey);
        if (v == null && r.before() != null) v = r.before().get(cfg.entityKey);
        return v == null ? null : String.valueOf(v);
    }

    @Override
    public void close() {
        if (online != null) online.close();
        if (view != null) view.close();
        if (events != null) events.close();
        closed = true;
    }

    /* —— 验证用(单测/底座集成验证) —— */
    boolean closed() { return closed; }

    TagParams cfg() { return cfg; }

    boolean onlineEnabled() { return online != null; }

    boolean viewEnabled() { return view != null; }

    /** 双写未启用时收集的命中标签(集成验证反射断言用) */
    public List<String[]> collected() { return collected; }
}
