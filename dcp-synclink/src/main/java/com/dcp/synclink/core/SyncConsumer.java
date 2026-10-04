package com.dcp.synclink.core;

import com.dcp.synclink.model.CdcRecord;
import com.dcp.synclink.model.Json;
import com.dcp.synclink.model.RouteUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 消费端(设计文档 §4.4, P2 解耦版): 消费者线程只做 解析+路由+投递, 写库交给 {@link BatchWriteService} 攒批。
 *
 * <pre>
 *  ① 批量拉取(一次 poll, 每逻辑表每实例 1 个消费者线程)
 *  ② 逐条: 反序列化(仅 SeaTunnel envelope 格式) → 身份/主键修改拦截(无身份或 beforeKey≠businessKey → 落死信,
 *     死信 offset 直接推进游标 —— 落死信=已处理, 防重启后死信重放无限增长)
 *  ③ 路由: 物理表号 = hash(businessKey) % tableCount(纯函数, 不看分区)
 *  ④ 构建 BatchEntry(TableOp + per-op 原始记录 OpRef) → offer 有界队列(满 → pause 分区等待 → resume)
 *  ⑤ 写库/水位由 BatchWriteService(共享写线程池攒批; 攒批失败逐条隔离入死信)负责;
 *  ⑥ offset 提交由消费者线程(容器监听线程)周期调用 BatchWriteService#tryCommit
 *     (提交"已写游标"; 独立提交 consumer 无组成员身份, commitSync 必失败, 已废弃)
 *
 *  ack 语义: 不依赖 Acknowledgment(其提交 poll position 会超前于写库进度),
 *  由 BatchWriteService 维护"已写 offset 游标" + 消费者线程 commitSync → 崩溃重放由幂等吸收。
 *
 *  新方案: 写者列裁判是唯一裁判(无单源行级 seq 裁判); 消息只认采集端 envelope 格式;
 *  主键修改(beforeKey≠businessKey)在采集端已拦截停任务, 消费端兑底落死信不写库。
 *
 *  无 Spring 装配: 配置读取走 LogicalTableReader(自足), 默认分表参数由 SynclinkComponents 注入。
 * </pre>
 */
@Slf4j
public class SyncConsumer {

    private final AtomicUpsertDao upsertDao;
    private final LogicalTableReader tableReader;
    private final DltService dltService;
    private final BatchWriteService batchWriteService;
    private final int defaultTableCount;
    private final String defaultTablePrefix;

    public SyncConsumer(AtomicUpsertDao upsertDao,
                        LogicalTableReader tableReader, DltService dltService,
                        BatchWriteService batchWriteService,
                        int defaultTableCount, String defaultTablePrefix) {
        this.upsertDao = upsertDao;
        this.tableReader = tableReader;
        this.dltService = dltService;
        this.batchWriteService = batchWriteService;
        this.defaultTableCount = defaultTableCount;
        this.defaultTablePrefix = defaultTablePrefix;
    }

    /**
     * 批量消费处理(消费者线程): 解析+路由 → 投递有界队列;
     * 队列满 → pause 分区等待 → resume; 写库由 BatchWriteService 攒批完成。
     */
    public void handleBatch(List<ConsumerRecord<String, String>> records) {
        if (records.isEmpty()) {
            return;
        }
        try {
            // 按逻辑表分组(一个 topic = 一个逻辑表, 通常单组; 兼容历史多 topic)
            Map<String, List<ConsumerRecord<String, String>>> byTable = new HashMap<>();
            // 无需写库的记录(水位事件/解析不出逻辑表的坏消息已落死信): 仅推进 offset 游标
            Map<TopicPartition, Long> skipped = new HashMap<>();
            for (ConsumerRecord<String, String> record : records) {
                // TiCDC enable-tidb-extension 每秒一条水位事件: 非数据行, 跳过(否则每条 NPE 落死信无限增长)
                if (isWatermark(record.value())) {
                    skipped.merge(new TopicPartition(record.topic(), record.partition()),
                            record.offset(), Math::max);
                    continue;
                }
                String lt = parseLogicalTable(record.value());
                if (lt == null) {
                    // 非水位事件但缺 logicalTable 字段: 落死信留证(不阻断本批其余记录)
                    try {
                        dltService.save(record, "无法解析逻辑表(消息缺少 logicalTable 字段)");
                    } catch (Exception dltEx) {
                        log.error("死信落库失败: {}", dltEx.getMessage());
                    }
                    skipped.merge(new TopicPartition(record.topic(), record.partition()),
                            record.offset(), Math::max);
                    continue;
                }
                byTable.computeIfAbsent(lt, k -> new ArrayList<>()).add(record);
            }
            // 跳过记录推进游标: 提交不超前写库, 此类记录本无写库动作
            batchWriteService.ackOffsets(skipped);
            for (Map.Entry<String, List<ConsumerRecord<String, String>>> e : byTable.entrySet()) {
                String logicalTable = e.getKey();
                List<ConsumerRecord<String, String>> batch = e.getValue();
                List<AtomicUpsertDao.TableOp> ops = new ArrayList<>(batch.size());
                // 快照行单独收集(批路径三步式模板); 补丁落地前 snapshot 为 null → 全部走增量模板
                List<AtomicUpsertDao.TableOp> snapOps = new ArrayList<>();
                // per-op 原始记录引用(缺陷 4): 与 ops/snapOps 一一对应, 桶只合并本桶 op 的 offset
                List<BatchWriteService.OpRef> opRefs = new ArrayList<>(batch.size());
                List<BatchWriteService.OpRef> snapOpRefs = new ArrayList<>();
                // 已落死信的记录: offset 直接推进(落死信=已处理, 不因批写库失败重放重复落死信)
                Map<TopicPartition, Long> dltOffsets = new HashMap<>();
                LogicalTableConfig cfg = tableReader.load(logicalTable);
                int tCount = cfg.getTableCount() > 0 ? cfg.getTableCount() : defaultTableCount;
                String prefix = cfg.getTablePrefix() != null ? cfg.getTablePrefix() : defaultTablePrefix;
                for (ConsumerRecord<String, String> record : batch) {
                    CdcRecord rec = parseRecord(record.value());
                    // 写者列(任务实例×数据源×库×表, 新方案必配): 无身份消息 → 落死信(不写库不阻塞)
                    String writer = AtomicUpsertDao.writerColOf(rec.getTaskId(), rec.getDatasourceId(),
                            rec.getSourceDb(), rec.getSourceTable());
                    if (writer == null) {
                        try {
                            dltService.save(record, "消息缺少写者身份(taskId/datasourceId/sourceDb/sourceTable), "
                                    + "新方案写者列裁判无法定位, 已拦截");
                        } catch (Exception dltEx) {
                            log.error("死信落库失败: {}", dltEx.getMessage());
                        }
                        dltOffsets.merge(new TopicPartition(record.topic(), record.partition()),
                                record.offset(), Math::max);
                        continue;
                    }
                    // 主键修改兑底拦截: 采集端已告警停任务, 这里落死信不写库(不再拆 DELETE+INSERT)
                    if (rec.getBeforeKey() != null && !rec.getBeforeKey().equals(rec.getBusinessKey())) {
                        try {
                            dltService.save(record, "主键值被修改: " + rec.getBeforeKey()
                                    + " → " + rec.getBusinessKey() + "(消费端兑底拦截, 采集端已暂停任务)");
                        } catch (Exception dltEx) {
                            log.error("死信落库失败: {}", dltEx.getMessage());
                        }
                        dltOffsets.merge(new TopicPartition(record.topic(), record.partition()),
                                record.offset(), Math::max);
                        continue;
                    }
                    List<AtomicUpsertDao.TableOp> targetOps =
                            Boolean.TRUE.equals(rec.getSnapshot()) ? snapOps : ops;
                    List<BatchWriteService.OpRef> targetRefs =
                            Boolean.TRUE.equals(rec.getSnapshot()) ? snapOpRefs : opRefs;
                    targetOps.add(buildOp(rec, rec.getBusinessKey(), prefix, tCount, rec.getRowKind(), writer));
                    targetRefs.add(new BatchWriteService.OpRef(record));
                }
                // 死信 offset 推进游标(校验前置: 校验通过才进 batch, 死信单独推进)
                batchWriteService.ackOffsets(dltOffsets);
                // 投递有界队列; 满 → 等待有空位 → resume(快照批与增量批各自成批, 空批跳过)
                batchWriteService.ensureQueue(logicalTable);
                offerBatch(logicalTable, ops, opRefs, false);
                offerBatch(logicalTable, snapOps, snapOpRefs, true);
            }
        } catch (Exception ex) {
            log.error("投递队列失败(逐条兜底, 单条失败写死信): {}", ex.getMessage());
            Map<TopicPartition, Long> fallbackOffsets = new HashMap<>();
            for (ConsumerRecord<String, String> record : records) {
                try {
                    processOne(record);
                } catch (Exception single) {
                    try {
                        dltService.save(record, single.getMessage());
                    } catch (Exception dltEx) {
                        log.error("死信落库失败: {}", dltEx.getMessage());
                    }
                }
                // 兑底路径每条记录均已处理(写库成功或落死信) → offset 推进(缺陷 3, 防死信重放)
                fallbackOffsets.merge(new TopicPartition(record.topic(), record.partition()),
                        record.offset(), Math::max);
            }
            batchWriteService.ackOffsets(fallbackOffsets);
        }
    }

    /**
     * 投递一个批次(满则短暂等待: 背压传导回 Kafka, 写线程消化后继续;
     * 不依赖 consumer.pause(避免容器接口耦合); max.poll.interval(默认5min)内队列必被消化)
     */
    private void offerBatch(String logicalTable, List<AtomicUpsertDao.TableOp> ops,
                            List<BatchWriteService.OpRef> opRefs, boolean snapshot) {
        if (ops.isEmpty()) {
            return;
        }
        while (!batchWriteService.offer(logicalTable,
                new BatchWriteService.BatchEntry(logicalTable, ops, opRefs, snapshot))) {
            try {
                Thread.sleep(20);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    /** 解析逻辑表名(CdcRecord 或 envelope); 反序列化失败或缺失 logicalTable → null */
    private String parseLogicalTable(String value) {
        try {
            CdcRecord rec = parseRecord(value);
            return rec.getLogicalTable();
        } catch (Exception e) {
            return null;
        }
    }

    /** TiCDC 水位事件识别(enable-tidb-extension 每秒一条, type=TIDB_WATERMARK, 无数据行) */
    private boolean isWatermark(String value) {
        if (value == null || !value.contains("TIDB_WATERMARK")) {
            return false;
        }
        try {
            var node = Json.readTree(value);
            return "TIDB_WATERMARK".equalsIgnoreCase(node.path("type").asText(""));
        } catch (Exception e) {
            return false;
        }
    }

    /** 单条处理(投递失败时逐条兑底; 事务外单条写, 幂等吸收) */
    private void processOne(ConsumerRecord<String, String> record) {
        CdcRecord rec = parseRecord(record.value());
        // 写者列必配: 无身份 → 落死信(不写库)
        String writer = AtomicUpsertDao.writerColOf(rec.getTaskId(), rec.getDatasourceId(),
                rec.getSourceDb(), rec.getSourceTable());
        if (writer == null) {
            dltService.save(record, "消息缺少写者身份(taskId/datasourceId/sourceDb/sourceTable), "
                    + "新方案写者列裁判无法定位, 已拦截");
            return;
        }
        // 主键修改兑底拦截: 落死信不写库
        if (rec.getBeforeKey() != null && !rec.getBeforeKey().equals(rec.getBusinessKey())) {
            dltService.save(record, "主键值被修改: " + rec.getBeforeKey()
                    + " → " + rec.getBusinessKey() + "(消费端兑底拦截, 采集端已暂停任务)");
            return;
        }
        LogicalTableConfig cfg = tableReader.load(rec.getLogicalTable());
        int tCount = cfg.getTableCount() > 0 ? cfg.getTableCount() : defaultTableCount;
        String prefix = cfg.getTablePrefix() != null ? cfg.getTablePrefix() : defaultTablePrefix;
        List<AtomicUpsertDao.TableOp> ops = new ArrayList<>();
        ops.add(buildOp(rec, rec.getBusinessKey(), prefix, tCount, rec.getRowKind(), writer));
        // 快照行走批路径三步式模板, 增量行保持现状单条/批量 upsert
        if (Boolean.TRUE.equals(rec.getSnapshot())) {
            upsertDao.applySnapshotBatch(ops);
        } else {
            upsertDao.applyBatch(ops);
        }
    }

    /**
     * 解析 Kafka 消息: 仅 SeaTunnel 采集端 envelope 格式
     * {@code {"__bucket":N,"__payload":"<CdcRecord JSON>"}}(新方案唯一消息格式);
     * 解析失败抛异常, 由上层兑底落死信。
     */
    private CdcRecord parseRecord(String value) {
        var node = Json.readTree(value);
        if (!node.has("__payload")) {
            throw new IllegalArgumentException("消息不是 SeaTunnel envelope 格式(缺少 __payload)");
        }
        return Json.fromJson(node.get("__payload").asText(), CdcRecord.class);
    }

    private AtomicUpsertDao.TableOp buildOp(CdcRecord rec, String businessKey,
                                            String prefix, int tableCount, String rowKind, String writer) {
        RouteUtil.Route route = RouteUtil.route(businessKey, 4096, tableCount);
        String table = RouteUtil.tableName(prefix, route.tableNo(), tableCount);
        Map<String, Object> fields = rec.getFields();
        // 业务键列回填: 采集端字段映射后 fields 可能不含 key 列(如 ext 表只映射 addr/level),
        // 但 ODKU 定位行需要 key 列值 → 从 CdcRecord.businessKey 回填
        // (N1(A1): 单键直填; 多键 businessKey 为 JSON 数组元素与 keys 一一对应, 键值含 '|' 不歧义;
        //  解析失败回退 '|' 拆分, 兼容旧格式消息重放)
        List<String> keys = rec.getKeyColumns();
        if (keys != null && !keys.isEmpty()) {
            final Map<String, Object> ff = fields;
            boolean missing = keys.stream()
                    .anyMatch(k -> ff.get(k) == null || String.valueOf(ff.get(k)).isBlank());
            if (missing) {
                fields = new java.util.LinkedHashMap<>(fields);
                if (keys.size() == 1) {
                    fields.put(keys.get(0), businessKey);
                } else {
                    List<String> parts = parseKeyValues(businessKey);
                    for (int i = 0; i < keys.size() && i < parts.size(); i++) {
                        Object v = fields.get(keys.get(i));
                        if (v == null || String.valueOf(v).isBlank()) {
                            fields.put(keys.get(i), parts.get(i));
                        }
                    }
                }
            }
        }
        return new AtomicUpsertDao.TableOp(table, rowKind, rec.getKeyColumns(), fields, rec.getSeq(), writer);
    }

    /** N1(A1): 多键键值解析——businessKey 为 JSON 数组字符串(元素与 keyColumns 一一对应);
     *  非 JSON(旧格式 '|' 拼接串)回退 '|' 拆分, 兼容改格式前已入 Kafka 的消息重放 */
    private List<String> parseKeyValues(String businessKey) {
        try {
            var node = Json.readTree(businessKey);
            if (node != null && node.isArray()) {
                List<String> vals = new ArrayList<>();
                node.forEach(e -> vals.add(e.isNull() ? "" : e.asText()));
                return vals;
            }
        } catch (Exception ignore) {
            // 非 JSON → 旧格式 '|' 拼接
        }
        String[] parts = businessKey.split("\\|", -1);
        return List.of(parts);
    }
}
