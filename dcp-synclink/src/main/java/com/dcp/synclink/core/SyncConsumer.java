package com.dcp.synclink.core;

import com.dcp.synclink.model.CdcRecord;
import com.dcp.synclink.model.Json;
import com.dcp.synclink.model.RouteUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import org.apache.kafka.common.TopicPartition;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 消费端(V3 §4.4, P2 解耦版): 消费者线程只做 解析+路由+投递, 写库交给 {@link BatchWriteService} 攒批。
 *
 * <pre>
 *  ① 批量拉取(一次 poll, 每逻辑表每实例 1 个消费者线程)
 *  ② 逐条: 反序列化 → 主键修改检测(beforeKey ≠ businessKey → 拆 DELETE+INSERT 成对)
 *  ③ 路由: 物理表号 = hash(businessKey) % tableCount(纯函数, 不看分区)
 *  ④ 构建 BatchEntry(TableOp + 分区 offset) → offer 有界队列(满 → pause 分区等待 → resume)
 *  ⑤ 写库/水位由 BatchWriteService(共享写线程池攒批)负责;
 *  ⑥ offset 提交由消费者线程(容器监听线程)周期调用 BatchWriteService#tryCommit
 *     (提交"已写游标"; 独立提交 consumer 无组成员身份, commitSync 必失败, 已废弃)
 *
 *  ack 语义: 不依赖 Acknowledgment(其提交 poll position 会超前于写库进度),
 *  由 BatchWriteService 维护"已写 offset 游标" + 消费者线程 commitSync → 崩溃重放由幂等吸收。
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
    private final JdbcTemplate metaJdbc;
    private final int defaultTableCount;
    private final String defaultTablePrefix;
    /** 单源判定缓存(逻辑表 → 是否单源): 单源表启用整行 seq 字段裁判 */
    private final java.util.concurrent.ConcurrentHashMap<String, Boolean> singleSourceCache =
            new java.util.concurrent.ConcurrentHashMap<>();

    public SyncConsumer(AtomicUpsertDao upsertDao,
                        LogicalTableReader tableReader, DltService dltService,
                        BatchWriteService batchWriteService,
                        JdbcTemplate jdbcTemplate,
                        int defaultTableCount, String defaultTablePrefix) {
        this.upsertDao = upsertDao;
        this.tableReader = tableReader;
        this.dltService = dltService;
        this.batchWriteService = batchWriteService;
        this.metaJdbc = jdbcTemplate;
        this.defaultTableCount = defaultTableCount;
        this.defaultTablePrefix = defaultTablePrefix;
    }

    /** 单源判定(缓存 1 分钟): cdc_st_task 只有 1 个源 → 整行 seq 裁判可用 */
    private boolean singleSource(String logicalTable) {
        if (logicalTable == null) {
            return true;
        }
        Boolean cached = singleSourceCache.get(logicalTable);
        if (cached != null) {
            return cached;
        }
        boolean single = true;
        try {
            Integer n = metaJdbc.queryForObject(
                    "SELECT COUNT(*) FROM cdc_st_task WHERE logical_table = ?", Integer.class, logicalTable);
            single = n == null || n <= 1;
        } catch (Exception e) {
            log.warn("单源判定失败(默认单源): {} - {}", logicalTable, e.getMessage());
        }
        singleSourceCache.put(logicalTable, single);
        return single;
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
            for (ConsumerRecord<String, String> record : records) {
                String lt = parseLogicalTable(record.value());
                byTable.computeIfAbsent(lt, k -> new ArrayList<>()).add(record);
            }
            for (Map.Entry<String, List<ConsumerRecord<String, String>>> e : byTable.entrySet()) {
                String logicalTable = e.getKey();
                List<ConsumerRecord<String, String>> batch = e.getValue();
                List<AtomicUpsertDao.TableOp> ops = new ArrayList<>(batch.size());
                // 快照行单独收集(批路径三步式模板); 补丁落地前 snapshot 为 null → 全部走增量模板
                List<AtomicUpsertDao.TableOp> snapOps = new ArrayList<>();
                Map<TopicPartition, Long> offsets = new HashMap<>();
                for (ConsumerRecord<String, String> record : batch) {
                    CdcRecord rec = parseRecord(record.value());
                    LogicalTableConfig cfg = tableReader.load(logicalTable);
                    int tCount = cfg.getTableCount() > 0 ? cfg.getTableCount() : defaultTableCount;
                    String prefix = cfg.getTablePrefix() != null ? cfg.getTablePrefix() : defaultTablePrefix;
                    // 写者列(任务实例×数据源×库×表): 有完整身份 → 各写者 seq 列裁判; 无身份(旧链路)退回单源行级 seq 裁判
                    String writer = AtomicUpsertDao.writerColOf(rec.getTaskId(), rec.getDatasourceId(),
                            rec.getSourceDb(), rec.getSourceTable());
                    boolean judge = writer == null && singleSource(logicalTable);
                    List<AtomicUpsertDao.TableOp> targetOps =
                            Boolean.TRUE.equals(rec.getSnapshot()) ? snapOps : ops;
                    if (rec.getBeforeKey() != null && !rec.getBeforeKey().equals(rec.getBusinessKey())) {
                        targetOps.add(buildOp(rec, rec.getBeforeKey(), prefix, tCount, "DELETE", judge, writer));
                        targetOps.add(buildOp(rec, rec.getBusinessKey(), prefix, tCount, rec.getRowKind(), judge, writer));
                    } else {
                        targetOps.add(buildOp(rec, rec.getBusinessKey(), prefix, tCount, rec.getRowKind(), judge, writer));
                    }
                    offsets.merge(new TopicPartition(record.topic(), record.partition()),
                            record.offset(), Math::max);
                }
                // 投递有界队列; 满 → 等待有空位 → resume(快照批与增量批各自成批, 空批跳过)
                batchWriteService.ensureQueue(logicalTable);
                offerBatch(logicalTable, batch, offsets, ops, false);
                offerBatch(logicalTable, batch, offsets, snapOps, true);
            }
        } catch (Exception ex) {
            log.error("投递队列失败(逐条兜底, 单条失败写死信): {}", ex.getMessage());
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
            }
        }
    }

    /**
     * 投递一个批次(满则短暂等待: 背压传导回 Kafka, 写线程消化后继续;
     * 不依赖 consumer.pause(避免容器接口耦合); max.poll.interval(默认5min)内队列必被消化)
     */
    private void offerBatch(String logicalTable, List<ConsumerRecord<String, String>> batch,
                            Map<TopicPartition, Long> offsets,
                            List<AtomicUpsertDao.TableOp> ops, boolean snapshot) {
        if (ops.isEmpty()) {
            return;
        }
        while (!batchWriteService.offer(logicalTable,
                new BatchWriteService.BatchEntry(logicalTable, ops, batch, offsets, snapshot))) {
            try {
                Thread.sleep(20);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    /** 解析逻辑表名(CdcRecord 或 envelope) */
    private String parseLogicalTable(String value) {
        try {
            CdcRecord rec = parseRecord(value);
            return rec.getLogicalTable();
        } catch (Exception e) {
            return "unknown";
        }
    }

    /** 单条处理(投递失败时逐条兑底; 事务外单条写, 幂等吸收) */
    private void processOne(ConsumerRecord<String, String> record) {
        CdcRecord rec = parseRecord(record.value());
        LogicalTableConfig cfg = tableReader.load(rec.getLogicalTable());
        int tCount = cfg.getTableCount() > 0 ? cfg.getTableCount() : defaultTableCount;
        String prefix = cfg.getTablePrefix() != null ? cfg.getTablePrefix() : defaultTablePrefix;
        List<AtomicUpsertDao.TableOp> ops = new ArrayList<>();
        // 写者列(任务实例×数据源×库×表): 有完整身份 → 落目标侧各写者 seq 列裁判; 无身份(旧链路)退回单源行级 seq 裁判
        String writer = AtomicUpsertDao.writerColOf(rec.getTaskId(), rec.getDatasourceId(),
                rec.getSourceDb(), rec.getSourceTable());
        boolean judge = writer == null && singleSource(rec.getLogicalTable());
        if (rec.getBeforeKey() != null && !rec.getBeforeKey().equals(rec.getBusinessKey())) {
            ops.add(buildOp(rec, rec.getBeforeKey(), prefix, tCount, "DELETE", judge, writer));
            ops.add(buildOp(rec, rec.getBusinessKey(), prefix, tCount, rec.getRowKind(), judge, writer));
        } else {
            ops.add(buildOp(rec, rec.getBusinessKey(), prefix, tCount, rec.getRowKind(), judge, writer));
        }
        // 快照行走批路径三步式模板, 增量行保持现状单条/批量 upsert
        if (Boolean.TRUE.equals(rec.getSnapshot())) {
            upsertDao.applySnapshotBatch(ops);
        } else {
            upsertDao.applyBatch(ops);
        }
    }

    /**
     * 解析 Kafka 消息: 兼容两种格式 ——
     * ① SeaTunnel 采集端(CdcSeqTransform): {"__bucket":N,"__payload":"<CdcRecord JSON>"}
     * ② 自研采集端/演示链路: 裸 CdcRecord JSON
     */
    private CdcRecord parseRecord(String value) {
        if (value != null && value.contains("\"__payload\"")) {
            try {
                var node = Json.readTree(value);
                return Json.fromJson(node.get("__payload").asText(), CdcRecord.class);
            } catch (Exception e) {
                log.warn("SeaTunnel envelope 解析失败, 按裸 CdcRecord 尝试: {}", e.getMessage());
            }
        }
        return Json.fromJson(value, CdcRecord.class);
    }

    private AtomicUpsertDao.TableOp buildOp(CdcRecord rec, String businessKey,
                                            String prefix, int tableCount, String rowKind, boolean judge, String writer) {
        RouteUtil.Route route = RouteUtil.route(businessKey, 4096, tableCount);
        String table = RouteUtil.tableName(prefix, route.tableNo(), tableCount);
        Map<String, Object> fields = rec.getFields();
        // 旧 key 分支(DELETE): 唯一键列的值要换成旧主键值, 否则删错行
        if (!businessKey.equals(rec.getBusinessKey())) {
            fields = new java.util.LinkedHashMap<>(rec.getFields());
            String keyCol = rec.getKeyColumns().isEmpty() ? null : rec.getKeyColumns().get(0);
            if (keyCol != null) {
                fields.put(keyCol, businessKey);
            }
        }
        // 业务键列回填: 采集端字段映射后 fields 可能不含 key 列(如 ext 表只映射 addr/level),
        // 但 ODKU 定位行需要 key 列值 → 从 CdcRecord.businessKey 回填(单键直填; 多键按 '|' 拆分)
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
                    String[] parts = String.valueOf(businessKey).split("\\|", -1);
                    for (int i = 0; i < keys.size() && i < parts.length; i++) {
                        Object v = fields.get(keys.get(i));
                        if (v == null || String.valueOf(v).isBlank()) {
                            fields.put(keys.get(i), parts[i]);
                        }
                    }
                }
            }
        }
        return new AtomicUpsertDao.TableOp(table, rowKind, rec.getKeyColumns(), fields, rec.getSeq(), judge, writer);
    }
}
