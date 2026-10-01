package com.dcp.schedulerbridge;

import com.dcp.scheduler.model.Enums;
import com.dcp.scheduler.model.TaskSpec;
import com.dcp.sync.LogicalTableConfigService;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import net.javacrumbs.shedlock.spring.annotation.SchedulerLock;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 统一调度框架提交端(方案 C1 ① 终态:backend 不再内嵌调度实例,也不直连 etcd)。
 *
 * <pre>
 *   提交链路(方案 §9):backend → dcp-worker REST POST /tasks → etcd → ns=sync 分派 → dcp-synclink 执行。
 *   外壳职责:① 逻辑表保存 → 提交 TaskSpec ② 启动扫描元库重建 TaskSpec(对账) ③ 逻辑表删除 → 注销 spec
 *             ④ 扩容建议消费:框架写 events/scale-up 事件 → 外壳扩 Kafka 分区 + 回写 spec.partitions。
 *   旧链路(SyncTopicRegistry 消费组自治)已下线:消费端唯一入口 = 框架, 执行器来自业务 jar。
 * </pre>
 */
@Slf4j
@Component
public class SchedulerRuntime {

    /** 自动扩容上限系数:maxPartitions = 建表分区数 × 系数(演示缺省 2)。 */
    @Value("${dcp.scheduler.sync-max-factor:2}")
    private int syncMaxFactor;

    private final KafkaProperties kafkaProperties;
    private final JdbcTemplate metaJdbc;
    private final WorkerClient worker;
    private ScheduledExecutorService scaleSuggestConsumer;

    public SchedulerRuntime(KafkaProperties kafkaProperties,
                            JdbcTemplate metaJdbc,
                            WorkerClient worker) {
        this.kafkaProperties = kafkaProperties;
        this.metaJdbc = metaJdbc;
        this.worker = worker;
    }

    @PostConstruct
    public void start() {
        // 扩容建议消费:框架建议(events/scale-up)→ 外壳扩 Kafka 分区 + 更新 spec.partitions
        startScaleSuggestConsumer();
        // 外壳职责:启动扫描元库,重建全部 TaskSpec(框架从 etcd 自愈,不依赖外壳进程常驻)
        resubmitAll();
        log.info("调度提交端就绪: ns=sync 任务提交走 dcp-worker REST");
    }

    @PreDestroy
    public void stop() {
        if (scaleSuggestConsumer != null) {
            scaleSuggestConsumer.shutdownNow();
        }
    }

    /** 框架托管是否生效(外壳提交的共同开关):worker 提交端恒可用。 */
    public boolean isEnabled() {
        return true;
    }

    /** 外壳提交:逻辑表保存/变更 → TaskSpec(接入设计 §四 ②;重复提交 = 更新)。 */
    public void submitSyncTask(LogicalTableConfigService.LogicalTableConfig cfg) {
        int pc = cfg.getPartitionCount() > 0 ? cfg.getPartitionCount() : 8;
        ensureTopicPartitions(cfg.getTopic(), pc);
        TaskSpec spec = new TaskSpec();
        spec.namespace = "sync";
        spec.taskId = cfg.getTableName();
        spec.topic = cfg.getTopic();
        spec.partitions = pc;
        // 自动扩缩开启:min=建表分区数(不缩破建表规模),max=pc×系数(框架建议→外壳扩 Kafka 分区)
        spec.minPartitions = pc;
        spec.maxPartitions = Math.max(pc, pc * Math.max(1, syncMaxFactor));
        spec.targetRps = 1000;
        spec.scaleDownHooks = "sync-noop";   // 缩容钩子:无生产路由,恒放行
        spec.handlerRef = new TaskSpec.HandlerRef();
        spec.handlerRef.type = Enums.HandlerType.SPI;
        spec.handlerRef.ref = "sync-writer";
        spec.handover = Enums.Handover.AGGRESSIVE;   // ODKU 幂等,崩溃重放吸收
        spec.bizMeta = new HashMap<>();
        spec.bizMeta.put("consumerGroup", cfg.getConsumerGroup() == null ? "" : cfg.getConsumerGroup());
        spec.bizMeta.put("tableCount", String.valueOf(cfg.getTableCount()));
        spec.bizMeta.put("tablePrefix", cfg.getTablePrefix() == null ? "" : cfg.getTablePrefix());
        worker.submitSpec(spec);
        log.info("[scheduler] 提交 sync TaskSpec: task={} topic={} partitions={} (max={})",
                cfg.getTableName(), cfg.getTopic(), pc, spec.maxPartitions);
    }

    /** 逻辑表删除 → 注销 spec(框架推进 DRAINING → 回收执行器与归属)。 */
    public void cancelSyncTask(String logicalTable) {
        worker.deleteSpec("sync", logicalTable);
        log.info("[scheduler] 注销 sync TaskSpec: task={}", logicalTable);
    }

    /** 启动重建:元库全部逻辑表 → TaskSpec(重启/etcd 丢失后框架自愈)。 */
    private void resubmitAll() {
        try {
            List<Map<String, Object>> tables = metaJdbc.queryForList(
                    "SELECT table_name, topic, consumer_group, partition_count FROM logical_table");
            for (Map<String, Object> row : tables) {
                LogicalTableConfigService.LogicalTableConfig cfg = cfgOf(row);
                if (cfg.getTopic() != null && !cfg.getTopic().isBlank()) {
                    submitSyncTask(cfg);
                }
            }
        } catch (Exception e) {
            log.warn("[scheduler] 启动扫描逻辑表失败(元库可能为空): {}", e.getMessage());
        }
    }

    /**
     * 运行期对账(CP-M 系列, 判据即重试队列; ShedLock 域级锁多实例互斥):
     * CP-M1 topic: logical_table 在(宽限期 180s) → ensureTopicPartitions 幂等补建(含分区扩);
     * CP-M2 spec: 同批行中框架 spec 缺 → 补 submitSyncTask(重复提交 = 更新);
     * CP-M3 删除: 框架 spec 在 ∧ logical_table 已删 → 补 cancelSyncTask(deleteSpec 幂等)。
     */
    @Scheduled(fixedDelay = 30_000, initialDelay = 60_000)
    @SchedulerLock(name = "syncModelReconcile", lockAtMostFor = "2m", lockAtLeastFor = "5s")
    public void reconcileSyncTasks() {
        try {
            // CP-M1/CP-M2 正向: 宽限期 180s 过滤(隔离「正常创建流程执行中」的误判)
            List<Map<String, Object>> tables = metaJdbc.queryForList(
                    "SELECT table_name, topic, consumer_group, partition_count FROM logical_table "
                            + "WHERE updated_at < NOW() - INTERVAL 180 SECOND");
            for (Map<String, Object> row : tables) {
                LogicalTableConfigService.LogicalTableConfig cfg = cfgOf(row);
                if (cfg.getTopic() == null || cfg.getTopic().isBlank()) {
                    continue;
                }
                ensureTopicPartitions(cfg.getTopic(), cfg.getPartitionCount());   // CP-M1
                if (worker.getSpec("sync", cfg.getTableName()).isEmpty()) {        // CP-M2
                    submitSyncTask(cfg);
                }
            }
            // CP-M3 反向: spec 在 ∧ 表删 → 补注销(删除动作瞬时无长流程窗口, 免宽限期)
            Set<String> existing = new HashSet<>();
            for (Map<String, Object> row : metaJdbc.queryForList("SELECT table_name FROM logical_table")) {
                existing.add(String.valueOf(row.get("table_name")));
            }
            for (String taskId : worker.listTasks("sync")) {
                if (!existing.contains(taskId)) {
                    log.info("[scheduler] 对账发现孤儿 sync spec, 补注销: task={}", taskId);
                    cancelSyncTask(taskId);
                }
            }
        } catch (Exception e) {
            log.warn("[scheduler] sync 域对账失败(下轮重试): {}", e.getMessage());
        }
    }

    private LogicalTableConfigService.LogicalTableConfig cfgOf(Map<String, Object> row) {
        LogicalTableConfigService.LogicalTableConfig cfg =
                new LogicalTableConfigService.LogicalTableConfig();
        cfg.setTableName(String.valueOf(row.get("table_name")));
        cfg.setTopic(row.get("topic") == null ? null : String.valueOf(row.get("topic")));
        cfg.setConsumerGroup(row.get("consumer_group") == null ? null : String.valueOf(row.get("consumer_group")));
        Object pc = row.get("partition_count");
        cfg.setPartitionCount(pc == null ? 0 : ((Number) pc).intValue());
        return cfg;
    }

    /** 确保 topic 分区数 ≥ partitionCount(sync-writer 执行器 assign 指定分区的前提);支持扩容。 */
    private void ensureTopicPartitions(String topic, int partitionCount) {
        if (topic == null || topic.isBlank()) {
            return;
        }
        try (AdminClient admin = AdminClient.create(kafkaProperties.buildAdminProperties(null))) {
            var desc = admin.describeTopics(List.of(topic)).allTopicNames().get(5, TimeUnit.SECONDS);
            var existing = desc.get(topic);
            if (existing == null) {
                admin.createTopics(List.of(new NewTopic(topic, partitionCount, (short) 1)))
                        .all().get(30, TimeUnit.SECONDS);
            } else if (existing.partitions().size() < partitionCount) {
                admin.createPartitions(Map.of(topic, NewPartitions.increaseTo(partitionCount)))
                        .all().get(30, TimeUnit.SECONDS);
                log.info("[scheduler] topic 扩分区: {} → {} 分区", topic, partitionCount);
            }
        } catch (Exception e) {
            log.warn("[scheduler] topic 分区确保失败 {}: {}", topic, e.getMessage());
        }
    }

    // ── 扩容建议消费(框架建议 events/scale-up → 外壳扩 Kafka 分区) ──

    /** 启动扩容建议消费者:30s 轮询(设计 §11.2 容忍秒~分钟级延迟),处理成功删事件键。 */
    private void startScaleSuggestConsumer() {
        scaleSuggestConsumer = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "scale-suggest-consumer");
            t.setDaemon(true);
            return t;
        });
        scaleSuggestConsumer.scheduleWithFixedDelay(this::consumeScaleSuggestions, 30, 30, TimeUnit.SECONDS);
    }

    private void consumeScaleSuggestions() {
        try {
            // worker 读 API:全部任务的 scale-up 事件(不再直连 etcd,方案 §11 依赖矩阵)
            for (Map<String, Object> item : worker.listEvents("sync", "scale-up")) {
                String taskId = String.valueOf(item.get("taskId"));
                Object evObj = item.get("event");
                if (!(evObj instanceof Map)) {
                    continue;
                }
                @SuppressWarnings("unchecked")
                Map<String, Object> ev = (Map<String, Object>) evObj;
                Object desireObj = ev.get("desire");
                if (desireObj == null) {
                    continue;
                }
                int desire = ((Number) desireObj).intValue();
                var specOpt = worker.getSpec("sync", taskId);
                if (specOpt.isEmpty()) {
                    continue;
                }
                TaskSpec spec = specOpt.get();
                ensureTopicPartitions(spec.topic, desire);
                // 扩分区完成 → 更新 spec.partitions(与 Kafka 实际分区数对齐,框架按新分区数对账)
                spec.partitions = desire;
                worker.submitSpec(spec);
                worker.deleteEvent("sync", taskId, "scale-up");
                log.info("[scheduler] 扩容建议已执行: task={} topic={} → {} 分区", taskId, spec.topic, desire);
            }
        } catch (Exception ex) {
            log.warn("[scheduler] 扩容建议消费失败(下轮重试): {}", ex.getMessage());
        }
    }
}
