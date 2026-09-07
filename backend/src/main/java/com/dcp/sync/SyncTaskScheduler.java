package com.dcp.sync;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 同步任务调度器(统一任务模型):
 * ① autoStopCheck: full-once/scheduled-batch 任务启动后, 轮询消费组 lag ≈ 0(追上当前)
 *   → 主动停止(SeaTunnel checkpoint 落盘记录位点) → run_status=COMPLETED
 * ② cronTrigger: scheduled-batch 任务按 cron 到点触发(同 jobId + savepoint 从上次位点续跑)
 */
@Slf4j
@Component
public class SyncTaskScheduler {

    private final JdbcTemplate metaJdbc;
    private final CdcStService cdcStService;
    private final KafkaProperties kafkaProperties;
    private final String defaultGroupPrefix = "dcp-sync-";
    /** 逻辑表 → lag 连续为 0 的次数(防抖动误停) */
    private final Map<String, Integer> zeroLagStreak = new ConcurrentHashMap<>();

    public SyncTaskScheduler(JdbcTemplate metaJdbc, CdcStService cdcStService, KafkaProperties kafkaProperties) {
        this.metaJdbc = metaJdbc;
        this.cdcStService = cdcStService;
        this.kafkaProperties = kafkaProperties;
    }

    /** 每 15s: 检查 autoStop 任务是否追上当前(lag≈0 连续 3 次 → 停) */
    @Scheduled(fixedDelayString = "${dcp.sync.scheduler.auto-stop-ms:15000}")
    public void autoStopCheck() {
        List<Map<String, Object>> tasks;
        try {
            tasks = metaJdbc.queryForList(
                    "SELECT DISTINCT logical_table FROM sync_task WHERE run_status = 'RUNNING'");
        } catch (Exception e) {
            return; // 旧表结构(无模式列)启动期容错
        }
        for (Map<String, Object> t : tasks) {
            String logicalTable = String.valueOf(t.get("logical_table"));
            try {
                // 只处理 autoStop 模式任务(与启动时同一逻辑: modeOf 取最新行)
                String mode = cdcStService.modeOf(logicalTable).syncMode();
                if (!"full-once".equals(mode) && !"scheduled-batch".equals(mode)
                        && !"full-scheduled".equals(mode)) {
                    zeroLagStreak.remove(logicalTable);
                    continue;
                }
                long lag = totalLag(logicalTable);
                if (lag <= 0) {
                    int streak = zeroLagStreak.merge(logicalTable, 1, Integer::sum);
                    if (streak >= 3) {
                        log.info("任务 {} 已追上当前(lag=0), 停止并标记完成", logicalTable);
                        cdcStService.stop(logicalTable);
                        metaJdbc.update("UPDATE sync_task SET run_status = 'COMPLETED', last_run_time = NOW() "
                                + "WHERE logical_table = ?", logicalTable);
                        metaJdbc.update("UPDATE cdc_run_history SET end_time = NOW(), status = 'COMPLETED', "
                                + "action = 'auto_complete' WHERE logical_table = ? AND status = 'RUNNING'", logicalTable);
                        metaJdbc.update("INSERT INTO cdc_run_history (logical_table, sync_mode, action, end_time, status, message) "
                                + "VALUES (?, ?, 'auto_complete', NOW(), 'COMPLETED', '已追上当前位点, 自动结束')",
                                logicalTable, mode);
                        zeroLagStreak.remove(logicalTable);
                    }
                } else {
                    zeroLagStreak.remove(logicalTable);
                }
            } catch (Exception e) {
                log.warn("autoStop 检查失败 {}: {}", logicalTable, e.getMessage());
            }
        }
    }

    /** 每分钟: cron 到点的 scheduled-batch 任务 → 触发(同 jobId savepoint 续位点) */
    @Scheduled(cron = "0 * * * * *")
    public void cronTrigger() {
        LocalDateTime now = LocalDateTime.now();
        List<Map<String, Object>> tasks;
        try {
            tasks = metaJdbc.queryForList(
                    "SELECT logical_table, cron_expr FROM sync_task "
                            + "WHERE sync_mode = 'scheduled-batch' AND cron_expr IS NOT NULL AND cron_expr != '' "
                            + "AND run_status IN ('IDLE', 'COMPLETED', 'FAILED')");
        } catch (Exception e) {
            return;
        }
        // 按逻辑表去重(一逻辑表多行共享模式)
        Map<String, String> toTrigger = new LinkedHashMap<>();
        for (Map<String, Object> t : tasks) {
            String cron = String.valueOf(t.get("cron_expr"));
            if (cronMatch(cron, now)) {
                toTrigger.putIfAbsent(String.valueOf(t.get("logical_table")), cron);
            }
        }
        for (Map.Entry<String, String> e : toTrigger.entrySet()) {
            try {
                log.info("定时批量触发: {} (cron={})", e.getKey(), e.getValue());
                cdcStService.start(e.getKey());
            } catch (Exception ex) {
                log.error("定时批量启动失败 {}: {}", e.getKey(), ex.getMessage());
            }
        }
    }

    /**
     * 轻量 cron 匹配(秒 分 时 日 月 周, 6 段; 兼容 5 段 = 前面补 0 秒)。
     * 支持: * 数字 范围a-b 步长斜杠n 列表a,b (周 0/7=周日, 1-6=周一~六)
     */
    static boolean cronMatch(String expr, LocalDateTime now) {
        try {
            String[] parts = expr.trim().split("\\s+");
            if (parts.length == 5) {
                parts = new String[]{"0", parts[0], parts[1], parts[2], parts[3], parts[4]};
            }
            if (parts.length != 6) {
                return false;
            }
            // 秒精度: 当前秒落在匹配秒内才算(避免每分钟触发多次)
            if (!segMatch(parts[0], now.getSecond())) return false;
            if (!segMatch(parts[1], now.getMinute())) return false;
            if (!segMatch(parts[2], now.getHour())) return false;
            if (!segMatch(parts[3], now.getDayOfMonth())) return false;
            if (!segMatch(parts[4], now.getMonthValue())) return false;
            int dow = now.getDayOfWeek().getValue(); // 1-7
            if (!segMatch(parts[5], dow) && !segMatch(parts[5], dow % 7)) return false;
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private static boolean segMatch(String seg, int value) {
        if (seg.equals("*")) return true;
        // 逗号列表
        for (String item : seg.split(",")) {
            String s = item.trim();
            int step = 1;
            int slash = s.indexOf('/');
            if (slash > 0) {
                step = Integer.parseInt(s.substring(slash + 1));
                s = s.substring(0, slash);
            }
            int lo, hi;
            if (s.equals("*")) {
                lo = 0;
                hi = 60;
            } else if (s.contains("-")) {
                String[] r = s.split("-");
                lo = Integer.parseInt(r[0]);
                hi = Integer.parseInt(r[1]);
            } else {
                lo = hi = Integer.parseInt(s);
            }
            if (value >= lo && value <= hi && (value - lo) % step == 0) {
                return true;
            }
        }
        return false;
    }

    /** 逻辑表消费组总 lag(末端位点 - 组提交位点) */
    private long totalLag(String logicalTable) {
        String topic = "dcp." + logicalTable;
        String group = groupOf(logicalTable, topic);
        try (AdminClient admin = AdminClient.create(kafkaProperties.buildAdminProperties(null))) {
            var groupOffsets = admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get();
            if (groupOffsets.isEmpty()) {
                return -1; // 无提交位点(可能刚启动还没写) → 不算追上
            }
            Set<TopicPartition> tps = new HashSet<>();
            for (TopicPartition tp : groupOffsets.keySet()) {
                if (tp.topic().equals(topic)) {
                    tps.add(tp);
                }
            }
            if (tps.isEmpty()) {
                return -1;
            }
            var endOffsets = admin.listOffsets(tps.stream()
                    .collect(java.util.stream.Collectors.toMap(tp -> tp, tp -> OffsetSpec.latest()))).all().get();
            long lag = 0;
            for (Map.Entry<TopicPartition, OffsetAndMetadata> en : groupOffsets.entrySet()) {
                if (en.getKey().topic().equals(topic)) {
                    long end = endOffsets.get(en.getKey()).offset();
                    long committed = en.getValue() == null ? 0 : en.getValue().offset();
                    lag += Math.max(0, end - committed);
                }
            }
            return lag;
        } catch (Exception e) {
            log.warn("lag 查询失败 {}/{}: {}", group, topic, e.getMessage());
            return -1;
        }
    }

    private String groupOf(String logicalTable, String topic) {
        try {
            String g = metaJdbc.queryForObject(
                    "SELECT consumer_group FROM logical_table WHERE table_name = ?", String.class, logicalTable);
            if (g != null && !g.isBlank()) {
                return g;
            }
        } catch (Exception ignore) {
        }
        return defaultGroupPrefix + logicalTable;
    }
}
