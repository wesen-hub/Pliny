package com.dcp.sync;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

/**
 * lag 监控(V3 §6.1): 定时巡检各逻辑表消费积压, 超过阈值持续告警。
 *
 * <p>扩容建议(按顺序):
 *   ① 实例 &lt; 分区 → 加实例(无副作用);
 *   ② 实例 = 分区 → 加分区(ScaleService.expandPartitions);
 *   ③ 单分区积压 → 加分区 + 拆热点桶; 单 key 热点 → 业务键拆分(seq 兜局部乱序)。
 */
@Slf4j
@Component
public class LagMonitor {

    private final JdbcTemplate metaJdbc;
    private final ScaleService scaleService;
    private final long lagThreshold;
    private final long consecutiveThreshold;
    private int consecutive = 0;

    public LagMonitor(JdbcTemplate metaJdbc, ScaleService scaleService,
                      @Value("${dcp.sync.lag-threshold:10000}") long lagThreshold,
                      @Value("${dcp.sync.lag-consecutive:3}") long consecutiveThreshold) {
        this.metaJdbc = metaJdbc;
        this.scaleService = scaleService;
        this.lagThreshold = lagThreshold;
        this.consecutiveThreshold = consecutiveThreshold;
    }

    @Scheduled(fixedDelayString = "${dcp.sync.lag-check-ms:30000}")
    public void check() {
        List<String> tables;
        try {
            tables = metaJdbc.queryForList("SELECT table_name FROM logical_table", String.class);
        } catch (Exception e) {
            return; // 元库表未就绪
        }
        boolean anyHigh = false;
        for (String t : tables) {
            try {
                Map<String, Object> lag = scaleService.lagOf(t);
                long total = ((Number) lag.get("totalLag")).longValue();
                if (total > lagThreshold) {
                    anyHigh = true;
                    log.warn("[lag 告警] 逻辑表 {} 积压 {} 条(阈值 {}), 分区 {} 个 — 建议: 先加实例, 不足则加分区",
                            t, total, lagThreshold, lag.get("partitionCount"));
                }
            } catch (Exception e) {
                log.debug("lag 巡检跳过 {}: {}", t, e.getMessage());
            }
        }
        if (anyHigh) {
            consecutive++;
            if (consecutive >= consecutiveThreshold) {
                log.error("[lag 持续告警] 连续 {} 轮积压超阈值, 请评估扩容(加实例 → 加分区 → 加 broker)",
                        consecutive);
                // TODO(P3+): 对接告警 webhook(钉钉/企微)
            }
        } else {
            consecutive = 0;
        }
    }
}
