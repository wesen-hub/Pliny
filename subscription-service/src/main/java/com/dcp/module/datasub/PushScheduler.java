package com.dcp.module.datasub;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 数据订阅 push 定时档自动投递(2026-09-09): freq='定时' 的生效 push 契约,
 * 按 push_interval_min 节拍自动 deliver(webhook/kafka 与手动投递同链路, 失败可见交付记录并在下一节拍重试)。
 * 独立调度线程(不占用 Spring 默认单线程调度器, 避免被长任务阻塞), 30s 扫一次"到期"契约。
 */
@Slf4j
@Component
public class PushScheduler {

    private final JdbcTemplate metaJdbc;
    private final DataSubService dataSubService;
    private ScheduledExecutorService executor;

    public PushScheduler(JdbcTemplate metaJdbc, DataSubService dataSubService) {
        this.metaJdbc = metaJdbc;
        this.dataSubService = dataSubService;
    }

    @PostConstruct
    public void start() {
        executor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "dcp-push-scheduler");
            t.setDaemon(true);
            return t;
        });
        tick();  // 启动即扫一次到期契约(同步, 便于观察)
        executor.scheduleWithFixedDelay(this::tick, 30, 30, TimeUnit.SECONDS);
        log.info("data-sub push 定时调度已启动(30s 扫描)");
    }

    @PreDestroy
    public void stop() {
        if (executor != null) executor.shutdownNow();
    }

    void tick() {
        // ① 定时档到期契约: 增量投递(无游标首轮=全量并初始化游标)
        List<Long> due;
        try {
            due = metaJdbc.queryForList(
                    "SELECT id FROM data_contract WHERE push='push' AND status='active' AND freq='定时'"
                            + " AND push_interval_min IS NOT NULL AND push_interval_min > 0"
                            + " AND (last_deliver_at IS NULL OR last_deliver_at < DATE_SUB(NOW(), INTERVAL push_interval_min MINUTE))",
                    Long.class);
        } catch (Exception e) {
            log.warn("data-sub push tick 扫描失败: {}", e.getMessage());
            return;
        }
        for (Long id : due) {
            try {
                dataSubService.deliver(id, "scheduler", true);
                log.info("data-sub 定时投递完成 contractId={}", id);
            } catch (Exception e) {
                log.warn("data-sub 定时投递失败 contractId={}: {}", id, e.getMessage());
            }
        }
        // ② 失败退避重试(最近一次 push 失败行 retries<3, 按 1m/5m/30m 指数退避; 达 3 次=死信停自动, 手动可重投)
        try {
            List<Long> retry = metaJdbc.queryForList(
                    "SELECT d.contract_id FROM data_sub_delivery d"
                            + " JOIN data_contract c ON c.id=d.contract_id"
                            + " WHERE d.mode='push' AND d.status='fail' AND d.retries<3 AND c.push='push' AND c.status='active'"
                            + " AND d.id=(SELECT MAX(d2.id) FROM data_sub_delivery d2 WHERE d2.contract_id=d.contract_id AND d2.mode='push')"
                            + " AND d.ts < DATE_SUB(NOW(), INTERVAL CASE d.retries WHEN 1 THEN 60 WHEN 2 THEN 300 ELSE 1800 END SECOND)",
                    Long.class);
            for (Long id : retry) {
                try {
                    dataSubService.deliver(id, "retry", true);
                    log.info("data-sub 退避重试完成 contractId={}", id);
                } catch (Exception e) {
                    log.warn("data-sub 退避重试失败 contractId={}: {}", id, e.getMessage());
                }
            }
        } catch (Exception e) {
            log.warn("data-sub 重试扫描失败: {}", e.getMessage());
        }
    }
}
