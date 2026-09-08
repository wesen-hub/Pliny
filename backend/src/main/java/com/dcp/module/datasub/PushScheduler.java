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
                dataSubService.deliver(id, "scheduler");
                log.info("data-sub 定时投递完成 contractId={}", id);
            } catch (Exception e) {
                log.warn("data-sub 定时投递失败 contractId={}: {}", id, e.getMessage());
            }
        }
    }
}
