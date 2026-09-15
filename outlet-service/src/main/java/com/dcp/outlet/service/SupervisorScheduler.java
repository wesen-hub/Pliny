package com.dcp.outlet.service;

import com.dcp.outlet.adapters.JdbcTaskStore;
import com.dcp.outlet.control.Alert;
import com.dcp.outlet.control.Supervisor;
import com.dcp.outlet.control.TaskStore;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * 巡检调度:周期性对账「期望状态 vs 实际状态」与租约 —— **只告警,不自动补齐**(D12)。
 *
 * <p>同时承担"资源回收":租约过期在巡检中被释放。
 */
@Component
public class SupervisorScheduler {

    private final Supervisor supervisor;
    private final TaskStore tasks;

    public SupervisorScheduler(Supervisor supervisor, TaskStore tasks) {
        this.supervisor = supervisor; this.tasks = tasks;
    }

    /** 每 30s 一轮。 */
    @Scheduled(fixedDelay = 30_000L, initialDelay = 30_000L)
    public void tick() {
        supervisor.tick();                                   // 租约过期巡检(全局)
        if (tasks instanceof JdbcTaskStore jdbc) {
            for (String taskId : jdbc.allTaskIds()) {
                List<Alert> alerts = supervisor.tickTask(taskId);
                // 告警已由 Supervisor 推给 AlertSink;此处仅保留计数便于观测
            }
        }
    }
}
