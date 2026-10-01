package com.dcp.outlet.service;

import com.dcp.outlet.adapters.JdbcTaskStore;
import com.dcp.outlet.control.Alert;
import com.dcp.outlet.control.Supervisor;
import com.dcp.outlet.control.TaskStore;
import net.javacrumbs.shedlock.spring.annotation.SchedulerLock;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * 巡检调度:周期性对账「期望状态 vs 实际状态」—— **自动修复分场景化**(CP1 重放 onCreated / CP3 补注销,
 * 宽限期 180s 避正常流程执行中;批失败仍人工)。ShedLock 域级锁保证多实例只有一台执行。
 *
 * <p>租约/停滞/失联巡检已由统一调度框架收编(实例 lease 判死 + 快照活性检测),
 * 这里只驱动任务级业务巡检(批失败 / 激活缺失 / 停止残留)。
 */
@Component
public class SupervisorScheduler {

    private final Supervisor supervisor;
    private final TaskStore tasks;

    public SupervisorScheduler(Supervisor supervisor, TaskStore tasks) {
        this.supervisor = supervisor; this.tasks = tasks;
    }

    /** 每 30s 一轮(锁租约上限 2m: 崩遇后最迟自愈的停摆预算)。 */
    @Scheduled(fixedDelay = 30_000L, initialDelay = 30_000L)
    @SchedulerLock(name = "outletSupervisorTick", lockAtMostFor = "2m", lockAtLeastFor = "5s")
    public void tick() {
        if (tasks instanceof JdbcTaskStore jdbc) {
            for (String taskId : jdbc.allTaskIds()) {
                List<Alert> alerts = supervisor.tickTask(taskId);
                // 告警已由 Supervisor 推给 AlertSink;此处仅保留计数便于观测
            }
        }
    }
}
