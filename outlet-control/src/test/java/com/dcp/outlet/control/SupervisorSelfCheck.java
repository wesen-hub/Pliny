package com.dcp.outlet.control;

import com.dcp.outlet.control.support.InMemoryTaskStore;
import com.dcp.outlet.control.support.InMemoryUnitStore;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 巡检器自检:**平台只告警、不自动补齐**(D12)。
 * 覆盖:租约过期 / 单元缺失 / 长时间无心跳 / 已下发但从未上报 / 期望运行却无 RUNNING / 批失败。
 */
public final class SupervisorSelfCheck {

    private static final String TASK = "t1";
    private static final String TOPIC = "dcp.cdc.target_db.rt_sync_c";
    private static int passed, failed;

    /** 内存单元状态 + 告警收集。 */
    private static final class MemStatus implements UnitStatusStore {
        final Map<String, List<UnitStatus>> byTask = new HashMap<>();

        @Override public List<UnitStatus> byTask(String taskId) {
            return byTask.getOrDefault(taskId, List.of());
        }

        @Override public List<UnitStatus> all() {
            List<UnitStatus> out = new ArrayList<>();
            byTask.values().forEach(out::addAll);
            return out;
        }

        void put(UnitStatus s) {
            byTask.computeIfAbsent(s.taskId, k -> new ArrayList<>()).add(s);
        }
    }

    public static void main(String[] args) {
        testLeaseExpired();
        testUnitMissing();
        testStalled();
        testNeverReportedAfterDispatch();
        testTaskNotRunning();
        testBatchFailed();
        testHealthyNoAlert();

        System.out.println();
        System.out.println("==================================================");
        System.out.println("  通过 " + passed + " 项, 失败 " + failed + " 项");
        System.out.println("==================================================");
        if (failed > 0) System.exit(1);
    }

    private static void testLeaseExpired() {
        Fixture f = new Fixture();
        f.leases.grant("u1", TASK, TOPIC, 0, 1, 1000L, 0L);          // 1000ms 后过期
        List<Alert> alerts = f.supervisor().tick();                    // now = 5000
        check("租约过期告警", alerts.stream().anyMatch(a -> a.kind() == Alert.Kind.LEASE_EXPIRED), true);
        check("过期后租约被释放", f.leases.activeCount(), 0);
    }

    private static void testUnitMissing() {
        Fixture f = new Fixture();
        f.task.status(TaskStatus.RUNNING).desiredState("running");
        f.units.insert(TASK, List.of(new PlannedUnit("u-missing", "rt_sync_c", TOPIC, List.of(0))));
        // 状态表里没有 u-missing
        List<Alert> alerts = f.supervisor().tickTask(TASK);
        check("单元缺失告警", alerts.stream().anyMatch(a -> a.kind() == Alert.Kind.UNIT_MISSING), true);
    }

    private static void testStalled() {
        Fixture f = new Fixture();
        f.task.status(TaskStatus.RUNNING).desiredState("running");
        f.units.insert(TASK, List.of(new PlannedUnit("u1", "rt_sync_c", TOPIC, List.of(0))));
        f.status.put(new UnitStatusStore.UnitStatus("u1", TASK, "RUNNING", 100, 999, 1_000L, 0L));  // 上次心跳 1s
        List<Alert> alerts = f.supervisor().tickTask(TASK);            // now = 5000, stall=3000
        check("停滞告警", alerts.stream().anyMatch(a -> a.kind() == Alert.Kind.UNIT_STALLED), true);
    }

    private static void testNeverReportedAfterDispatch() {
        Fixture f = new Fixture();
        f.task.status(TaskStatus.RUNNING).desiredState("running");
        f.units.insert(TASK, List.of(new PlannedUnit("u1", "rt_sync_c", TOPIC, List.of(0))));
        f.status.put(new UnitStatusStore.UnitStatus("u1", TASK, "DISPATCHED", 0, 0, 0L, 4_500L));    // 0.5s 前下发(宽限 3s 内)
        List<Alert> alerts = f.supervisor().tickTask(TASK);            // 宽限 3000ms → 未超
        check("下发宽限内不告警", alerts.stream().noneMatch(a -> a.kind() == Alert.Kind.UNIT_STALLED), true);

        Fixture f2 = new Fixture();
        f2.task.status(TaskStatus.RUNNING).desiredState("running");
        f2.units.insert(TASK, List.of(new PlannedUnit("u1", "rt_sync_c", TOPIC, List.of(0))));
        f2.status.put(new UnitStatusStore.UnitStatus("u1", TASK, "DISPATCHED", 0, 0, 0L, 1L));       // 早已下发(远超宽限)
        List<Alert> a2 = f2.supervisor().tickTask(TASK);
        check("超宽限未上报 → 告警", a2.stream().anyMatch(a -> a.kind() == Alert.Kind.UNIT_STALLED), true);
    }

    private static void testTaskNotRunning() {
        Fixture f = new Fixture();
        f.task.status(TaskStatus.RUNNING).desiredState("running");
        f.units.insert(TASK, List.of(new PlannedUnit("u1", "rt_sync_c", TOPIC, List.of(0))));
        f.status.put(new UnitStatusStore.UnitStatus("u1", TASK, "PAUSED", 10, 100, 4_000L, 0L));
        List<Alert> alerts = f.supervisor().tickTask(TASK);
        check("期望运行但无 RUNNING 告警",
                alerts.stream().anyMatch(a -> a.kind() == Alert.Kind.TASK_NOT_RUNNING), true);
    }

    private static void testBatchFailed() {
        Fixture f = new Fixture();
        f.task.status(TaskStatus.BATCH_FAILED).desiredState("running");
        List<Alert> alerts = f.supervisor().tickTask(TASK);
        check("批失败告警(需人工重做批)",
                alerts.stream().anyMatch(a -> a.kind() == Alert.Kind.BATCH_FAILED), true);
    }

    private static void testHealthyNoAlert() {
        Fixture f = new Fixture();
        f.task.status(TaskStatus.RUNNING).desiredState("running");
        f.units.insert(TASK, List.of(new PlannedUnit("u1", "rt_sync_c", TOPIC, List.of(0))));
        f.status.put(new UnitStatusStore.UnitStatus("u1", TASK, "RUNNING", 10, 100, 4_900L, 0L));
        List<Alert> alerts = f.supervisor().tickTask(TASK);
        check("健康任务零告警", alerts.size(), 0);
    }

    private static final class Fixture {
        final InMemoryTaskStore tasks = new InMemoryTaskStore();
        final InMemoryUnitStore units = new InMemoryUnitStore();
        final MemStatus status = new MemStatus();
        final LeaseManager leases = new LeaseManager();
        final List<Alert> fired = new ArrayList<>();
        final TaskRecord task = new TaskRecord(TASK).consumerGroup("task-t1");

        Fixture() {
            tasks.insert(task);
            tasks.update(task);
        }

        Supervisor supervisor() {
            return new Supervisor(tasks, units, status, leases, fired::add, 1_000L, 3_000L, 3_000L, () -> 5_000L);
        }
    }

    private static void check(String what, Object actual, Object expected) {
        boolean ok = String.valueOf(actual).equals(String.valueOf(expected));
        if (ok) passed++; else failed++;
        System.out.println((ok ? "  PASS  " : "  FAIL  ") + what + " = " + actual
                + (ok ? "" : "   (期望 " + expected + ")"));
    }
}
