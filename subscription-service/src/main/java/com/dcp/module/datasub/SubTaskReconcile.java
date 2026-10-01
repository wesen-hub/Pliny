package com.dcp.module.datasub;

import lombok.extern.slf4j.Slf4j;
import net.javacrumbs.shedlock.spring.annotation.SchedulerLock;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

/**
 * 订阅域底座任务双向对账(ShedLock 域级锁 subTaskReconcile 多实例互斥; 判据=目标态差异, 判据即重试队列):
 *
 * <p>CP0 创建方向: 生效 push 契约(实时/流式)在 ∧ 底座缺(宽限期 180s 过滤=判活) → 补 deploy(幂等 exists 短路);
 * CP4 删除方向: 底座 sub 任务在 ∧ 契约已删/停用/改档 ∧ 未停 → 补 stopTask(desired_state 收敛后判据自然不再命中)。
 * 与湖域 lakeTaskReconcile 同构(文档 §8)。
 */
@Slf4j
@Component
public class SubTaskReconcile {

    private final JdbcTemplate metaJdbc;
    private final SubTaskDeployer deployer;

    public SubTaskReconcile(JdbcTemplate metaJdbc, SubTaskDeployer deployer) {
        this.metaJdbc = metaJdbc;
        this.deployer = deployer;
    }

    @Scheduled(fixedDelay = 15000, initialDelay = 30000)
    @SchedulerLock(name = "subTaskReconcile", lockAtMostFor = "2m", lockAtLeastFor = "5s")
    public void reconcileBaseTasks() {
        // CP0 创建方向(扫描只读; 宽限期在 SQL 内, 排除「approve 正常登记中」的误判)
        List<Map<String, Object>> missing = metaJdbc.queryForList(
                "SELECT c.id FROM data_contract c "
                        + "WHERE c.push = 'push' AND c.status = 'active' AND c.freq IN ('实时','流式') "
                        + "  AND c.update_time < NOW() - INTERVAL 180 SECOND "
                        + "  AND NOT EXISTS (SELECT 1 FROM consumer_task t "
                        + "      WHERE t.task_id = CONCAT('sub-push-', c.id))");
        for (Map<String, Object> row : missing) {
            try {
                long id = ((Number) row.get("id")).longValue();
                Map<String, Object> r = deployer.deploy(id);
                log.info("[sub-task] 对账补登记: contractId={} → {}",
                        id, Boolean.TRUE.equals(r.get("created")) ? "ok" : r.getOrDefault("error", "skipped"));
            } catch (Exception e) {
                log.warn("[sub-task] 对账补登记失败(下轮判据再命中重试): {}", e.getMessage());
            }
        }

        // CP4 删除方向(契约删/停/改档 → 底座任务在且未停 → 补 stop; stop 后 desired_state 收敛)
        List<Map<String, Object>> orphan = metaJdbc.queryForList(
                "SELECT t.task_id FROM consumer_task t "
                        + "WHERE t.namespace = 'sub' AND t.task_id LIKE 'sub-push-%' "
                        + "  AND t.desired_state <> 'stopped' "
                        + "  AND NOT EXISTS (SELECT 1 FROM data_contract c "
                        + "      WHERE c.push = 'push' AND c.status = 'active' AND c.freq IN ('实时','流式') "
                        + "      AND CONCAT('sub-push-', c.id) = t.task_id)");
        for (Map<String, Object> row : orphan) {
            try {
                String taskId = String.valueOf(row.get("task_id"));
                long id = Long.parseLong(taskId.substring("sub-push-".length()));
                deployer.stopTask(id);
                log.info("[sub-task] 对账补停止: task={} (契约已删/停用/改档)", taskId);
            } catch (Exception e) {
                log.warn("[sub-task] 对账补停止失败(下轮重试): {}", e.getMessage());
            }
        }
    }
}
