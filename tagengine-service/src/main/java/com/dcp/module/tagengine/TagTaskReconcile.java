package com.dcp.module.tagengine;

import lombok.extern.slf4j.Slf4j;
import net.javacrumbs.shedlock.spring.annotation.SchedulerLock;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

/**
 * 打标域底座任务双向对账(ShedLock 域级锁 tagTaskReconcile 多实例互斥; 判据=目标态差异, 判据即重试队列):
 *
 * <p>CP0 创建方向: 已发布打标任务(published)在 ∧ 底座缺(宽限期 180s 过滤=判活) → 补 deploy(幂等 exists 短路);
 * CP4 删除方向: 底座 tag 任务在 ∧ 打标任务已删/停用 ∧ 未停 → 补 stopTask(desired_state 收敛后判据自然不再命中)。
 * 与湖域 lakeTaskReconcile/订阅域 subTaskReconcile 同构(文档 §8)。
 */
@Slf4j
@Component
public class TagTaskReconcile {

    private final JdbcTemplate metaJdbc;
    private final TagV2Deployer deployer;

    public TagTaskReconcile(JdbcTemplate metaJdbc, TagV2Deployer deployer) {
        this.metaJdbc = metaJdbc;
        this.deployer = deployer;
    }

    @Scheduled(fixedDelay = 15000, initialDelay = 30000)
    @SchedulerLock(name = "tagTaskReconcile", lockAtMostFor = "2m", lockAtLeastFor = "5s")
    public void reconcileBaseTasks() {
        // CP0 创建方向(扫描只读; 宽限期在 SQL 内, 排除「发布后正常登记中」的误判)
        List<Map<String, Object>> missing = metaJdbc.queryForList(
                "SELECT t.id FROM tag_task t "
                        + "WHERE t.status = 'published' "
                        + "  AND t.update_time < NOW() - INTERVAL 180 SECOND "
                        + "  AND NOT EXISTS (SELECT 1 FROM consumer_task c "
                        + "      WHERE c.task_id = CONCAT('tag-task-', t.id))");
        for (Map<String, Object> row : missing) {
            try {
                long id = ((Number) row.get("id")).longValue();
                Map<String, Object> r = deployer.deploy(id);
                log.info("[tag-v2] 对账补登记: taskId={} → {}",
                        id, Boolean.TRUE.equals(r.get("created")) ? "ok" : r.getOrDefault("skipped", "?"));
            } catch (Exception e) {
                log.warn("[tag-v2] 对账补登记失败(下轮判据再命中重试): {}", e.getMessage());
            }
        }

        // CP4 删除方向(任务删/停 → 底座任务在且未停 → 补 stop; stop 后 desired_state 收敛)
        List<Map<String, Object>> orphan = metaJdbc.queryForList(
                "SELECT c.task_id FROM consumer_task c "
                        + "WHERE c.namespace = 'tag' AND c.task_id LIKE 'tag-task-%' "
                        + "  AND c.desired_state <> 'stopped' "
                        + "  AND NOT EXISTS (SELECT 1 FROM tag_task t "
                        + "      WHERE t.status = 'published' "
                        + "      AND CONCAT('tag-task-', t.id) = c.task_id)");
        for (Map<String, Object> row : orphan) {
            try {
                String taskId = String.valueOf(row.get("task_id"));
                long id = Long.parseLong(taskId.substring("tag-task-".length()));
                deployer.stopTask(id);
                log.info("[tag-v2] 对账补停止: task={} (打标任务已删/停用)", taskId);
            } catch (Exception e) {
                log.warn("[tag-v2] 对账补停止失败(下轮重试): {}", e.getMessage());
            }
        }
    }
}
