package com.dcp.sync;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import java.util.List;
import java.util.Map;

/**
 * 采集任务分派服务(一任务一 job: 任务粒度 = cdc_st_task 行)。
 *
 * <p>职责:
 * ① 建表自愈(cdc_worker/server_id_pool/sync_backfill_run + cdc_st_task 加列 worker_id/server_id_range 幂等);
 * ② worker 选择: cdc_worker 中 enabled=1 且运行任务数最少(无登记 → 回退默认单 URL);
 * ③ server-id 区间: 从 server_id_pool 领取(每任务固定宽 5), 绑定 owner_task_id;
 * ④ 判死迁移: 换 worker_id(server-id 区间不变不漂移)。
 *
 * <p>设计定论(2026-10-12 拍板): 放弃作业族(一源库一 job), 回归一任务一 job——
 * 每任务独立 checkpoint/独立 binlog 流/独立 worker/独立 server-id; 任务只停不删
 * (任务 ID 永不复用), 区间领取后终身跟随任务, 池只增不减。
 */
@Slf4j
@Service
public class CdcTaskDispatchService {

    private final JdbcTemplate metaJdbc;

    /** 默认 SeaTunnel REST 入口(cdc_worker 无登记时的回退) */
    @Value("${dcp.seatunnel.url:http://seatunnel:8081}")
    private String defaultSeatunnelUrl;

    /** 每任务占用的 server-id 宽度(单表 source, 5 个足够 Debezium 内部使用) */
    public static final int SERVER_IDS_PER_TASK = 5;
    /** 池扩容步长(每次自动追加的空闲区间宽度, 可支撑 20 个任务) */
    private static final int POOL_EXTEND_WIDTH = 100;
    /** 池基准起点(历史 sidBase 起点 5400, 保持兼容不冲突) */
    private static final long POOL_BASE_START = 5400L;

    public CdcTaskDispatchService(JdbcTemplate metaJdbc) {
        this.metaJdbc = metaJdbc;
    }

    @PostConstruct
    public void initTables() {
        try {
            metaJdbc.execute("CREATE TABLE IF NOT EXISTS cdc_worker ("
                    + "id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,"
                    + "worker_id VARCHAR(64) NOT NULL UNIQUE,"
                    + "rest_url VARCHAR(255) NOT NULL,"
                    + "enabled TINYINT NOT NULL DEFAULT 1,"
                    + "update_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"
                    + ") ENGINE=InnoDB COMMENT='SeaTunnel worker 实例登记(独立实例定向提交)'");
            metaJdbc.execute("CREATE TABLE IF NOT EXISTS server_id_pool ("
                    + "id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,"
                    + "start_id BIGINT NOT NULL,"
                    + "end_id BIGINT NOT NULL,"
                    + "owner_task_id BIGINT UNSIGNED NULL,"
                    + "update_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,"
                    + "KEY idx_owner (owner_task_id)) ENGINE=InnoDB COMMENT='server-id 区间池(owner=采集任务 task_id)'");
            metaJdbc.execute("CREATE TABLE IF NOT EXISTS sync_backfill_run ("
                    + "id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,"
                    + "logical_table VARCHAR(128) NOT NULL,"
                    + "sync_task_id BIGINT UNSIGNED NOT NULL,"
                    + "task_id BIGINT NOT NULL,"
                    + "datasource_id BIGINT NOT NULL,"
                    + "source_db VARCHAR(64) NOT NULL,"
                    + "source_table VARCHAR(64) NOT NULL,"
                    + "source_col VARCHAR(128) NOT NULL,"
                    + "target_col VARCHAR(128) NOT NULL,"
                    + "cursor_json TEXT NULL,"
                    + "status VARCHAR(16) NOT NULL DEFAULT 'PENDING',"
                    + "total_rows BIGINT NULL,"
                    + "processed_rows BIGINT NOT NULL DEFAULT 0,"
                    + "error_msg VARCHAR(1024) NULL,"
                    + "update_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,"
                    + "KEY idx_status (status), KEY idx_logical (logical_table)"
                    + ") ENGINE=InnoDB COMMENT='字段级回补任务'");
        } catch (Exception e) {
            log.warn("任务分派表初始化失败: {}", e.getMessage());
        }
        // 一任务一 job: cdc_st_task 加 worker_id/server_id_range 列(幂等, 旧库升级)
        try {
            metaJdbc.execute("ALTER TABLE cdc_st_task ADD COLUMN worker_id VARCHAR(64) NULL "
                    + "COMMENT '绑定的 SeaTunnel worker(cdc_worker.worker_id, NULL=默认入口)'");
        } catch (Exception e) {
            // 列已存在 → 幂等忽略
        }
        try {
            metaJdbc.execute("ALTER TABLE cdc_st_task ADD COLUMN server_id_range VARCHAR(64) NULL "
                    + "COMMENT '本任务独占的 server-id 区间(server_id_pool 领取, 迁移不漂移)'");
        } catch (Exception e) {
            // 列已存在 → 幂等忽略
        }
        // 池表 owner 列迁移: 旧 owner_family 语义废弃, 缺 owner_task_id 列时补列
        try {
            metaJdbc.execute("ALTER TABLE server_id_pool ADD COLUMN owner_task_id BIGINT UNSIGNED NULL "
                    + "COMMENT '归属采集任务(cdc_st_task.task_id)'");
        } catch (Exception e) {
            // 列已存在 → 幂等忽略
        }
        // 作业族废弃清理(2026-10-12 拍板): 旧表/旧列删除(幂等, 旧库升级; 运行时状态非业务数据)
        try {
            metaJdbc.execute("DROP TABLE IF EXISTS cdc_job_family");
        } catch (Exception ignore) {
            // 表不存在 → 忽略
        }
        try {
            metaJdbc.execute("ALTER TABLE cdc_st_task DROP COLUMN family_id");
        } catch (Exception ignore) {
            // 列已删除 → 忽略
        }
        try {
            metaJdbc.execute("ALTER TABLE server_id_pool DROP COLUMN owner_family");
        } catch (Exception ignore) {
            // 列已删除 → 忽略
        }
    }

    // ───────────────── worker 清单与 URL ─────────────────

    /** 查 worker 的 REST 入口; 未登记/已下线 → null(调用方回退默认 URL) */
    public String workerUrl(String workerId) {
        if (workerId == null || workerId.isBlank()) {
            return null;
        }
        try {
            List<String> rows = metaJdbc.queryForList(
                    "SELECT rest_url FROM cdc_worker WHERE worker_id = ? AND enabled = 1",
                    String.class, workerId);
            return rows.isEmpty() ? null : rows.get(0);
        } catch (Exception e) {
            return null;
        }
    }

    public String defaultWorkerUrl() {
        return defaultSeatunnelUrl;
    }

    /** 选最闲 worker(排除指定 worker, 判死迁移用): enabled=1 中运行任务数最少(并列取登记最早); 无登记 → null(回退默认) */
    public String pickLeastLoadedWorker(String excludeWorkerId) {
        try {
            List<Map<String, Object>> rows = metaJdbc.queryForList(
                    "SELECT w.worker_id, COUNT(t.task_id) AS tasks FROM cdc_worker w "
                            + "LEFT JOIN cdc_st_task t ON t.worker_id = w.worker_id AND t.running = 1 "
                            + "WHERE w.enabled = 1 AND w.worker_id <> ? GROUP BY w.worker_id, w.id "
                            + "ORDER BY tasks ASC, w.id ASC",
                    excludeWorkerId == null ? "" : excludeWorkerId);
            return rows.isEmpty() ? null : String.valueOf(rows.get(0).get("worker_id"));
        } catch (Exception e) {
            log.warn("worker 选择失败(排除 {}), 回退默认: {}", excludeWorkerId, e.getMessage());
            return null;
        }
    }

    /** 选最闲 worker: enabled=1 中运行任务数最少(并列取登记最早); 无登记 → null(回退默认) */
    public String pickLeastLoadedWorker() {
        return pickLeastLoadedWorker("");
    }

    // ───────────────── server-id 区间池(owner = 任务) ─────────────────

    /**
     * 领取宽度 >= width 的空闲区间(优先最小合适段, 切分后剩余归池);
     * 池中无合适段 → 自动扩容(POOL_EXTEND_WIDTH), 返回 "start-end"。
     */
    public String allocateRange(int width) {
        List<Map<String, Object>> free = metaJdbc.queryForList(
                "SELECT id, start_id, end_id FROM server_id_pool WHERE owner_task_id IS NULL "
                        + "ORDER BY (end_id - start_id) ASC, start_id ASC");
        long segId = -1, segStart = -1, segEnd = -1;
        for (Map<String, Object> row : free) {
            long start = ((Number) row.get("start_id")).longValue();
            long end = ((Number) row.get("end_id")).longValue();
            if (end - start + 1 >= width) {
                segId = ((Number) row.get("id")).longValue();
                segStart = start;
                segEnd = end;
                break;
            }
        }
        if (segId < 0) {
            // 扩容: 基准 = 池内最大 end_id(或 POOL_BASE_START-1), 追加一段
            long base = POOL_BASE_START - 1;
            List<Long> maxEnd = metaJdbc.queryForList("SELECT MAX(end_id) FROM server_id_pool", Long.class);
            if (!maxEnd.isEmpty() && maxEnd.get(0) != null && maxEnd.get(0) >= base) {
                base = maxEnd.get(0);
            }
            metaJdbc.update("INSERT INTO server_id_pool (start_id, end_id, owner_task_id) VALUES (?, ?, NULL)",
                    base + 1, base + Math.max(POOL_EXTEND_WIDTH, width));
            segId = metaJdbc.queryForObject(
                    "SELECT id FROM server_id_pool WHERE start_id = ? AND owner_task_id IS NULL",
                    Long.class, base + 1);
            segStart = base + 1;
            segEnd = base + Math.max(POOL_EXTEND_WIDTH, width);
        }
        long allocStart = segStart;
        long allocEnd = segStart + width - 1;
        if (allocEnd == segEnd) {
            // 整段领取: 直接标记占用(由调用方后续 UPDATE owner; 此处先返回, 调用方需再绑定)
            metaJdbc.update("UPDATE server_id_pool SET owner_task_id = -1 WHERE id = ?", segId);
        } else {
            metaJdbc.update("UPDATE server_id_pool SET start_id = ? WHERE id = ?", allocEnd + 1, segId);
            metaJdbc.update("INSERT INTO server_id_pool (start_id, end_id, owner_task_id) VALUES (?, ?, -1)",
                    allocStart, allocEnd);
        }
        log.info("server-id 区间已领取: {}-{} (宽 {})", allocStart, allocEnd, width);
        return allocStart + "-" + allocEnd;
    }

    /** 区间绑定任务(领取段 owner_task_id=-1 → taskId) */
    public void bindRangeToTask(long taskId, String range) {
        String[] parts = range.split("-");
        if (parts.length != 2) {
            return;
        }
        metaJdbc.update("UPDATE server_id_pool SET owner_task_id = ? WHERE start_id = ? AND owner_task_id = -1",
                taskId, Long.parseLong(parts[0]));
    }

    /** 区间宽度(支持多段逗号分隔求和) */
    public static int rangeWidth(String range) {
        if (range == null || range.isBlank()) {
            return 0;
        }
        int width = 0;
        for (String seg : range.split(",")) {
            String[] parts = seg.trim().split("-");
            if (parts.length == 2) {
                try {
                    width += (int) (Long.parseLong(parts[1]) - Long.parseLong(parts[0]) + 1);
                } catch (NumberFormatException ignore) {
                    // 非法段跳过
                }
            }
        }
        return width;
    }

    /** 任务级领取并绑定 server-id 区间(注册时一次, 终身跟随; 已有则复用) */
    public String ensureRangeForTask(long taskId, String currentRange) {
        if (currentRange != null && !currentRange.isBlank()) {
            return currentRange;
        }
        String range = allocateRange(SERVER_IDS_PER_TASK);
        bindRangeToTask(taskId, range);
        metaJdbc.update("UPDATE cdc_st_task SET server_id_range = ? WHERE task_id = ?", range, taskId);
        return range;
    }

    /** 判死迁移: 换 worker_id(server-id 区间不变不漂移) */
    public void migrateTask(long taskId, String toWorkerId) {
        metaJdbc.update("UPDATE cdc_st_task SET worker_id = ? WHERE task_id = ?", toWorkerId, taskId);
        log.info("采集任务判死迁移: task={} → worker={}", taskId, toWorkerId);
    }
}
