package com.dcp.sync;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import java.util.List;
import java.util.Map;

/**
 * 采集作业族与分派服务(一源库一作业族; 源库单点约束 + 判死迁移)。
 *
 * <p>职责:
 * ① 建表自愈(4 张新表 + cdc_st_task.family_id 加列幂等);
 * ② 作业族登记: 确保 (datasource_id, source_db) 唯一归属一个作业族;
 * ③ worker 选择: cdc_worker 中 enabled=1 且作业族数最少(无登记 → 回退默认单 URL, 兼容旧部署);
 * ④ server-id 区间: 从 server_id_pool 领取(宽度=表数×5), 释放回收, 池空自动扩容;
 * ⑤ 判死迁移: 换 worker_id, server-id 区间不变(不漂移)。
 */
@Slf4j
@Service
public class CdcJobFamilyService {

    private final JdbcTemplate metaJdbc;

    /** 默认 SeaTunnel REST 入口(cdc_worker 无登记时的回退, 兼容旧部署) */
    @Value("${dcp.seatunnel.url:http://seatunnel:8081}")
    private String defaultSeatunnelUrl;

    /** 每张源表占用的 server-id 宽度(与旧实现每 job 区间宽 5 一致) */
    public static final int SERVER_IDS_PER_TABLE = 5;
    /** 池扩容步长(每次自动追加的空闲区间宽度, 可支撑 20 张表) */
    private static final int POOL_EXTEND_WIDTH = 100;
    /** 池基准起点(旧实现 sidBase 起点 5400, 保持兼容不冲突) */
    private static final long POOL_BASE_START = 5400L;

    public CdcJobFamilyService(JdbcTemplate metaJdbc) {
        this.metaJdbc = metaJdbc;
    }

    @PostConstruct
    public void initTables() {
        try {
            metaJdbc.execute("CREATE TABLE IF NOT EXISTS cdc_job_family ("
                    + "id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,"
                    + "datasource_id BIGINT NOT NULL,"
                    + "source_db VARCHAR(64) NOT NULL,"
                    + "worker_id VARCHAR(64) NOT NULL,"
                    + "server_id_range VARCHAR(64) NOT NULL,"
                    + "sea_tunnel_job_id VARCHAR(64) NULL,"
                    + "running TINYINT NOT NULL DEFAULT 0,"
                    + "update_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,"
                    + "UNIQUE KEY uk_family (datasource_id, source_db),"
                    + "KEY idx_worker (worker_id)) ENGINE=InnoDB COMMENT='采集作业族: 一源库一作业族'");
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
                    + "owner_family BIGINT UNSIGNED NULL,"
                    + "update_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,"
                    + "KEY idx_owner (owner_family)) ENGINE=InnoDB COMMENT='server-id 区间池'");
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
            log.warn("作业族分派表初始化失败: {}", e.getMessage());
        }
        try {
            metaJdbc.execute("ALTER TABLE cdc_st_task ADD COLUMN family_id BIGINT UNSIGNED NOT NULL DEFAULT 0 "
                    + "COMMENT '所属作业族(cdc_job_family.id, 0=未归属)'");
        } catch (Exception e) {
            // 列已存在(重复执行/旧表已加列) → 幂等忽略
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

    /** 选最闲 worker(排除指定 worker, 判死迁移用): enabled=1 中作业族数最少(并列取登记最早); 无登记 → null(回退默认) */
    public String pickLeastLoadedWorker(String excludeWorkerId) {
        try {
            List<Map<String, Object>> rows = metaJdbc.queryForList(
                    "SELECT w.worker_id, COUNT(f.id) AS families FROM cdc_worker w "
                            + "LEFT JOIN cdc_job_family f ON f.worker_id = w.worker_id "
                            + "WHERE w.enabled = 1 AND w.worker_id <> ? GROUP BY w.worker_id, w.id "
                            + "ORDER BY families ASC, w.id ASC",
                    excludeWorkerId == null ? "" : excludeWorkerId);
            return rows.isEmpty() ? null : String.valueOf(rows.get(0).get("worker_id"));
        } catch (Exception e) {
            log.warn("worker 选择失败(排除 {}), 回退默认: {}", excludeWorkerId, e.getMessage());
            return null;
        }
    }

    /** 选最闲 worker: enabled=1 中作业族数最少(并列取登记最早); 无登记 → null(回退默认) */
    public String pickLeastLoadedWorker() {
        return pickLeastLoadedWorker("");
    }

    // ───────────────── 作业族登记与分派 ─────────────────

    /**
     * 确保 (datasourceId, sourceDb) 已有作业族(无则创建并分派), 返回 familyId。
     * 已有作业族不动绑定(日常加表/加列只动族内配置); server-id 区间宽度不足时换领。
     */
    public long ensureFamily(long datasourceId, String sourceDb, int tableCount) {
        List<Long> ids = metaJdbc.queryForList(
                "SELECT id FROM cdc_job_family WHERE datasource_id = ? AND source_db = ?",
                Long.class, datasourceId, sourceDb);
        if (!ids.isEmpty()) {
            long familyId = ids.get(0);
            reallocRangeIfNeeded(familyId, tableCount);
            return familyId;
        }
        String workerId = pickLeastLoadedWorker();
        if (workerId == null) {
            workerId = "default";
        }
        int width = Math.max(SERVER_IDS_PER_TABLE * tableCount, SERVER_IDS_PER_TABLE);
        String range = allocateRange(width);
        metaJdbc.update("INSERT INTO cdc_job_family (datasource_id, source_db, worker_id, server_id_range) "
                        + "VALUES (?, ?, ?, ?)",
                datasourceId, sourceDb, workerId, range);
        long familyId = metaJdbc.queryForObject(
                "SELECT id FROM cdc_job_family WHERE datasource_id = ? AND source_db = ?",
                Long.class, datasourceId, sourceDb);
        log.info("作业族已创建并分派: family={} ds={} db={} worker={} serverIdRange={}",
                familyId, datasourceId, sourceDb, workerId, range);
        return familyId;
    }

    /** 作业族信息查询(单行: id/datasource_id/source_db/worker_id/server_id_range/sea_tunnel_job_id/running) */
    public Map<String, Object> familyOf(long familyId) {
        List<Map<String, Object>> rows = metaJdbc.queryForList(
                "SELECT id, datasource_id, source_db, worker_id, server_id_range, sea_tunnel_job_id, running "
                        + "FROM cdc_job_family WHERE id = ?", familyId);
        return rows.isEmpty() ? null : rows.get(0);
    }

    /** 查 (datasourceId, sourceDb) 的作业族 id; 无 → 0 */
    public long familyIdOf(long datasourceId, String sourceDb) {
        List<Long> ids = metaJdbc.queryForList(
                "SELECT id FROM cdc_job_family WHERE datasource_id = ? AND source_db = ?",
                Long.class, datasourceId, sourceDb);
        return ids.isEmpty() ? 0L : ids.get(0);
    }

    /** 全部作业族(巡检用, 按 id 升序) */
    public List<Map<String, Object>> listFamilies() {
        return metaJdbc.queryForList(
                "SELECT id, datasource_id, source_db, worker_id, server_id_range, sea_tunnel_job_id, running "
                        + "FROM cdc_job_family ORDER BY id");
    }

    /** 源库下线: 删除绑定并释放 server-id 区间(末表下线时由调用方触发) */
    public void releaseFamily(long familyId) {
        metaJdbc.update("UPDATE server_id_pool SET owner_family = NULL WHERE owner_family = ?", familyId);
        metaJdbc.update("DELETE FROM cdc_job_family WHERE id = ?", familyId);
        log.info("作业族已释放: family={}", familyId);
    }

    // ───────────────── server-id 区间池 ─────────────────

    /**
     * 领取宽度 >= width 的空闲区间(优先最小合适段, 切分后剩余归池);
     * 池中无合适段 → 自动扩容(POOL_EXTEND_WIDTH), 返回 "start-end"。
     */
    public String allocateRange(int width) {
        List<Map<String, Object>> free = metaJdbc.queryForList(
                "SELECT id, start_id, end_id FROM server_id_pool WHERE owner_family IS NULL "
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
            metaJdbc.update("INSERT INTO server_id_pool (start_id, end_id, owner_family) VALUES (?, ?, NULL)",
                    base + 1, base + Math.max(POOL_EXTEND_WIDTH, width));
            segId = metaJdbc.queryForObject(
                    "SELECT id FROM server_id_pool WHERE start_id = ? AND owner_family IS NULL",
                    Long.class, base + 1);
            segStart = base + 1;
            segEnd = base + Math.max(POOL_EXTEND_WIDTH, width);
        }
        long allocStart = segStart;
        long allocEnd = segStart + width - 1;
        if (allocEnd == segEnd) {
            // 整段领取: 直接标记占用(由调用方后续 UPDATE owner; 此处先返回, 调用方需再绑定)
            metaJdbc.update("UPDATE server_id_pool SET owner_family = -1 WHERE id = ?", segId);
        } else {
            metaJdbc.update("UPDATE server_id_pool SET start_id = ? WHERE id = ?", allocEnd + 1, segId);
            metaJdbc.update("INSERT INTO server_id_pool (start_id, end_id, owner_family) VALUES (?, ?, -1)",
                    allocStart, allocEnd);
        }
        log.info("server-id 区间已领取: {}-{} (宽 {})", allocStart, allocEnd, width);
        return allocStart + "-" + allocEnd;
    }

    /** 区间绑定作业族(领取段 owner_family=-1 → familyId) */
    public void bindRangeToFamily(long familyId, String range) {
        String[] parts = range.split("-");
        if (parts.length != 2) {
            return;
        }
        metaJdbc.update("UPDATE server_id_pool SET owner_family = ? WHERE start_id = ? AND owner_family = -1",
                familyId, Long.parseLong(parts[0]));
    }

    /** 换领区间: 表数增长导致宽度不足时(仅作业族未运行时允许); 返回当前区间 */
    public String reallocRangeIfNeeded(long familyId, int tableCount) {
        Map<String, Object> f = familyOf(familyId);
        if (f == null) {
            return null;
        }
        String current = String.valueOf(f.get("server_id_range"));
        int need = SERVER_IDS_PER_TABLE * tableCount;
        if (rangeWidth(current) >= need) {
            return current;
        }
        Object running = f.get("running");
        if (running != null && ((Number) running).intValue() == 1) {
            log.warn("作业族运行中, 区间宽度不足暂不换领(停旧启新时再换): family={} range={} need={}",
                    familyId, current, need);
            return current;
        }
        // 停旧状态: 释放旧区间, 领新宽区间
        metaJdbc.update("UPDATE server_id_pool SET owner_family = NULL WHERE owner_family = ?", familyId);
        String newRange = allocateRange(need);
        bindRangeToFamily(familyId, newRange);
        metaJdbc.update("UPDATE cdc_job_family SET server_id_range = ? WHERE id = ?", newRange, familyId);
        log.info("作业族 server-id 区间换领: family={} {} → {} (表数 {})", familyId, current, newRange, tableCount);
        return newRange;
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

    /** 判死迁移: 换 worker_id(server-id 区间不变不漂移) */
    public void migrateFamily(long familyId, String toWorkerId) {
        metaJdbc.update("UPDATE cdc_job_family SET worker_id = ? WHERE id = ?", toWorkerId, familyId);
        log.info("作业族判死迁移: family={} → worker={}", familyId, toWorkerId);
    }
}
