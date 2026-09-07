package com.dcp.sync;

import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;

/**
 * V3 同步链路 schema 迁移(幂等, 启动时执行)。
 *
 * <p>采集端收敛(2026-08-31): 自研 CdcCollector 退役, 采集统一走 SeaTunnel(cdc_st_task)。
 * 迁移: ① cdc_st_task 旧结构(logical_table 主键) → task_id 自增主键 +
 * UNIQUE(logical_table, source_db, source_table) + field_mapping 列(多表合并 + 字段映射)。
 */
@Slf4j
@Component
public class CdcStSchemaMigrator {

    private final JdbcTemplate metaJdbc;

    public CdcStSchemaMigrator(JdbcTemplate metaJdbc) {
        this.metaJdbc = metaJdbc;
    }

    @PostConstruct
    public void migrate() {
        try {
            Integer hasTable = metaJdbc.queryForObject(
                    "SELECT COUNT(*) FROM information_schema.TABLES "
                            + "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'cdc_st_task'",
                    Integer.class);
            if (hasTable == null || hasTable == 0) {
                return; // 全新环境, schema.sql 会建新结构
            }
            Integer hasTaskId = metaJdbc.queryForObject(
                    "SELECT COUNT(*) FROM information_schema.COLUMNS "
                            + "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'cdc_st_task' AND COLUMN_NAME = 'task_id'",
                    Integer.class);
            if (hasTaskId != null && hasTaskId > 0) {
                ensureRowChainColumn();
                ensureDltPayloadText();
                ensureLogicalTargetColumn();
                ensureSyncTaskModeColumns();
                ensureLogicalModeColumns();
                ensureRunHistoryTable();
                ensureModelTable();
                return; // 已是新结构
            }
            log.warn("检测到旧结构 cdc_st_task(主键=logical_table), 迁移为 task_id 主键 + 多源唯一键 + field_mapping...");
            metaJdbc.execute("ALTER TABLE cdc_st_task DROP PRIMARY KEY, "
                    + "ADD COLUMN task_id BIGINT NOT NULL AUTO_INCREMENT, "
                    + "ADD COLUMN field_mapping JSON NULL, "
                    + "ADD PRIMARY KEY (task_id), "
                    + "ADD UNIQUE KEY uk_src (logical_table, source_db, source_table)");
            ensureRowChainColumn();
            ensureDltPayloadText();
            log.info("cdc_st_task 迁移完成: task_id 主键 + uk_src + field_mapping + row_chain");
        } catch (Exception e) {
            log.error("cdc_st_task schema 迁移失败(不影响启动, 后续写入可能报错): {}", e.getMessage());
        }
    }

    /** dlt_message.payload 兼容非法 JSON(TEXT 而非 JSON 类型) */
    private void ensureDltPayloadText() {
        try {
            Integer hasTable = metaJdbc.queryForObject(
                    "SELECT COUNT(*) FROM information_schema.TABLES "
                            + "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'dlt_message'",
                    Integer.class);
            if (hasTable == null || hasTable == 0) {
                return;
            }
            metaJdbc.execute("ALTER TABLE dlt_message MODIFY COLUMN payload TEXT NULL");
        } catch (Exception e) {
            log.warn("dlt_message.payload 类型调整失败: {}", e.getMessage());
        }
    }

    /** 补 row_chain 列(2026-08-31 行级转换: 仅 filter/filter-kind, 采集端执行) */
    /** logical_table 补 target_database 列(多目标库路由; 默认 target_db) */
    private void ensureLogicalTargetColumn() {
        try {
            Integer hasCol = metaJdbc.queryForObject(
                    "SELECT COUNT(*) FROM information_schema.COLUMNS "
                            + "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'logical_table' AND COLUMN_NAME = 'target_database'",
                    Integer.class);
            if (hasCol != null && hasCol == 0) {
                metaJdbc.execute("ALTER TABLE logical_table ADD COLUMN target_database VARCHAR(64) "
                        + "NOT NULL DEFAULT 'target_db' COMMENT '目标库(多目标库路由)'");
                log.info("logical_table 补列 target_database");
            }
        } catch (Exception e) {
            log.warn("补 target_database 列失败: {}", e.getMessage());
        }
    }

    /** 模型定义表(建模入口: 建模型 = 定义逻辑表 + 建 topic/分区 + 预建物理表) */
    private void ensureModelTable() {
        try {
            metaJdbc.execute("CREATE TABLE IF NOT EXISTS model_definition ("
                    + "id BIGINT AUTO_INCREMENT PRIMARY KEY, "
                    + "name VARCHAR(128) NOT NULL COMMENT '模型名', "
                    + "layer VARCHAR(16) NOT NULL DEFAULT 'DWD' COMMENT 'ODS/DWD/DWS/ADS', "
                    + "domain VARCHAR(64) NULL COMMENT '业务域', "
                    + "table_name VARCHAR(128) NOT NULL COMMENT '逻辑表(物理分表前缀)', "
                    + "topic VARCHAR(128) NOT NULL COMMENT 'Kafka topic', "
                    + "bucket_count INT NOT NULL DEFAULT 4096, "
                    + "partition_count INT NOT NULL DEFAULT 8 COMMENT 'Kafka 分区数', "
                    + "table_count INT NOT NULL DEFAULT 50 COMMENT '物理分表数', "
                    + "key_columns JSON NULL COMMENT '业务键', "
                    + "fields JSON NULL COMMENT '模型字段定义(name/type/desc)', "
                    + "description VARCHAR(512) NULL, "
                    + "create_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP, "
                    + "UNIQUE KEY uk_table (table_name)"
                    + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='数据模型定义(发布即建 topic/物理表)'");
        } catch (Exception e) {
            log.warn("建 model_definition 失败: {}", e.getMessage());
        }
    }

    /** 任务执行历史表(V3 任务维度: 每次 启动/停止/自动完成) */
    private void ensureRunHistoryTable() {
        try {
            metaJdbc.execute("CREATE TABLE IF NOT EXISTS cdc_run_history ("
                    + "id BIGINT AUTO_INCREMENT PRIMARY KEY, "
                    + "logical_table VARCHAR(128) NOT NULL, "
                    + "sync_mode VARCHAR(16) NULL COMMENT 'full-cdc/incremental/full-once/full-scheduled/scheduled-batch', "
                    + "job_id VARCHAR(64) NULL COMMENT 'SeaTunnel jobId', "
                    + "action VARCHAR(16) NOT NULL COMMENT 'start/stop/auto_complete/fail', "
                    + "start_time DATETIME NULL, end_time DATETIME NULL, "
                    + "status VARCHAR(16) NULL COMMENT 'RUNNING/COMPLETED/STOPPED/FAILED', "
                    + "message VARCHAR(512) NULL, "
                    + "KEY idx_logical (logical_table, id), KEY idx_time (end_time)"
                    + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='同步任务执行历史'");
        } catch (Exception e) {
            log.warn("建 cdc_run_history 失败: {}", e.getMessage());
        }
    }

    /** logical_table 补任务级模式列(模式挂任务=逻辑表, 不挂源表行) */
    private void ensureLogicalModeColumns() {
        addColumnIfAbsent("logical_table", "sync_mode", "VARCHAR(16) NOT NULL DEFAULT 'full-cdc' COMMENT '同步模式: full-once/scheduled-batch/full-cdc/incremental'");
        addColumnIfAbsent("logical_table", "full_scope", "VARCHAR(16) NULL COMMENT 'full-once 范围: snapshot-only/snapshot-incr'");
        addColumnIfAbsent("logical_table", "cron_expr", "VARCHAR(64) NULL COMMENT '定时批量执行时间(cron)'");
        addColumnIfAbsent("logical_table", "start_mode", "VARCHAR(16) NOT NULL DEFAULT 'earliest' COMMENT '增量起始位点'");
        addColumnIfAbsent("logical_table", "start_value", "VARCHAR(64) NULL COMMENT '起始位点值'");
        addColumnIfAbsent("logical_table", "run_status", "VARCHAR(16) NOT NULL DEFAULT 'IDLE' COMMENT 'IDLE/RUNNING/COMPLETED/FAILED'");
        addColumnIfAbsent("logical_table", "last_run_time", "DATETIME NULL COMMENT '上次执行完成时间'");
    }

    /** sync_task 补同步模式/调度/位点列(统一任务模型, 去 V2) */
    private void ensureSyncTaskModeColumns() {
        addColumnIfAbsent("sync_task", "sync_mode", "VARCHAR(16) NOT NULL DEFAULT 'full-cdc' COMMENT '同步模式: full-once/scheduled-batch/full-cdc/incremental'");
        addColumnIfAbsent("sync_task", "full_scope", "VARCHAR(16) NULL COMMENT 'full-once 范围: snapshot-only/snapshot-incr'");
        addColumnIfAbsent("sync_task", "cron_expr", "VARCHAR(64) NULL COMMENT '定时批量执行时间(cron)'");
        addColumnIfAbsent("sync_task", "start_mode", "VARCHAR(16) NOT NULL DEFAULT 'earliest' COMMENT '增量起始位点: earliest/latest/timestamp/specific'");
        addColumnIfAbsent("sync_task", "start_value", "VARCHAR(64) NULL COMMENT '起始位点值(timestamp/specific)'");
        addColumnIfAbsent("sync_task", "last_run_time", "DATETIME NULL COMMENT '上次执行完成时间'");
        addColumnIfAbsent("sync_task", "run_status", "VARCHAR(16) NOT NULL DEFAULT 'IDLE' COMMENT 'IDLE/RUNNING/COMPLETED/FAILED'");
    }

    private void addColumnIfAbsent(String table, String column, String ddl) {
        try {
            Integer hasCol = metaJdbc.queryForObject(
                    "SELECT COUNT(*) FROM information_schema.COLUMNS "
                            + "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND COLUMN_NAME = ?",
                    Integer.class, table, column);
            if (hasCol != null && hasCol == 0) {
                metaJdbc.execute("ALTER TABLE " + table + " ADD COLUMN " + column + " " + ddl);
                log.info("{} 补列 {}", table, column);
            }
        } catch (Exception e) {
            log.warn("补列失败 {}:{} - {}", table, column, e.getMessage());
        }
    }

    private void ensureRowChainColumn() {
        try {
            Integer hasCol = metaJdbc.queryForObject(
                    "SELECT COUNT(*) FROM information_schema.COLUMNS "
                            + "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'cdc_st_task' AND COLUMN_NAME = 'row_chain'",
                    Integer.class);
            if (hasCol == null || hasCol == 0) {
                metaJdbc.execute("ALTER TABLE cdc_st_task ADD COLUMN row_chain JSON NULL "
                        + "COMMENT '行级转换链(仅 filter/filter-kind)'");
                log.info("cdc_st_task 补列 row_chain");
            }
        } catch (Exception e) {
            log.warn("补 row_chain 列失败: {}", e.getMessage());
        }
    }
}
