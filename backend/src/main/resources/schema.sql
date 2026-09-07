-- ============================================================
-- 数据采集平台 元数据库 DDL（MySQL 8.0, utf8mb4）
-- 幂等脚本: 启动时由 spring.sql.init 自动执行（IF NOT EXISTS）
-- 设计依据: 《数据采集平台设计方案 V2.0》§7.2
-- ============================================================

-- 数据源配置
CREATE TABLE IF NOT EXISTS datasource_config (
  id                     BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  name                   VARCHAR(128) NOT NULL COMMENT '业务名称',
  type                   VARCHAR(32)  NOT NULL COMMENT 'mysql|postgresql|oracle|sqlserver|mongodb',
  host                   VARCHAR(255) NOT NULL,
  port                   INT          NOT NULL,
  database_name          VARCHAR(128) NOT NULL,
  username               VARCHAR(128) NOT NULL,
  password_ciphertext    VARCHAR(512) NOT NULL COMMENT 'AES-256-GCM: base64(IV||CT||TAG)',
  key_version            INT          NOT NULL DEFAULT 1,
  extra_params           JSON         NULL COMMENT '字符集/时区/超时等扩展参数',
  group_name             VARCHAR(64)  NULL,
  env                    VARCHAR(16)  NULL COMMENT 'dev|test|prod',
  enabled                TINYINT(1)   NOT NULL DEFAULT 1,
  last_health_check_time DATETIME     NULL,
  last_health_check_result VARCHAR(16) NULL,
  create_by              VARCHAR(64)  NULL,
  update_by              VARCHAR(64)  NULL,
  create_time            DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
  update_time            DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  deleted                TINYINT(1)   NOT NULL DEFAULT 0,
  KEY idx_type (type),
  KEY idx_group (group_name),
  KEY idx_env (env)
) ENGINE=InnoDB COMMENT='数据源配置表';

-- 任务定义
CREATE TABLE IF NOT EXISTS job_definition (
  id                    BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  job_name              VARCHAR(128) NOT NULL,
  source_datasource_id  BIGINT UNSIGNED NOT NULL,
  target_datasource_id  BIGINT UNSIGNED NOT NULL,
  target_table_name     VARCHAR(128) NOT NULL,
  sync_mode             VARCHAR(16)  NOT NULL COMMENT 'full|incremental|cdc',
  incremental_field     VARCHAR(128) NULL COMMENT '增量字段(仅incremental)',
  cron_expr             VARCHAR(64)  NULL,
  misfire_policy        VARCHAR(16)  NOT NULL DEFAULT 'DO_NOTHING',
  enabled               TINYINT(1)   NOT NULL DEFAULT 1,
  status                VARCHAR(16)  NOT NULL DEFAULT 'READY' COMMENT 'READY|RUNNING|SUCCESS|FAILED|STOPPED',
  timeout_minutes       INT          NOT NULL DEFAULT 120,
  parallelism           INT          NOT NULL DEFAULT 2,
  retry_times           INT          NOT NULL DEFAULT 3,
  retry_interval_seconds INT         NOT NULL DEFAULT 60,
  alarm_enabled         TINYINT(1)   NOT NULL DEFAULT 1,
  description           VARCHAR(512) NULL,
  last_execution_time   DATETIME     NULL,
  last_execution_status VARCHAR(16)  NULL,
  version               INT          NOT NULL DEFAULT 0 COMMENT '乐观锁/运行锁',
  create_by             VARCHAR(64)  NULL,
  update_by             VARCHAR(64)  NULL,
  create_time           DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
  update_time           DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  deleted               TINYINT(1)   NOT NULL DEFAULT 0,
  KEY idx_source (source_datasource_id),
  KEY idx_target (target_datasource_id),
  KEY idx_status (status)
) ENGINE=InnoDB COMMENT='任务定义表';

-- 抽取配置（与任务 1:1）
CREATE TABLE IF NOT EXISTS extract_config (
  id           BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  job_id       BIGINT UNSIGNED NOT NULL UNIQUE,
  source_type  VARCHAR(16) NOT NULL COMMENT 'table|sql',
  content      TEXT        NOT NULL COMMENT '表名 或 完整SQL',
  preview_limit INT        NOT NULL DEFAULT 100,
  create_time  DATETIME    NOT NULL DEFAULT CURRENT_TIMESTAMP,
  update_time  DATETIME    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB COMMENT='抽取配置表';

-- 字段映射（核心）
CREATE TABLE IF NOT EXISTS field_mapping (
  id             BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  job_id         BIGINT UNSIGNED NOT NULL,
  source_field   VARCHAR(128) NOT NULL,
  target_field   VARCHAR(128) NOT NULL,
  transform_type VARCHAR(16)  NOT NULL DEFAULT 'direct' COMMENT 'direct|dictionary|expression',
  transform_rule TEXT         NULL COMMENT '字典名 或 Aviator表达式',
  sort_no        INT          NOT NULL DEFAULT 0,
  create_time    DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
  update_time    DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uk_job_target (job_id, target_field),
  KEY idx_job (job_id)
) ENGINE=InnoDB COMMENT='字段映射表';

-- 字典
CREATE TABLE IF NOT EXISTS dict_config (
  id          BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  dict_name   VARCHAR(128) NOT NULL UNIQUE,
  description VARCHAR(255) NULL,
  create_by   VARCHAR(64)  NULL,
  update_by   VARCHAR(64)  NULL,
  create_time DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
  update_time DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  deleted     TINYINT(1)   NOT NULL DEFAULT 0
) ENGINE=InnoDB COMMENT='字典定义表';

CREATE TABLE IF NOT EXISTS dict_entry (
  id           BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  dict_id      BIGINT UNSIGNED NOT NULL,
  source_value VARCHAR(255) NOT NULL,
  target_value VARCHAR(255) NOT NULL,
  sort_no      INT NOT NULL DEFAULT 0,
  create_time  DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  update_time  DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uk_dict_source (dict_id, source_value),
  KEY idx_dict (dict_id)
) ENGINE=InnoDB COMMENT='字典条目表';

-- 增量位点
CREATE TABLE IF NOT EXISTS sync_state (
  job_id          BIGINT UNSIGNED PRIMARY KEY,
  last_sync_value VARCHAR(255) NOT NULL COMMENT '上次位点(字符串化)',
  value_type      VARCHAR(16)  NOT NULL DEFAULT 'LONG' COMMENT 'LONG|DATETIME|STRING',
  update_time     DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB COMMENT='增量同步位点表';

-- 执行历史
CREATE TABLE IF NOT EXISTS job_execution_history (
  id                BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  job_id            BIGINT UNSIGNED NOT NULL,
  trigger_type      VARCHAR(16)  NOT NULL COMMENT 'MANUAL|CRON',
  sea_tunnel_job_id VARCHAR(64)  NULL COMMENT '引擎jobId(轻量引擎为执行ID)',
  status            VARCHAR(16)  NOT NULL COMMENT 'RUNNING|SUCCESS|FAILED|STOPPED|TIMEOUT',
  start_time        DATETIME     NOT NULL,
  end_time          DATETIME     NULL,
  duration_ms       BIGINT       NULL,
  source_count      BIGINT       NULL,
  target_count      BIGINT       NULL,
  error_message     TEXT         NULL,
  retry_count       INT          NOT NULL DEFAULT 0,
  create_time       DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
  KEY idx_job_time (job_id, start_time),
  KEY idx_status (status),
  KEY idx_start (start_time)
) ENGINE=InnoDB COMMENT='任务执行历史表';

-- 结构化执行日志（30天滚动清理）
CREATE TABLE IF NOT EXISTS job_execution_log (
  id           BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  execution_id BIGINT UNSIGNED NOT NULL,
  log_level    VARCHAR(8)  NOT NULL COMMENT 'INFO|WARN|ERROR',
  log_content  TEXT        NOT NULL,
  log_time     DATETIME    NOT NULL,
  KEY idx_execution (execution_id, log_time)
) ENGINE=InnoDB COMMENT='执行日志表';

-- 告警配置
CREATE TABLE IF NOT EXISTS alarm_config (
  id            BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  job_id        BIGINT UNSIGNED NOT NULL,
  alarm_type    VARCHAR(16)  NOT NULL COMMENT 'EMAIL|DINGTALK|WECHAT',
  receiver      VARCHAR(512) NOT NULL COMMENT '邮箱列表 或 webhook URL',
  trigger_event VARCHAR(16)  NOT NULL COMMENT 'FAILED|TIMEOUT|SUCCESS',
  enabled       TINYINT(1)   NOT NULL DEFAULT 1,
  create_time   DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
  update_time   DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  KEY idx_job (job_id)
) ENGINE=InnoDB COMMENT='告警配置表';

-- 权限与审计
CREATE TABLE IF NOT EXISTS sys_user (
  id            BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  username      VARCHAR(64) NOT NULL UNIQUE,
  password_hash VARCHAR(255) NOT NULL COMMENT 'BCrypt',
  nickname      VARCHAR(64) NULL,
  email         VARCHAR(128) NULL,
  phone         VARCHAR(32) NULL,
  status        TINYINT(1)  NOT NULL DEFAULT 1,
  create_time   DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  update_time   DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB COMMENT='用户表';

CREATE TABLE IF NOT EXISTS sys_role (
  id          BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  role_code   VARCHAR(64) NOT NULL UNIQUE COMMENT 'ADMIN|OPERATOR|VIEWER|AUDITOR',
  role_name   VARCHAR(64) NULL,
  description VARCHAR(255) NULL,
  create_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  update_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB COMMENT='角色表';

CREATE TABLE IF NOT EXISTS sys_user_role (
  id      BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  user_id BIGINT UNSIGNED NOT NULL,
  role_id BIGINT UNSIGNED NOT NULL,
  UNIQUE KEY uk_user_role (user_id, role_id)
) ENGINE=InnoDB COMMENT='用户角色关联表';

CREATE TABLE IF NOT EXISTS sys_operation_log (
  id          BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  user_id     BIGINT UNSIGNED NULL,
  username    VARCHAR(64) NULL,
  module      VARCHAR(32) NOT NULL COMMENT 'datasource|job|dict|system...',
  operation   VARCHAR(32) NOT NULL COMMENT 'CREATE|UPDATE|DELETE|ENABLE|DISABLE|RUN|STOP|LOGIN',
  target_type VARCHAR(32) NULL,
  target_id   BIGINT UNSIGNED NULL,
  detail      JSON NULL COMMENT '变更前后diff',
  ip          VARCHAR(64) NULL,
  create_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  KEY idx_module_time (module, create_time),
  KEY idx_user (user_id)
) ENGINE=InnoDB COMMENT='操作审计表';

-- ============================================================
-- V3 同步链路(P0):全局 seq / 逻辑表注册(桶映射) / 水位
-- ============================================================

-- 全局 seq 计数器(每逻辑表一行;区间预分配,行锁保证并发不重)
CREATE TABLE IF NOT EXISTS seq_counter (
  table_name  VARCHAR(128) PRIMARY KEY COMMENT '逻辑表名',
  cur         BIGINT NOT NULL DEFAULT 0 COMMENT '当前水位(已分配的最大号)',
  update_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB COMMENT='全局 seq 计数器(区间预分配)';

-- 逻辑表注册:topic / 桶数 / 分表规则 / 桶→分区映射(扩容时热更新)
CREATE TABLE IF NOT EXISTS logical_table (
  table_name    VARCHAR(128) PRIMARY KEY COMMENT '逻辑表名',
  topic         VARCHAR(128) NOT NULL COMMENT 'Kafka topic',
  bucket_count  INT NOT NULL DEFAULT 4096 COMMENT '固定桶数(定死不可改)',
  table_count   INT NOT NULL DEFAULT 50 COMMENT '目标物理分表数(定死不可改)',
  table_prefix  VARCHAR(64)  NOT NULL COMMENT '物理表名前缀,如 t_order',
  partition_count INT NOT NULL DEFAULT 8 COMMENT 'topic 分区数',
  bucket_map    TEXT NULL COMMENT '桶→分区映射(JSON,扩容时重算热更新;NULL=顺序映射 bucket%partition_count)',
  consumer_group VARCHAR(128) NULL COMMENT '消费组 id',
  business_key  JSON NULL COMMENT '业务主键配置: {mode: merge|scope, scopeFields:[], systemNames:[], keyFields:[]}(键构成上线后不可变)',
  key_columns   JSON NULL COMMENT '目标唯一键列(幂等收敛依据)',
  updated_at    DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB COMMENT='逻辑表注册与桶映射';

-- SeaTunnel CDC → Kafka 采集任务(架构文档采集端实现: connector-cdc-mysql + CdcSeqTransform + Kafka sink)
-- 一张源表 = 一个 SeaTunnel job; 同一逻辑表可注册多张源表 → 多表合并字段级收敛
CREATE TABLE IF NOT EXISTS cdc_st_task (
  task_id             BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '采集任务 id(每源表一个 job)',
  logical_table       VARCHAR(128) NOT NULL COMMENT '逻辑表名(多源表共享 = 多表合并)',
  datasource_id       BIGINT NOT NULL COMMENT '源数据源 id',
  source_db           VARCHAR(64) NOT NULL COMMENT '源库名',
  source_table        VARCHAR(64) NOT NULL COMMENT '源表名',
  topic               VARCHAR(128) NOT NULL COMMENT 'Kafka topic(消费端监听)',
  business_key_fields VARCHAR(255) NOT NULL COMMENT '业务主键字段(JSON array, 按声明顺序拼接)',
  key_columns         VARCHAR(255) NOT NULL COMMENT '目标唯一键列(JSON array)',
  bucket_count        INT NOT NULL DEFAULT 4096 COMMENT '固定桶数',
  field_mapping       JSON NULL COMMENT '字段映射配置(该源表 → 模型字段, 向导 Step3 产物)',
  row_chain           JSON NULL COMMENT '行级转换链(该源表: 仅 filter/filter-kind, 向导 Step4 产物)',
  sea_tunnel_job_id   VARCHAR(64) NULL COMMENT 'SeaTunnel jobId(重启同 id 恢复 checkpoint)',
  running             TINYINT NOT NULL DEFAULT 0 COMMENT '1=运行中',
  update_time         DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  sync_task_id        BIGINT NOT NULL DEFAULT 0 COMMENT '所属 UI 任务(sync_task.id; 0=逻辑表直建)',
  UNIQUE KEY uk_task_src (sync_task_id, logical_table, source_db, source_table)
) ENGINE=InnoDB COMMENT='SeaTunnel CDC→Kafka 采集任务(同(逻辑,源)可多任务实例, 各实例独立 seq/写者列)';

-- 同步任务配置(向导提交产物: 源表/模型/字段映射/行级链/业务主键)
CREATE TABLE IF NOT EXISTS sync_task (
  id             BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  name           VARCHAR(128) NOT NULL COMMENT '任务名',
  logical_table  VARCHAR(128) NOT NULL COMMENT '逻辑表(目标模型表名)',
  datasource_id  BIGINT NOT NULL COMMENT '源数据源',
  source_db      VARCHAR(64)  NULL,
  source_table   VARCHAR(64)  NULL,
  field_mapping  JSON NOT NULL COMMENT '字段映射(来源+加工链)',
  row_chain      JSON NULL COMMENT '行级转换链',
  business_key   JSON NULL COMMENT '业务主键配置',
  status         VARCHAR(16) NOT NULL DEFAULT 'DRAFT' COMMENT 'DRAFT|SUBMITTED|APPROVED|RUNNING',
  create_time    DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  update_time    DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB COMMENT='同步任务配置(向导提交)';

-- 同步死信队列(消费失败消息落库; 页面: 同步任务 → 死信监控)
CREATE TABLE IF NOT EXISTS dlt_message (
  id               BIGINT AUTO_INCREMENT PRIMARY KEY,
  logical_table    VARCHAR(128) NOT NULL COMMENT '逻辑表',
  source_table     VARCHAR(64)  NULL COMMENT '源表(可解析时)',
  topic            VARCHAR(128) NOT NULL COMMENT 'Kafka topic',
  partition_id     INT          NOT NULL COMMENT '分区',
  offset_id        BIGINT       NOT NULL COMMENT 'offset',
  business_key     VARCHAR(255) NULL COMMENT '业务主键',
  payload          TEXT         NULL COMMENT '原始消息(CdcRecord 或 envelope, 可能是非法 JSON)',
  error_msg        TEXT         NULL COMMENT '失败原因',
  fail_count       INT          NOT NULL DEFAULT 1 COMMENT '失败次数',
  status           VARCHAR(16)  NOT NULL DEFAULT 'PENDING' COMMENT 'PENDING|REPLAYED|IGNORED',
  first_fail_time  DATETIME     NOT NULL COMMENT '首次失败',
  last_fail_time   DATETIME     NOT NULL COMMENT '最近失败',
  replay_time      DATETIME     NULL COMMENT '重放时间',
  KEY idx_status (status),
  KEY idx_logical (logical_table),
  KEY idx_time (last_fail_time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='同步死信队列';

-- ============ 数据资产编目(可维护: 多层级节点 + 多维挂载 + 资产元数据) ============
-- 目录节点(多层级, 维度: biz业务/src来源/sec密级/custom自定义)
CREATE TABLE IF NOT EXISTS asset_catalog_node (
  id          BIGINT AUTO_INCREMENT PRIMARY KEY,
  dim         VARCHAR(32)  NOT NULL COMMENT '维度: biz/src/sec/custom',
  parent_id   BIGINT       NOT NULL DEFAULT 0 COMMENT '父节点, 0=根',
  name        VARCHAR(128) NOT NULL COMMENT '节点名',
  sort        INT          NOT NULL DEFAULT 0 COMMENT '排序',
  create_time DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
  KEY idx_dim_parent (dim, parent_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='资产目录节点(多层级多维度)';

-- 资产可维护元数据(逻辑表的编目扩展; table_name 关联 logical_table)
CREATE TABLE IF NOT EXISTS asset_meta (
  table_name   VARCHAR(128) NOT NULL PRIMARY KEY COMMENT '逻辑表',
  display_name VARCHAR(128) NULL COMMENT '展示名',
  description  VARCHAR(512) NULL COMMENT '描述',
  domain       VARCHAR(64)  NULL COMMENT '业务域',
  layer        VARCHAR(16)  NULL COMMENT '分层 ODS/DWD/DWS/ADS',
  secret_level TINYINT      NOT NULL DEFAULT 2 COMMENT '密级 1公开2内部3机密4绝密',
  owner        VARCHAR(64)  NULL COMMENT '负责人',
  quality      VARCHAR(8)   NULL COMMENT '质量等级',
  tags         JSON         NULL COMMENT '标签',
  update_time  DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='资产元数据(可维护)';

-- 资产 ↔ 目录节点挂载(一个资产可挂多个节点 = 多维度)
CREATE TABLE IF NOT EXISTS asset_node_binding (
  id          BIGINT       NOT NULL AUTO_INCREMENT PRIMARY KEY,
  node_id     BIGINT       NOT NULL COMMENT '目录节点',
  table_name  VARCHAR(128) NOT NULL COMMENT '逻辑表',
  create_time DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY uk_node_asset (node_id, table_name),
  KEY idx_asset (table_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='资产-目录节点挂载';

-- ============ 标签体系(多级 tag_dict; 资产打标存 asset_meta.tags = 标签 path 数组) ============
CREATE TABLE IF NOT EXISTS tag_dict (
  id          BIGINT AUTO_INCREMENT PRIMARY KEY,
  parent_id   BIGINT       NOT NULL DEFAULT 0 COMMENT '父标签, 0=根',
  name        VARCHAR(64)  NOT NULL COMMENT '标签名',
  category    VARCHAR(32)  NOT NULL DEFAULT '自定义标签' COMMENT '业务标签/系统标签/敏感标签/自定义标签',
  granularity VARCHAR(16)  NOT NULL DEFAULT '表级' COMMENT '表级/字段级/记录级',
  source      VARCHAR(64)  NULL COMMENT '来源: 模型定义/采集自动继承/定级审批/ML检测/手动',
  sort        INT          NOT NULL DEFAULT 0,
  status      VARCHAR(16)  NOT NULL DEFAULT '启用' COMMENT '启用/停用',
  create_time DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
  KEY idx_parent (parent_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='标签字典(多级)';

-- ============ 订阅中心(标签条件订阅, 命中资产 = 标签子集) ============
CREATE TABLE IF NOT EXISTS asset_subscription (
  id          BIGINT AUTO_INCREMENT PRIMARY KEY,
  name        VARCHAR(128) NOT NULL COMMENT '订阅名称',
  expr        JSON         NOT NULL COMMENT '条件: {tags:[path], op:AND/OR, exclude:[], maxSecret:n}',
  mode        VARCHAR(16)  NOT NULL DEFAULT 'pull' COMMENT 'pull/push',
  target      VARCHAR(255) NULL COMMENT '接收方描述',
  status      VARCHAR(16)  NOT NULL DEFAULT '运行中' COMMENT '运行中/暂停',
  create_by   VARCHAR(64)  NULL,
  create_time DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
  update_time DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='资产订阅(标签条件)';
