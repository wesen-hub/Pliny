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
-- 一任务一 job(2026-10-12 拍板): 每任务独立 checkpoint/独立 binlog 流/独立 worker/独立 server-id;
-- 同一逻辑表可注册多张源表 → 多表合并字段级收敛
CREATE TABLE IF NOT EXISTS cdc_st_task (
  task_id             BIGINT AUTO_INCREMENT PRIMARY KEY COMMENT '采集任务 id(每任务一个 SeaTunnel job)',
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
  snapshot_start_position VARCHAR(255) NULL COMMENT '快照起点位点(方案 A: 首次 start 读源库当前位点持久化, 重放复用保证 S 恒定)',
  worker_id           VARCHAR(64) NULL COMMENT '绑定的 SeaTunnel worker(cdc_worker.worker_id, NULL=默认入口)',
  server_id_range     VARCHAR(64) NULL COMMENT '本任务独占 server-id 区间(server_id_pool 领取, 迁移不漂移)',
  running             TINYINT NOT NULL DEFAULT 0 COMMENT '1=运行中',
  update_time         DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  sync_task_id        BIGINT NOT NULL DEFAULT 0 COMMENT '所属 UI 任务(sync_task.id; 0=逻辑表直建)',
  UNIQUE KEY uk_task_src (sync_task_id, logical_table, source_db, source_table)
) ENGINE=InnoDB COMMENT='SeaTunnel CDC→Kafka 采集任务(一任务一 job; 同(逻辑,源)可多任务实例, 各实例独立 seq/写者列)';

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

-- 字段流注册(源列 → 逻辑表列): 同一源列只能有一条采集流(跨任务唯一, 防重复采集同一列)
-- 约束口径: 表级可多流(多源表→同一逻辑表), 字段级只能一条流(源表.列 → 逻辑表.列 全局唯一);
-- 说明: 同一目标列可以有多个来源(不同源表的同名/异名列), 但一旦两个来源存在相同业务键的行,
--       按“后写覆盖先写”语义收敛(零仲裁) —— 业务键重叠不做静态校验(需看具体行值, 无法在创建期判定)
CREATE TABLE IF NOT EXISTS sync_field_flow (
  id            BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  logical_table VARCHAR(128) NOT NULL COMMENT '目标逻辑表',
  target_col    VARCHAR(128) NOT NULL COMMENT '目标列(模型字段)',
  datasource_id BIGINT       NOT NULL COMMENT '源数据源',
  source_db     VARCHAR(64)  NOT NULL DEFAULT '' COMMENT '源库',
  source_table  VARCHAR(64)  NOT NULL COMMENT '源表',
  source_col    VARCHAR(128) NOT NULL COMMENT '源列',
  sync_task_id  BIGINT UNSIGNED NOT NULL COMMENT '归属同步任务(sync_task.id)',
  update_time   DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uk_flow (datasource_id, source_db, source_table, source_col),
  KEY idx_task (sync_task_id),
  KEY idx_logical (logical_table)
) ENGINE=InnoDB COMMENT='字段流注册: 同一源列只能有一条采集流(跨任务唯一)';

-- ============================================================
-- 采集任务分派(一任务一 job; 独立 worker + 独立 server-id + 判死迁移)
-- ============================================================

-- SeaTunnel worker 实例登记(独立实例定向提交; 运维初始化, 无记录时回退默认单 URL)
CREATE TABLE IF NOT EXISTS cdc_worker (
  id          BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  worker_id   VARCHAR(64) NOT NULL UNIQUE COMMENT 'worker 标识(如 worker-1)',
  rest_url    VARCHAR(255) NOT NULL COMMENT 'REST 提交入口(http://host:8081)',
  enabled     TINYINT NOT NULL DEFAULT 1 COMMENT '1=可用, 0=下线排空',
  update_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB COMMENT='SeaTunnel worker 实例登记(独立实例定向提交)';

-- server-id 区间池(全局唯一分配: 任务注册时领取固定宽 5, 终身跟随任务, 池只增不减)
CREATE TABLE IF NOT EXISTS server_id_pool (
  id           BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  start_id     BIGINT NOT NULL COMMENT '区间起点(含)',
  end_id       BIGINT NOT NULL COMMENT '区间终点(含)',
  owner_task_id BIGINT UNSIGNED NULL COMMENT '归属采集任务(cdc_st_task.task_id), NULL=空闲',
  update_time  DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  KEY idx_owner (owner_task_id)
) ENGINE=InnoDB COMMENT='server-id 区间池(全局唯一分配, 任务终身跟随)';

-- 字段级回补任务(后加列存量: 主键区间分片 + 游标断点续跑 + 回补小号 seq)
CREATE TABLE IF NOT EXISTS sync_backfill_run (
  id            BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  logical_table VARCHAR(128) NOT NULL COMMENT '逻辑表',
  sync_task_id  BIGINT UNSIGNED NOT NULL COMMENT '归属页面任务(sync_task.id)',
  task_id       BIGINT NOT NULL COMMENT '归属采集任务实例(cdc_st_task.task_id, 写者列来源)',
  datasource_id BIGINT NOT NULL COMMENT '源数据源',
  source_db     VARCHAR(64) NOT NULL COMMENT '源库',
  source_table  VARCHAR(64) NOT NULL COMMENT '源表',
  source_col    VARCHAR(128) NOT NULL COMMENT '回补源列',
  target_col    VARCHAR(128) NOT NULL COMMENT '目标列(modelField)',
  cursor_json   TEXT NULL COMMENT '主键游标(JSON: 最后一行主键值, 断点续跑)',
  status        VARCHAR(16) NOT NULL DEFAULT 'PENDING' COMMENT 'PENDING|BACKFILLING|DONE|FAILED',
  total_rows    BIGINT NULL COMMENT '预估总行数(探测值, 参考)',
  processed_rows BIGINT NOT NULL DEFAULT 0 COMMENT '已处理行数',
  error_msg     VARCHAR(1024) NULL,
  update_time   DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  KEY idx_status (status),
  KEY idx_logical (logical_table)
) ENGINE=InnoDB COMMENT='字段级回补任务(后加列存量, 游标断点续跑)';

-- 同步链路告警表(2026-10-04 新方案: 减列/主键修改等异常告警, 人工确认闭环)
CREATE TABLE IF NOT EXISTS sync_alert (
  id            BIGINT AUTO_INCREMENT PRIMARY KEY,
  logical_table VARCHAR(128) NOT NULL COMMENT '逻辑表',
  task_id       BIGINT NULL COMMENT '采集任务 id(cdc_st_task.task_id, 可空)',
  alert_type    VARCHAR(32)  NOT NULL COMMENT '告警类型: missing_column=映射引用源列缺失, primary_key_change=主键值修改, ddl_change=源表 TRUNCATE/DROP/ALTER(N6, DBA 登记)',
  detail        VARCHAR(1024) NULL COMMENT '详情: 缺失列名与映射 / beforeKey→businessKey',
  status        VARCHAR(16)  NOT NULL DEFAULT 'OPEN' COMMENT 'OPEN=未处理, CLOSED=已处理',
  created_at    DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at    DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  KEY idx_lt (logical_table),
  KEY idx_status (status)
) ENGINE=InnoDB COMMENT='同步链路告警(缺列/主键修改, 人工确认闭环)';

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

-- ============ 权限体系(2026-09-08): 系统权限(功能 RBAC) + 数据权限 ============
CREATE TABLE IF NOT EXISTS sys_permission (
  id          BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  code        VARCHAR(96) NOT NULL UNIQUE COMMENT '权限码 模块:资源:动作',
  name        VARCHAR(64)  NOT NULL,
  module      VARCHAR(32)  NOT NULL COMMENT '平台运维|资产|打标V2|标签消费|系统',
  type        VARCHAR(8)   NOT NULL DEFAULT 'action' COMMENT 'menu(菜单页)|action(操作)',
  hot         TINYINT      NOT NULL DEFAULT 0 COMMENT '高危独立点(发布/启停/删除/导出/管理)',
  create_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB COMMENT='权限点表';

CREATE TABLE IF NOT EXISTS sys_role_permission (
  role_id   BIGINT UNSIGNED NOT NULL,
  perm_code VARCHAR(96) NOT NULL,
  PRIMARY KEY (role_id, perm_code)
) ENGINE=InnoDB COMMENT='角色-权限点关联';

-- 数据权限(L0-L3 × scope/dataset/column/tag + 行规则; 设计文档 2026-09-08)
CREATE TABLE IF NOT EXISTS data_policy (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  res_type VARCHAR(16) NOT NULL COMMENT 'scope|dataset|column|tag',
  dim VARCHAR(8) NULL COMMENT 'scope: biz|sec|src',
  res_path VARCHAR(512) NOT NULL COMMENT '分类路径/表名/列名/标签全路径',
  subject_type VARCHAR(8) NOT NULL COMMENT 'role|user',
  subject_id VARCHAR(64) NOT NULL,
  level TINYINT NOT NULL COMMENT '0-3',
  eff VARCHAR(8) NOT NULL DEFAULT 'grant' COMMENT 'grant|deny',
  enabled TINYINT NOT NULL DEFAULT 1,
  update_by VARCHAR(64), update_time DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uk_policy (res_type, res_path, subject_type, subject_id)
) ENGINE=InnoDB COMMENT='数据权限策略';

CREATE TABLE IF NOT EXISTS row_rule (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  name VARCHAR(128) NOT NULL,
  res_type VARCHAR(16) NOT NULL COMMENT 'dataset|tag',
  res_path VARCHAR(512) NOT NULL,
  kind VARCHAR(8) NOT NULL COMMENT 'cond|list|tag_ref',
  cond_json TEXT NULL COMMENT '{field,op,val}',
  list_text MEDIUMTEXT NULL,
  tag_ref_path VARCHAR(512) NULL,
  note VARCHAR(512), enabled TINYINT NOT NULL DEFAULT 1,
  update_time DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB COMMENT='行规则(手工配置)';

-- ============ 数据订阅(2026-09-09): 对外数据契约 ============
CREATE TABLE IF NOT EXISTS data_contract (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  name VARCHAR(128) NOT NULL,
  subscriber VARCHAR(64) NOT NULL COMMENT '订阅方(接入应用名)',
  view_type VARCHAR(16) NOT NULL COMMENT 'logical|tag',
  view_json MEDIUMTEXT NOT NULL COMMENT '视图定义(逻辑表: tables[]+cols[]+conds[]+merge; 标签: tags[]+op+exclude+shape+joinTable/cols)',
  push VARCHAR(8) NOT NULL DEFAULT 'pull' COMMENT 'pull|push',
  freq VARCHAR(24) NOT NULL DEFAULT 'manual' COMMENT 'manual|实时|定时|批量|流式(语义标注)',
  target_type VARCHAR(16) NULL COMMENT 'pull: api; push: webhook|kafka',
  target_url VARCHAR(512) NULL,
  topic VARCHAR(128) NULL COMMENT 'push kafka topic',
  max_secret TINYINT NOT NULL DEFAULT 2,
  ttl DATE NULL,
  status VARCHAR(16) NOT NULL DEFAULT 'pending' COMMENT 'pending|active|stopped|expired|rejected',
  token VARCHAR(64) NULL COMMENT '生效签发: pull 拉取鉴权(X-Dcp-Token)',
  webhook_secret VARCHAR(64) NULL COMMENT '生效签发: push webhook 验签密钥',
  approved_by VARCHAR(64), approved_at DATETIME, reject_reason VARCHAR(512),
  push_interval_min INT NULL COMMENT 'push 定时档: 自动投递间隔分钟',
  last_deliver_at DATETIME NULL COMMENT '最近一次投递时间(调度节拍)',
  last_cursor VARCHAR(128) NULL COMMENT 'push/交付推进游标',
  create_by VARCHAR(64), create_time DATETIME DEFAULT CURRENT_TIMESTAMP, update_time DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  KEY idx_status (status)
) ENGINE=InnoDB COMMENT='数据订阅契约';

CREATE TABLE IF NOT EXISTS data_sub_delivery (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  contract_id BIGINT NOT NULL,
  mode VARCHAR(8) NOT NULL COMMENT 'pull|push',
  channel VARCHAR(16) NOT NULL COMMENT 'api|webhook|kafka|export',
  range_text VARCHAR(255) NULL COMMENT '数据范围/游标',
  rows_count INT NULL,
  status VARCHAR(16) NOT NULL COMMENT 'ok|fail|retrying',
  retries INT NOT NULL DEFAULT 0,
  error_msg VARCHAR(1024), ts DATETIME DEFAULT CURRENT_TIMESTAMP,
  KEY idx_contract (contract_id, id)
) ENGINE=InnoDB COMMENT='数据订阅交付记录';

-- ============ 数据订阅(2026-09-09): 异步导出任务 ============
CREATE TABLE IF NOT EXISTS data_sub_export (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  contract_id BIGINT NOT NULL,
  format VARCHAR(8) NOT NULL COMMENT 'csv|json',
  status VARCHAR(12) NOT NULL DEFAULT 'pending' COMMENT 'pending|done|fail',
  rows_count INT NULL,
  file_name VARCHAR(128) NULL,
  error_msg VARCHAR(512),
  expire_at DATETIME NULL,
  create_time DATETIME DEFAULT CURRENT_TIMESTAMP,
  KEY idx_contract (contract_id, id)
) ENGINE=InnoDB COMMENT='数据订阅异步导出任务';

-- ============ 数据资产目录(2026-09-09): 资产 × 业务标签绑定 ============
CREATE TABLE IF NOT EXISTS asset_tag_binding (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  asset_table VARCHAR(128) NOT NULL COMMENT '资产(逻辑表名)',
  tag_path VARCHAR(255) NOT NULL COMMENT '业务标签路径(如 业务域/电商)',
  create_by VARCHAR(64), create_time DATETIME DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY uk_asset_tag (asset_table, tag_path),
  KEY idx_tag (tag_path)
) ENGINE=InnoDB COMMENT='资产目录人工打标绑定';

-- ============ ShedLock 分布式锁表(2026-10-01): 定时任务多实例互斥 ============
CREATE TABLE IF NOT EXISTS shedlock (
  name VARCHAR(64) NOT NULL,
  lock_until TIMESTAMP(3) NOT NULL,
  locked_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  locked_by VARCHAR(255) NOT NULL,
  PRIMARY KEY (name)
) ENGINE=InnoDB COMMENT='ShedLock 分布式锁(定时任务多实例互斥)';

-- ============ SeaTunnel worker 登记种子(2026-10-07): 独立实例定向提交 ============
-- 单机演示环境登记 worker-1(容器内 REST 入口); 多 worker 由运维在页面/元库追加登记,
-- 平台按最闲分派采集任务, 判死后自动迁移。rest_url 必须是 backend 可达地址。
INSERT IGNORE INTO cdc_worker (worker_id, rest_url, enabled) VALUES ('worker-1', 'http://seatunnel:8081', 1);

-- ============ 逻辑表配置变更审计(2026-10-09): 审计 + 热更新通知载体 ============
CREATE TABLE IF NOT EXISTS logical_table_audit (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  table_name VARCHAR(128) NOT NULL COMMENT '逻辑表名',
  action VARCHAR(32) NOT NULL COMMENT 'CREATE/UPDATE/TOPIC_CHANGED/PARTITIONS',
  operator VARCHAR(64) NULL COMMENT '操作者(接口层暂不采集身份则置 NULL)',
  changed_fields JSON NULL COMMENT '变更字段: {字段: {from, to}}',
  created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  KEY idx_lt_audit (table_name, created_at)
) ENGINE=InnoDB COMMENT='逻辑表配置变更审计';
