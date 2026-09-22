-- 增量消费通道 v2 · 元数据 DDL(2026-09-15)
-- 落库位置:平台元数据库(本机 dcp-mysql / 库 dcp)
-- 与《详细设计-v2》§5 数据模型对齐;唯一键用于**数据库层防双消费**。

-- 1) 流登记(新表感知的产物):逻辑表名 → 物理 topic + 分区数
CREATE TABLE IF NOT EXISTS chg_stream (
  stream        VARCHAR(64)  NOT NULL COMMENT '逻辑表名,如 rt_sync_c',
  topic         VARCHAR(255) NOT NULL COMMENT '物理 topic,如 dcp.cdc.target_db.rt_sync_c',
  partitions    INT          NOT NULL DEFAULT 1,
  earliest      BIGINT       NOT NULL DEFAULT 0,
  latest        BIGINT       NOT NULL DEFAULT 0,
  registered_at DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at    DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (stream),
  UNIQUE KEY uk_topic (topic)
) COMMENT 'v2 流登记表(可消费的流)';

-- 2) 稀疏锚点(topic, partition, epoch, offset) → commitTs
CREATE TABLE IF NOT EXISTS chg_anchor (
  topic     VARCHAR(255) NOT NULL,
  partition_ INT        NOT NULL,
  epoch     INT        NOT NULL,
  offset_   BIGINT     NOT NULL,
  commit_ts BIGINT     NOT NULL,
  wall_time BIGINT     NOT NULL,
  PRIMARY KEY (topic, partition_, epoch, offset_),
  KEY idx_tso (topic, partition_, epoch, commit_ts)   -- ★ 支撑"二分找 ≤ TSO 的最大锚点"
) COMMENT 'v2 稀疏锚点索引(加速器,非真相源,可重建)';

-- 3) 分区水位(≤ 该 TSO 的事件已全部到齐)
CREATE TABLE IF NOT EXISTS chg_watermark_latest (
  topic        VARCHAR(255) NOT NULL,
  partition_   INT          NOT NULL,
  epoch        INT          NOT NULL,
  watermark_ts BIGINT       NOT NULL,
  offset_      BIGINT       NOT NULL,
  updated_at   DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (topic, partition_, epoch)
) COMMENT 'v2 分区水位最新值';

-- 4) 消费逻辑注册(发布流程的产物)
-- 2026-09-22 生产化扩展四列:status(启停/废弃)、published_by(发布人)、jar_size(字节)、preflight(试加载预检结果)
CREATE TABLE IF NOT EXISTS consumer_logic (
  logic_id    VARCHAR(64)  NOT NULL,
  version     VARCHAR(32)  NOT NULL,
  description VARCHAR(255) NULL,
  artifact_uri VARCHAR(512) NULL COMMENT 'jar 位置(方式③);classpath/SPI 方式可空',
  artifact_hash VARCHAR(128) NULL,
  registered_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  status      VARCHAR(16)  NOT NULL DEFAULT 'active' COMMENT 'active | disabled | archived',
  published_by VARCHAR(64)  NULL COMMENT '发布人',
  jar_size    BIGINT       NULL COMMENT 'jar 大小(字节)',
  preflight   VARCHAR(1024) NULL COMMENT '试加载预检结果(通过/失败原因)',
  PRIMARY KEY (logic_id, version)
) COMMENT 'v2 消费逻辑注册表';

-- 4-b) 逻辑发布审计(谁、何时、发布/启停/删除/下载了什么版本)
CREATE TABLE IF NOT EXISTS logic_publish_audit (
  id         BIGINT        NOT NULL AUTO_INCREMENT,
  logic_id   VARCHAR(64)   NOT NULL,
  version    VARCHAR(32)   NOT NULL,
  action     VARCHAR(32)   NOT NULL COMMENT 'publish | disable | enable | archive | delete | download',
  operator   VARCHAR(64)   NULL,
  detail     VARCHAR(1024) NULL,
  created_at DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  KEY idx_logic (logic_id, version)
) COMMENT 'v2 逻辑发布审计表';

-- 存量库迁移(新列非空默认已兜底;重复执行需手工判断,MySQL 8 无 ADD COLUMN IF NOT EXISTS):
-- ALTER TABLE consumer_logic
--   ADD COLUMN status VARCHAR(16) NOT NULL DEFAULT 'active' AFTER registered_at,
--   ADD COLUMN published_by VARCHAR(64) NULL AFTER status,
--   ADD COLUMN jar_size BIGINT NULL AFTER published_by,
--   ADD COLUMN preflight VARCHAR(1024) NULL AFTER jar_size;

-- 5) 消费任务
CREATE TABLE IF NOT EXISTS consumer_task (
  task_id        VARCHAR(64)  NOT NULL,
  name           VARCHAR(128) NULL,
  consumer_type  VARCHAR(16)  NOT NULL DEFAULT 'custom',
  owner          VARCHAR(64)  NULL,
  streams        JSON         NOT NULL COMMENT '逻辑表名数组',
  logic_id       VARCHAR(64)  NOT NULL,
  logic_version  VARCHAR(32)  NOT NULL,
  instances      INT          NOT NULL,
  contract_ref   VARCHAR(64)  NOT NULL,
  start_policy   VARCHAR(16)  NOT NULL DEFAULT 'snapshot',
  desired_state  VARCHAR(16)  NOT NULL DEFAULT 'running',
  incremental_engine VARCHAR(16) NOT NULL DEFAULT 'unit' COMMENT 'unit(v2 消费单元) | external(Flink 等)',
  consumer_group VARCHAR(128) NOT NULL,
  epoch          INT          NOT NULL DEFAULT 1,
  start_tso      BIGINT       NULL COMMENT '批阶段产出 T0',
  params         JSON         NULL COMMENT '业务参数(内核只透传)',
  status         VARCHAR(24)  NOT NULL DEFAULT 'CREATED',
  create_time    DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
  update_time    DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (task_id),
  UNIQUE KEY uk_group (consumer_group)          -- 一任务一消费组
) COMMENT 'v2 消费任务';

-- 6) 消费单元(调度原子)
CREATE TABLE IF NOT EXISTS consumer_unit (
  unit_id       VARCHAR(128) NOT NULL,
  task_id       VARCHAR(64)  NOT NULL,
  topic         VARCHAR(255) NOT NULL,
  stream        VARCHAR(64)  NOT NULL,
  epoch         INT          NOT NULL,
  state         VARCHAR(24)  NOT NULL DEFAULT 'PENDING',
  partitions    VARCHAR(255) NOT NULL COMMENT '分区组,如 0 或 0,2',
  start_offsets JSON         NULL,
  lease_expire_at DATETIME   NULL,
  node          VARCHAR(64)  NULL,
  last_offset   BIGINT       NULL,
  last_tso      BIGINT       NULL,
  create_time   DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
  update_time   DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (unit_id),
  KEY idx_task (task_id, epoch)
) COMMENT 'v2 消费单元';

-- 7) 单元↔分区 明细表 —— **数据库层防双消费**
CREATE TABLE IF NOT EXISTS consumer_unit_partition (
  unit_id   VARCHAR(128) NOT NULL,
  task_id   VARCHAR(64)  NOT NULL,
  topic     VARCHAR(255) NOT NULL,
  partition_ INT         NOT NULL,
  epoch     INT          NOT NULL,
  create_time DATETIME   NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (topic, partition_, epoch),        -- ★ 同分区同代只能被一个单元认领
  KEY idx_unit (unit_id)
) COMMENT 'v2 单元分区归属(同分区同代唯一 → 防双消费)';

-- 8) 死信(重试耗尽后落这里;绝不静默丢弃)
CREATE TABLE IF NOT EXISTS consumer_dlq (
  dlq_id     BIGINT AUTO_INCREMENT PRIMARY KEY,
  task_id    VARCHAR(64)  NOT NULL,
  unit_id    VARCHAR(128) NOT NULL,
  topic      VARCHAR(255) NOT NULL,
  partition_ INT          NOT NULL,
  offset_    BIGINT       NOT NULL,
  tso        BIGINT       NULL,
  stream     VARCHAR(64)  NULL,
  op         VARCHAR(16)  NULL,
  error_msg  VARCHAR(1024) NULL,
  retries    INT          NOT NULL DEFAULT 0,
  created_at DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
  replayed_at DATETIME    NULL,
  KEY idx_task (task_id, created_at)
) COMMENT 'v2 死信(可查/可重放)';
