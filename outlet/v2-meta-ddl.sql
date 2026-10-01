-- 增量消费通道 v2 · 元数据 DDL(2026-09-30,彻底版)
-- 落库位置:平台元数据库(本机 dcp-mysql / 库 dcp)
-- 与《详细设计-v2》§5 数据模型对齐;调度状态/规划代/批起点/并发全部由统一调度框架承担,
-- 元库只保留业务登记;**防双消费由调度框架归属键唯一保证**(不再依赖数据库层唯一键)。

-- 1) 流登记(新表感知的产物):逻辑表名 → 物理 topic + 分区数
CREATE TABLE IF NOT EXISTS chg_stream (
  stream        VARCHAR(64)  NOT NULL COMMENT '逻辑表名,如 rt_sync_c',
  topic         VARCHAR(255) NOT NULL COMMENT '物理 topic,如 dcp.cdc.target_db.rt_sync_c',
  kafka_bootstrap VARCHAR(255) NOT NULL DEFAULT '' COMMENT '该流所在 Kafka 地址(随任务指令透传, ns 与 Kafka 解耦)',
  partitions    INT          NOT NULL DEFAULT 1,
  earliest      BIGINT       NOT NULL DEFAULT 0,
  latest        BIGINT       NOT NULL DEFAULT 0,
  registered_at DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at    DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (stream),
  UNIQUE KEY uk_topic (topic)
) COMMENT 'v2 流登记表(可消费的流)';

-- 存量库升级(底座激活衔接):加 kafka_bootstrap 列
-- ALTER TABLE chg_stream ADD COLUMN kafka_bootstrap VARCHAR(255) NOT NULL DEFAULT '' AFTER topic;

-- 2) 稀疏锚点(topic, partition, epoch, offset) → commitTs; fence=1 为栅栏点(前缀上界刷新点)
CREATE TABLE IF NOT EXISTS chg_anchor (
  topic     VARCHAR(255) NOT NULL,
  partition_ INT        NOT NULL,
  epoch     INT        NOT NULL,
  offset_   BIGINT     NOT NULL,
  commit_ts BIGINT     NOT NULL,
  wall_time BIGINT     NOT NULL,
  fence     TINYINT    NOT NULL DEFAULT 0 COMMENT '1=栅栏点(commitTs ≥ 分区历史最大, 起点查询只信栅栏)',
  PRIMARY KEY (topic, partition_, epoch, offset_),
  KEY idx_tso (topic, partition_, epoch, commit_ts),   -- 排障观测(全量锚点)
  KEY idx_fence_tso (topic, partition_, epoch, fence, commit_ts)  -- ★ 栅栏查询:二分找 ≤ TSO 的最大栅栏
) COMMENT 'v2 稀疏锚点索引(加速器,非真相源,可重建)';

-- 存量库升级(栅栏方案 §3.4):加 fence 列 + 栅栏查询索引
-- ALTER TABLE chg_anchor ADD COLUMN fence TINYINT NOT NULL DEFAULT 0;
-- ALTER TABLE chg_anchor ADD KEY idx_fence_tso (topic, partition_, epoch, fence, commit_ts);

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

-- 5) 消费任务(纯业务登记;调度状态由框架 spec/status 键承载)
CREATE TABLE IF NOT EXISTS consumer_task (
  task_id        VARCHAR(64)  NOT NULL,
  name           VARCHAR(128) NULL,
  consumer_type  VARCHAR(16)  NOT NULL DEFAULT 'custom',
  namespace      VARCHAR(32)  NOT NULL DEFAULT 'outlet' COMMENT '调度框架 ns(消费底座多 ns 拆分: tag/lake/sub/ingest; 与 Kafka 解耦)',
  owner          VARCHAR(64)  NULL,
  streams        JSON         NOT NULL COMMENT '逻辑表名数组',
  logic_id       VARCHAR(64)  NOT NULL,
  logic_version  VARCHAR(32)  NOT NULL,
  contract_ref   VARCHAR(64)  NOT NULL,
  start_policy   VARCHAR(16)  NOT NULL DEFAULT 'snapshot',
  desired_state  VARCHAR(16)  NOT NULL DEFAULT 'running',
  incremental_engine VARCHAR(16) NOT NULL DEFAULT 'unit' COMMENT 'unit(框架接管) | external(Flink 等)',
  consumer_group VARCHAR(128) NOT NULL,
  params         JSON         NULL COMMENT '业务参数(内核只透传)',
  create_time    DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
  update_time    DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (task_id),
  UNIQUE KEY uk_group (consumer_group)          -- 一任务一消费组
) COMMENT 'v2 消费任务(纯业务登记)';

-- 存量库升级(底座多 ns 拆分):加 namespace 列
-- ALTER TABLE consumer_task ADD COLUMN namespace VARCHAR(32) NOT NULL DEFAULT 'outlet' AFTER consumer_type;

-- (已删除) consumer_unit / consumer_unit_partition:
--   调度原子从“单元(分区子集)”降维为“分区”——一个消费任务 = 框架一个 TaskSpec(partitions=真实分区数),
--   防双消费由框架归属键唯一(单键单 owner)+ 判死双确认 + 保守交接统一保证,数据库层闸退位。

-- 8) 死信(重试耗尽后落这里;绝不静默丢弃)
--     unit_id 列保留但语义已变:填 taskId(彻底版无单元层,分区归属由框架管理)
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

-- 9) 锚点索引器分区污染死信(C1):解析失败后该分区不再产栅栏(查询退化 fallback);
--    人工介入确认后 DELETE 该行恢复采样 —— 与 consumer_dlq 同为"绝不静默丢弃"的死信语义
CREATE TABLE IF NOT EXISTS chg_anchor_poison (
  topic        VARCHAR(255) NOT NULL,
  partition_   INT          NOT NULL,
  epoch        INT          NOT NULL,
  poisoned_at  DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (topic, partition_, epoch)
) COMMENT 'v2 索引器分区污染死信(C1)';

-- 10) 批阶段终态物化(补偿对账判据载体):批执行体心跳续租, 终态含 t0(交接输入);
--     handover_at 为交接完成标记(CP2b 判据收敛依据: 补发成功即回写, 否则判据永不收敛)
CREATE TABLE IF NOT EXISTS batch_job (
  task_id       VARCHAR(128) NOT NULL,
  state         VARCHAR(16)  NOT NULL COMMENT 'RUNNING|SUCCEEDED|FAILED',
  t0            BIGINT       NULL COMMENT '批完成时刻(交接输入, 算不出来必须物化)',
  handover_at   DATETIME     NULL COMMENT '交接完成标记(CP2b 收敛依据: 补发成功即回写)',
  heartbeat_at  DATETIME     NULL COMMENT '批执行体续约(每 30s 刷新)',
  create_time   DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
  update_time   DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (task_id)
) COMMENT 'v2 批阶段终态物化(对账判据载体)';

-- 11) ShedLock 分布式锁表(定时任务多实例互斥; 与 backend 元库结构一致, 各服务锁名不冲突可共用一份)
CREATE TABLE IF NOT EXISTS shedlock (
  name       VARCHAR(64)  NOT NULL,
  lock_until TIMESTAMP(3) NOT NULL,
  locked_at  TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  locked_by  VARCHAR(255) NOT NULL,
  PRIMARY KEY (name)
) COMMENT 'ShedLock 分布式锁(定时任务多实例互斥)';
