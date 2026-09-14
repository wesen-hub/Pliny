-- ============================================================================
-- 出口模块元库表 + 演示种子(2026-09-14)
-- 表与《变更流出口-表结构DDL-2026-09-14.sql》一致;此处用于**平台元库(dcp)**,
-- 便于注册表走 mapper 模式、快照任务/物化清单落库。
-- 说明:设计里 consumer_offset 属于"消费方自己的库";平台内部消费方(演示)直接用元库。
-- 执行:docker exec -i dcp-mysql mysql -h mysql -P 3306 -uroot -proot123 < outlet/outlet-meta-ddl.sql
-- ============================================================================
USE dcp;

CREATE TABLE IF NOT EXISTS consumer (
  consumer_id     VARCHAR(64)  NOT NULL,
  name            VARCHAR(128) NOT NULL,
  consumer_type   VARCHAR(32)  NOT NULL,
  owner           VARCHAR(64)  NOT NULL,
  status          VARCHAR(16)  NOT NULL DEFAULT 'active',
  channel         VARCHAR(16)  NOT NULL DEFAULT 'api',
  executor        VARCHAR(16)  NOT NULL DEFAULT 'stream',
  subject_id      VARCHAR(64)  NULL,
  quota_qps       INT          NOT NULL DEFAULT 10,
  quota_rows_day  BIGINT       NOT NULL DEFAULT 1000000,
  quota_conn      INT          NOT NULL DEFAULT 4,
  create_time     DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  update_time     DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (consumer_id)
) ENGINE=InnoDB COMMENT='消费方服务账号(出口)';

CREATE TABLE IF NOT EXISTS contract (
  contract_id      VARCHAR(64)  NOT NULL,
  consumer_id      VARCHAR(64)  NOT NULL,
  streams          JSON         NOT NULL,
  column_whitelist JSON         NOT NULL,
  row_condition    TEXT         NULL,
  max_level        INT          NOT NULL DEFAULT 0,
  mask_rules       JSON         NULL,
  channel          VARCHAR(16)  NOT NULL DEFAULT 'api',
  start_policy     VARCHAR(32)  NOT NULL DEFAULT 'snapshot',
  token_hash       VARCHAR(128) NULL,
  token_expire_at  DATETIME(3)  NULL,
  status           VARCHAR(16)  NOT NULL DEFAULT 'active',
  create_time      DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  update_time      DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (contract_id),
  KEY idx_consumer_status (consumer_id, status)
) ENGINE=InnoDB COMMENT='消费契约(出口)';

CREATE TABLE IF NOT EXISTS consumer_offset (
  consumer_id  VARCHAR(64)  NOT NULL,
  stream_id    VARCHAR(128) NOT NULL,
  epoch        INT          NOT NULL,
  position_tso BIGINT       NOT NULL,
  updated_at   DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (consumer_id, stream_id)
) ENGINE=InnoDB COMMENT='消费方位点(权威;演示用元库)';

CREATE TABLE IF NOT EXISTS consumer_offset_seen (
  consumer_id  VARCHAR(64)  NOT NULL,
  stream_id    VARCHAR(128) NOT NULL,
  epoch        INT          NOT NULL,
  position_tso BIGINT       NOT NULL,
  lag_ts       BIGINT       NULL,
  update_time  DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (consumer_id, stream_id)
) ENGINE=InnoDB COMMENT='平台侧位点观测(非权威)';

CREATE TABLE IF NOT EXISTS snapshot_job (
  id            BIGINT       NOT NULL AUTO_INCREMENT,
  consumer_id   VARCHAR(64)  NOT NULL,
  stream_id     VARCHAR(128) NOT NULL,
  contract_id   VARCHAR(64)  NOT NULL,
  boundary_tso  BIGINT       NOT NULL,
  cursor_pk     VARCHAR(255) NULL,
  page_token    VARCHAR(64)  NULL,
  status        VARCHAR(16)  NOT NULL DEFAULT 'READY',
  rows_estimate BIGINT       NULL,
  rows_exported BIGINT       NOT NULL DEFAULT 0,
  expire_at     DATETIME(3)  NOT NULL,
  create_time   DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  update_time   DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (id),
  UNIQUE KEY uk_consumer_stream_boundary (consumer_id, stream_id, boundary_tso),
  KEY idx_status_expire (status, expire_at)
) ENGINE=InnoDB COMMENT='快照任务(出口)';

CREATE TABLE IF NOT EXISTS snapshot_manifest (
  manifest_id   VARCHAR(128) NOT NULL,
  consumer_id   VARCHAR(64)  NOT NULL,
  stream_id     VARCHAR(128) NOT NULL,
  job_type      VARCHAR(16)  NOT NULL,
  boundary_tso  BIGINT       NULL,
  lo_tso        BIGINT       NULL,
  hi_tso        BIGINT       NULL,
  staging_path  VARCHAR(512) NULL,
  rows_written  BIGINT       NULL,
  checksum      VARCHAR(128) NULL,
  status        VARCHAR(16)  NOT NULL DEFAULT 'RUNNING',
  epoch         INT          NOT NULL,
  publish_time  DATETIME(3)  NULL,
  create_time   DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  PRIMARY KEY (manifest_id),
  KEY idx_consumer_stream_status (consumer_id, stream_id, status)
) ENGINE=InnoDB COMMENT='物化清单(批通道)';

-- ------------------------------------------------------------------ 演示种子
-- ① 内部消费方(打标/清洗类,走出口不豁免)+ 宽契约
INSERT INTO consumer (consumer_id, name, consumer_type, owner, status, channel, executor, subject_id, quota_qps, quota_rows_day, quota_conn)
VALUES ('demo-tag', '演示打标任务', 'tag', 'wesen', 'active', 'internal', 'stream', 'role_data_engineer', 50, 10000000, 4)
ON DUPLICATE KEY UPDATE name = VALUES(name);

INSERT INTO contract (contract_id, consumer_id, streams, column_whitelist, row_condition, max_level, mask_rules, channel, start_policy, status)
VALUES ('ct-tag-rt_sync_c', 'demo-tag', '["rt_sync_c"]', '{"rt_sync_c":["id","v","seq"]}', NULL, 3, '{}', 'internal', 'snapshot', 'active')
ON DUPLICATE KEY UPDATE streams = VALUES(streams), column_whitelist = VALUES(column_whitelist);

-- ② 对外订阅方:列收窄(v 可见、seq 不可见)+ 密级上限 → 用于验证"越权列连 key 都不出现"
INSERT INTO consumer (consumer_id, name, consumer_type, owner, status, channel, executor, subject_id, quota_qps, quota_rows_day, quota_conn)
VALUES ('demo-sub', '演示订阅方', 'subscribe', 'wesen', 'active', 'api', 'stream', 'role_data_viewer', 10, 1000000, 4)
ON DUPLICATE KEY UPDATE name = VALUES(name);

INSERT INTO contract (contract_id, consumer_id, streams, column_whitelist, row_condition, max_level, mask_rules, channel, start_policy, status)
VALUES ('ct-sub-rt_sync_c', 'demo-sub', '["rt_sync_c"]', '{"rt_sync_c":["id","v"]}', NULL, 2, '{}', 'api', 'snapshot', 'active')
ON DUPLICATE KEY UPDATE streams = VALUES(streams), column_whitelist = VALUES(column_whitelist);

SELECT '出口元库表与种子就绪' AS result;
