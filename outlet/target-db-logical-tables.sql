-- ============================================================================
-- A6 补充:为新集群补建**元库已注册**的逻辑表(biz_orders / rt_orders_b / rt_sync_c / t_oms_sync)
-- 依据:元库 logical_table 注册了这 4 张表(topic/prefix/table_count=1/key_columns),
--       但新集群 target_db 里没有 → 写段即使收到消息也写不进去。
-- 列:每表 = 键列 + 业务列(演示用) + 平台 seq 收敛列 + 出口 _commit_ts
-- 执行:docker exec -i dcp-mysql mysql -h dcp-outlet-tidb1 -P 4000 -uroot < outlet/target-db-logical-tables.sql
-- ============================================================================
CREATE DATABASE IF NOT EXISTS target_db;

-- rt_sync_c:key = id
DROP TABLE IF EXISTS target_db.rt_sync_c;
CREATE TABLE target_db.rt_sync_c (
  id         BIGINT      NOT NULL,
  v          VARCHAR(64) NULL,
  seq        BIGINT      NOT NULL DEFAULT 0,
  _commit_ts BIGINT      NOT NULL DEFAULT 0,
  PRIMARY KEY (id)
) ENGINE=InnoDB COMMENT='逻辑表 rt_sync_c(topic dcp.rt_sync_c)';

DROP TABLE IF EXISTS target_db.rt_sync_c_authority;
CREATE TABLE target_db.rt_sync_c_authority LIKE target_db.rt_sync_c;

-- biz_orders:key = order_id
DROP TABLE IF EXISTS target_db.biz_orders;
CREATE TABLE target_db.biz_orders (
  order_id   BIGINT        NOT NULL,
  amount     DECIMAL(12,2) NULL,
  status     VARCHAR(16)   NULL,
  seq        BIGINT        NOT NULL DEFAULT 0,
  _commit_ts BIGINT        NOT NULL DEFAULT 0,
  PRIMARY KEY (order_id)
) ENGINE=InnoDB COMMENT='逻辑表 biz_orders(topic dcp.biz_orders)';

DROP TABLE IF EXISTS target_db.biz_orders_authority;
CREATE TABLE target_db.biz_orders_authority LIKE target_db.biz_orders;

-- rt_orders_b:key = order_id
DROP TABLE IF EXISTS target_db.rt_orders_b;
CREATE TABLE target_db.rt_orders_b (
  order_id   BIGINT        NOT NULL,
  amount     DECIMAL(12,2) NULL,
  seq        BIGINT        NOT NULL DEFAULT 0,
  _commit_ts BIGINT        NOT NULL DEFAULT 0,
  PRIMARY KEY (order_id)
) ENGINE=InnoDB COMMENT='逻辑表 rt_orders_b(topic dcp.rt_orders_b)';

DROP TABLE IF EXISTS target_db.rt_orders_b_authority;
CREATE TABLE target_db.rt_orders_b_authority LIKE target_db.rt_orders_b;

-- t_oms_sync:key = id
DROP TABLE IF EXISTS target_db.t_oms_sync;
CREATE TABLE target_db.t_oms_sync (
  id         BIGINT      NOT NULL,
  order_no   VARCHAR(32) NULL,
  seq        BIGINT      NOT NULL DEFAULT 0,
  _commit_ts BIGINT      NOT NULL DEFAULT 0,
  PRIMARY KEY (id)
) ENGINE=InnoDB COMMENT='逻辑表 t_oms_sync(topic dcp.t_oms_sync)';

DROP TABLE IF EXISTS target_db.t_oms_sync_authority;
CREATE TABLE target_db.t_oms_sync_authority LIKE target_db.t_oms_sync;

SELECT '逻辑表(注册表)补建完成' AS result;
