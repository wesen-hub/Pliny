-- ============================================================================
-- A5 · 数据面重建(2026-09-14)
-- 依据:决策 13「现有单节点 TiDB 不保留、旧数据不迁移」→ 只重建结构与演示数据。
--
-- 平台约定(取自代码,非猜测):
--   · 目标表带 `seq BIGINT` —— AtomicUpsertDao 的幂等收敛列(ODKU + seq = GREATEST(seq, VALUES(seq)))
--   · 逻辑表物理表 = {table_prefix};权威副本 = {table_prefix}_authority(AuthorityMerger)
--   · 出口侧再加 `_commit_ts BIGINT` —— 事件 TSO 版本列(设计 §6.4.1 衔接契约 4)
--
-- 执行:docker exec -i dcp-mysql mysql -h dcp-outlet-tidb1 -P 4000 -uroot < outlet/target-db-rebuild.sql
-- ============================================================================
CREATE DATABASE IF NOT EXISTS target_db;

-- 1) 订单宽表(单表目标,幂等 REPLACE 演示)
DROP TABLE IF EXISTS target_db.dw_order;
CREATE TABLE target_db.dw_order (
  id          BIGINT       NOT NULL COMMENT '订单ID',
  userId      BIGINT       NOT NULL COMMENT '用户ID',
  price       DECIMAL(10,2) NOT NULL COMMENT '单价',
  quantity    INT          NOT NULL COMMENT '数量',
  discount    DECIMAL(4,2) NOT NULL COMMENT '折扣',
  statusDesc  VARCHAR(16)  NULL COMMENT '状态中文(字典转换)',
  orderDate   VARCHAR(10)  NULL COMMENT '下单日期(表达式转换)',
  totalAmount DECIMAL(12,2) NULL COMMENT '总金额(表达式计算)',
  seq         BIGINT       NOT NULL DEFAULT 0 COMMENT '平台幂等收敛列',
  _commit_ts  BIGINT       NOT NULL DEFAULT 0 COMMENT '出口事件版本列(事件 TSO)',
  PRIMARY KEY (id)
) ENGINE=InnoDB COMMENT='订单宽表(目标)';

-- 2) 多表合并宽表(逻辑表 customer_wide)+ 权威副本
DROP TABLE IF EXISTS target_db.customer_wide;
CREATE TABLE target_db.customer_wide (
  id_card_no VARCHAR(32)  NOT NULL COMMENT '身份证号(跨系统公共业务键)',
  name       VARCHAR(64)  NULL COMMENT '姓名(源A)',
  phone      VARCHAR(16)  NULL COMMENT '手机号(源A)',
  addr       VARCHAR(128) NULL COMMENT '住址(源B)',
  level      INT          NULL COMMENT '会员等级(源B)',
  seq        BIGINT       NOT NULL DEFAULT 0 COMMENT '平台幂等收敛列(字段级)',
  _commit_ts BIGINT       NOT NULL DEFAULT 0 COMMENT '出口事件版本列',
  PRIMARY KEY (id_card_no)
) ENGINE=InnoDB COMMENT='客户宽表(多表合并目标)';

DROP TABLE IF EXISTS target_db.customer_wide_authority;
CREATE TABLE target_db.customer_wide_authority (
  id_card_no VARCHAR(32)  NOT NULL,
  name       VARCHAR(64)  NULL,
  phone      VARCHAR(16)  NULL,
  addr       VARCHAR(128) NULL,
  level      INT          NULL,
  seq        BIGINT       NOT NULL DEFAULT 0,
  _commit_ts BIGINT       NOT NULL DEFAULT 0,
  PRIMARY KEY (id_card_no)
) ENGINE=InnoDB COMMENT='客户宽表权威副本(= 单表全量,替代 Iceberg 的演示形态)';

-- 3) 核心单表 + 权威表(清单 A5 判据:核心表可写入)
DROP TABLE IF EXISTS target_db.t_tidb_single;
CREATE TABLE target_db.t_tidb_single (
  id         BIGINT      NOT NULL,
  v          VARCHAR(64) NULL,
  seq        BIGINT      NOT NULL DEFAULT 0,
  _commit_ts BIGINT      NOT NULL DEFAULT 0,
  PRIMARY KEY (id)
) ENGINE=InnoDB COMMENT='TiDB 单表演示表';

DROP TABLE IF EXISTS target_db.t_tidb_single_authority;
CREATE TABLE target_db.t_tidb_single_authority (
  id         BIGINT      NOT NULL,
  v          VARCHAR(64) NULL,
  seq        BIGINT      NOT NULL DEFAULT 0,
  _commit_ts BIGINT      NOT NULL DEFAULT 0,
  PRIMARY KEY (id)
) ENGINE=InnoDB COMMENT='TiDB 单表权威副本';

-- 4) MongoDB → MySQL 同步演示目标表
DROP TABLE IF EXISTS target_db.mongo_users;
CREATE TABLE target_db.mongo_users (
  id         VARCHAR(32) PRIMARY KEY,
  name       VARCHAR(64) NOT NULL,
  age        INT         NULL,
  email      VARCHAR(128) NULL,
  created_at DATETIME    NULL,
  status     VARCHAR(16) NULL,
  seq        BIGINT      NOT NULL DEFAULT 0,
  _commit_ts BIGINT      NOT NULL DEFAULT 0
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Mongo 同步目标表';

-- 5) 演示数据(少量,够跑通链路与验收断言)
INSERT INTO target_db.dw_order VALUES
 (1, 1001,  99.00, 2, 0.00, '待支付', '2026-08-01', 198.00, 1, 0),
 (2, 1002, 199.00, 1, 0.20, '已支付', '2026-08-01', 159.20, 2, 0);

INSERT INTO target_db.customer_wide (id_card_no, name, phone, addr, level, seq, _commit_ts) VALUES
 ('110101199001011234', '张三', '13800138000', '北京市朝阳区建国路88号', 2, 1, 0),
 ('310104198505152345', '李四', '13900139000', '上海市浦东新区世纪大道100号', 1, 2, 0);

INSERT INTO target_db.t_tidb_single VALUES (1, 'alpha', 1, 0), (2, 'beta', 2, 0);

SELECT 'target_db 重建完成' AS result;
