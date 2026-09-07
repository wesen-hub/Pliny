-- ============================================================
-- 演示数据：源库 source_db（模拟业务系统）+ 目标库 target_db
-- 首次初始化容器时自动执行（docker-entrypoint-initdb.d）
-- ============================================================

CREATE DATABASE IF NOT EXISTS source_db DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
CREATE DATABASE IF NOT EXISTS target_db DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

USE source_db;

DROP TABLE IF EXISTS t_order;
CREATE TABLE t_order (
  id           BIGINT       NOT NULL AUTO_INCREMENT COMMENT '订单ID(增量字段)',
  user_id      BIGINT       NOT NULL COMMENT '用户ID',
  price        DECIMAL(10,2) NOT NULL COMMENT '单价',
  quantity     INT          NOT NULL DEFAULT 1 COMMENT '数量',
  discount     DECIMAL(4,2) NOT NULL DEFAULT 1.00 COMMENT '优惠比例(0.2=打八折, 实付=1-0.2)',
  status       VARCHAR(8)   NOT NULL DEFAULT '0' COMMENT '状态:0待支付 1已支付 2已取消 3已发货',
  created_at   DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  total_amount DECIMAL(12,2) NULL COMMENT '总金额(冗余列,演示用)',
  PRIMARY KEY (id),
  KEY idx_status (status),
  KEY idx_created (created_at)
) ENGINE=InnoDB COMMENT='订单表(源)';

INSERT INTO t_order (id, user_id, price, quantity, discount, status, created_at, total_amount) VALUES
(1, 1001, 99.00, 2, 0.00, '0', '2026-08-01 10:00:00', NULL),
(2, 1002, 199.00, 1, 0.20, '1', '2026-08-01 11:30:00', NULL),
(3, 1001, 59.00, 5, 0.00, '1', '2026-08-02 09:15:00', NULL),
(4, 1003, 3999.00, 1, 0.10, '2', '2026-08-02 14:20:00', NULL),
(5, 1004, 29.90, 10, 0.00, '1', '2026-08-03 16:45:00', NULL),
(6, 1002, 129.00, 3, 0.15, '3', '2026-08-04 08:05:00', NULL),
(7, 1005, 599.00, 1, 0.00, '1', '2026-08-05 20:10:00', NULL),
(8, 1003, 79.00, 4, 0.25, '0', '2026-08-06 12:00:00', NULL);

USE target_db;

DROP TABLE IF EXISTS dw_order;
CREATE TABLE dw_order (
  id          BIGINT       NOT NULL COMMENT '订单ID',
  userId      BIGINT       NOT NULL COMMENT '用户ID',
  price       DECIMAL(10,2) NOT NULL COMMENT '单价',
  quantity    INT          NOT NULL COMMENT '数量',
  discount    DECIMAL(4,2) NOT NULL COMMENT '折扣',
  statusDesc  VARCHAR(16)  NULL COMMENT '状态中文(字典转换)',
  orderDate   VARCHAR(10)  NULL COMMENT '下单日期(表达式转换)',
  totalAmount DECIMAL(12,2) NULL COMMENT '总金额(表达式计算)',
  PRIMARY KEY (id)
) ENGINE=InnoDB COMMENT='订单宽表(目标, 幂等 REPLACE)';

-- 额外插入几条源数据, 用于演示"增量同步"（任务执行后手动执行下面的SQL即可看到增量效果）
-- USE source_db;
-- INSERT INTO t_order (user_id, price, quantity, discount, status, created_at) VALUES
-- (1006, 888.00, 1, 1.00, '1', NOW());

-- MongoDB 演示目标表（用户同步演示: MongoDB -> MySQL）
CREATE TABLE IF NOT EXISTS mongo_users (
  id VARCHAR(32) PRIMARY KEY,
  name VARCHAR(64) NOT NULL,
  age INT NULL,
  email VARCHAR(128) NULL,
  created_at DATETIME NULL,
  status VARCHAR(16) NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ============================================================
-- 多表合并演示源表: 两张源表 → 同一张客户宽表(公共业务键 id_card_no)
--   t_user_base: 身份/联系方式(来源系统 A)
--   t_user_ext : 住址/等级(来源系统 B)
-- 消费端字段级部分更新(ODKU 只动自己的列)+ seq 裁判 → 宽表收敛
-- ============================================================
USE source_db;
SET NAMES utf8mb4;

DROP TABLE IF EXISTS t_user_base;
CREATE TABLE t_user_base (
  id_card_no VARCHAR(32)  NOT NULL COMMENT '身份证号(跨系统公共业务键)',
  name       VARCHAR(64)  NOT NULL COMMENT '姓名',
  phone      VARCHAR(16)  NULL COMMENT '手机号',
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (id_card_no)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='客户基础信息(源A, 多表合并演示)';

INSERT INTO t_user_base (id_card_no, name, phone) VALUES
('110101199001011234', '张三', '13800138000'),
('310104198505152345', '李四', '13900139000'),
('440305199212123456', '王五', '13700137000');

DROP TABLE IF EXISTS t_user_ext;
CREATE TABLE t_user_ext (
  id_card_no VARCHAR(32)  NOT NULL COMMENT '身份证号(跨系统公共业务键)',
  addr       VARCHAR(128) NULL COMMENT '住址',
  level      INT          NOT NULL DEFAULT 1 COMMENT '会员等级 1普通/2银/3金',
  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (id_card_no)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='客户扩展信息(源B, 多表合并演示)';

INSERT INTO t_user_ext (id_card_no, addr, level) VALUES
('110101199001011234', '北京市朝阳区建国路88号', 2),
('310104198505152345', '上海市浦东新区世纪大道100号', 1),
('440305199212123456', '深圳市南山区科技园', 3);
