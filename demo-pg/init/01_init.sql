-- PostgreSQL CDC 演示源库(首次初始化)
CREATE TABLE t_pg_user (
  id BIGSERIAL PRIMARY KEY,
  id_card_no VARCHAR(32) NOT NULL UNIQUE,
  name VARCHAR(64) NOT NULL,
  phone VARCHAR(16),
  city VARCHAR(64),
  updated_at TIMESTAMP DEFAULT now()
);
INSERT INTO t_pg_user (id_card_no, name, phone, city) VALUES
('110101199001011234', '张三', '13800138000', '北京'),
('310104198505152345', '李四', '13900139000', '上海'),
('440305199212123456', '王五', '13700137000', '深圳');
