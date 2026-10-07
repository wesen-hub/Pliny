-- 腾讯云 MySQL 从库初始化: 建立与 RDS 主库的 binlog 复制(演练 H4 读写分离)
-- 前置: RDS 侧已创建复制账号 repl 并开启 binlog; 主库已通过 mysqldump --master-data=2 导出并导入本从库
-- 容器启动后手动执行(或由部署脚本代跑):
--   docker exec mysql-slave mysql -uroot -p -e "CHANGE REPLICATION SOURCE TO
--     SOURCE_HOST='<RDS_HOST>',SOURCE_PORT=3306,SOURCE_USER='repl',SOURCE_PASSWORD='<REPL_PWD>',
--     SOURCE_AUTO_POSITION=1; START REPLICA;"

-- 只读校验(从库 read-only=ON 由容器参数保证, 此处仅为部署时人工核对提示)
SELECT 'mysql-slave initialized; run CHANGE REPLICATION SOURCE manually' AS notice;
