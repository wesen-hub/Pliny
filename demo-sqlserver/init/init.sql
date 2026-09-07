-- ============================================================
-- 数据采集平台 V3 — SQL Server 演示源库初始化
-- 用法: 容器 healthy 后执行  docker exec -i dcp-sqlserver /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P 'DcpDemo@123456' -C -i /init/init.sql
-- 链路: SQL Server CDC(事务日志) → connector-cdc-sqlserver → CdcSeqTransform → Kafka → 宽表
-- 注意: 表名/列名建议大写风格, CDC 捕获作业依赖 SQL Server Agent(MSSQL_AGENT_ENABLED=true)
-- ============================================================
SET NOCOUNT ON;
GO

IF DB_ID('source_db') IS NULL
BEGIN
    CREATE DATABASE source_db;
END
GO

USE source_db;
GO

IF OBJECT_ID('dbo.t_sqlserver_user', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.t_sqlserver_user (
        id          INT IDENTITY(1,1) PRIMARY KEY,
        id_card_no  NVARCHAR(32)  NOT NULL,
        name        NVARCHAR(64)  NOT NULL,
        phone       NVARCHAR(20)  NOT NULL,
        city        NVARCHAR(64)  NULL,
        updated_at  DATETIME2     DEFAULT SYSDATETIME()
    );
    CREATE UNIQUE INDEX uq_t_sqlserver_user_idcard ON dbo.t_sqlserver_user(id_card_no);
END
GO

-- 种子数据(幂等: 仅当表为空时插入)
IF NOT EXISTS (SELECT 1 FROM dbo.t_sqlserver_user)
BEGIN
    INSERT INTO dbo.t_sqlserver_user (id_card_no, name, phone, city) VALUES
        (N'110101199001011234', N'张三', N'13811112222', N'北京'),
        (N'440305199212123456', N'李四', N'13733334444', N'深圳'),
        (N'310104198505152345', N'王五', N'13955556666', N'上海');
END
GO

-- 启用数据库级 CDC(幂等)
IF NOT EXISTS (SELECT 1 FROM sys.databases WHERE name = 'source_db' AND is_cdc_enabled = 1)
BEGIN
    EXEC sys.sp_cdc_enable_db;
END
GO

-- 启用表级 CDC(幂等)
IF NOT EXISTS (SELECT 1 FROM cdc.change_tables WHERE capture_instance = 'dbo_t_sqlserver_user')
BEGIN
    EXEC sys.sp_cdc_enable_table
        @source_schema = N'dbo',
        @source_name   = N't_sqlserver_user',
        @role_name     = NULL,
        @capture_instance = N'dbo_t_sqlserver_user';
END
GO

SELECT 'init done' AS status, COUNT(*) AS rows FROM dbo.t_sqlserver_user;
GO
