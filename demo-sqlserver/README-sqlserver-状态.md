# SQLServer CDC 链路状态（2026-09-01）

## ❌ 本地 Apple Silicon 无法端到端验证
- `mcr.microsoft.com/mssql/server:2019-latest` 和 `2022-latest` 在 Docker Desktop
  Rosetta（`platform: linux/amd64`）下均无法启动：
  ```
  Invalid mapping of address 0x2aaa... in reserved address space below 0x400000000000
  ```
  （SQL Server 二进制的 VA 布局要求与 Rosetta 模拟冲突；ulimits stack 限制无效）
- Azure SQL Edge（arm64 原生镜像）不支持 SQL Server Agent → 无 CDC 捕获作业 → 无法验证 CDC

## ✅ 代码/配置已就绪（需 x86_64 环境验证）
- `demo-sqlserver/init/init.sql`：建库 source_db + 表 dbo.t_sqlserver_user（id_card_no 唯一键、
  3 行中文种子）+ 幂等启用 `sp_cdc_enable_db` / `sp_cdc_enable_table`
- `docker-compose.yml` sqlserver 服务：2019-latest + linux/amd64 + MSSQL_AGENT_ENABLED=true +
  sqlserver-data 卷 + sqlcmd healthcheck（restart: "no" 避免本地崩溃循环）
- 后端 `CdcStService` SqlServer-CDC 分支 + probeSchema（mssql-jdbc-12.6.1 已在 pom 与容器
  /opt/plugins/connector-cdc-oracle/ 下）
- 验证步骤（在 x86_64 机器上）：
  1. `docker compose up -d sqlserver`，等 healthy（Rosetta 下更慢）
  2. `docker exec -i dcp-sqlserver /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P 'DcpDemo@123456' -C -i /init/init.sql`
  3. 注册数据源（type=sqlserver, databaseName=source_db）+ 逻辑表 t_sqlserver_wide（键 id_card_no）
     + cdc_st_task（源表 dbo.t_sqlserver_user）→ start → 验证快照+增量
  4. 注意：SQL Server CDC 捕获作业依赖 Agent（MSSQL_AGENT_ENABLED=true 已设）；
     SeaTunnel SqlServer-CDC 的 table-names 格式为 `db.dbo.table`（catalog 缺陷待实测确认，
     PG/Oracle 都需显式 schema）

## 相关
- Oracle CDC 链路已完整验证通过：见 README-oracle-验证.md
