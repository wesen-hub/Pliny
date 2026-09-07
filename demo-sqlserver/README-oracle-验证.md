# Oracle CDC 链路验证记录（2026-09-01）

## 结论：✅ 全部通过
- 快照：seed 5 行正确落库（分桶 00/03/04/05），手机号脱敏正常（138****2222 等）
- 增量：UPDATE（张三 city 广州→杭州，seq 16007）+ INSERT（钱七，seq 16008）均落库
- 水位：lake_watermark 推进到 11004（lag 归零）
- LogMiner 在 CDB 执行，无 ORA-65040

## 踩坑与修复（3 个 ASM 字节码补丁 + 2 个后端修复）
环境：Oracle 23ai（gvenzl/oracle-free:slim 26ai），CDB=FREE，PDB=FREEPDB1，c##dcp

### 1. SPI 补丁（此前已有）
connector-cdc-oracle-2.3.12.jar 需裁剪 Factory SPI + 补 CatalogFactory SPI
→ seatunnel/patch_cdc_spi.py

### 2. 版本解析失败（"Failed to resolve Oracle database version"）
根因：新 gvenzl 镜像 banner 是 "Oracle AI Database 26ai..."，Debezium 查询
`WHERE BANNER LIKE 'Oracle Database%'` 匹配不到；且 BANNER_FULL 列是多行的
（含 \nVersion...），正则 `(?:.*)` 不匹配换行。
修复：OracleConnection.class 版本查询改为
`SELECT BANNER FROM V$VERSION WHERE BANNER LIKE 'Oracle%'`（单行 BANNER 列）
→ /tmp/PatchOracleVersion2.java

### 3. connection.pdb.name 注入（Debezium 引擎配置）
SeaTunnel Oracle-CDC 的 dbzProperties 来自 source 配置的 `debezium: {...}` 块
（DEBEZIUM_PROPERTIES option，cdc-base JdbcSourceConfigFactory.fromReadonlyConfig
→ dbzProperties.putAll → create() 里 props.putAll(dbzProperties)）。
后端 CdcStService oracle 分支加：
```java
s.put("debezium", java.util.Map.of("database.pdb.name", task.getSourceDb()));
```
注意 key 是 database.pdb.name（Debezium Oracle 的 PDB_NAME 常量），不是 connection.pdb.name。

### 4. schema 读取连接切 PDB（OracleDialect.openJdbcConnection + OracleSchema.readTableSchema）
SeaTunnel 的 schema 读取连接不走 Debezium 的 setSessionToPdb（Debezium 引擎内部
会切，但 SeaTunnel 自己的 OracleSchema 连接不会）。两个 patch：
- OracleDialect.openJdbcConnection: 创建连接后调 setSessionToPdb(getPdbName())
- OracleSchema.readTableSchema: readSchema 前调 setSessionToPdb(getPdbName())
  （覆盖 SplitFetcher 的 registerDatabaseHistory 路径，其连接不走 openJdbcConnection）
→ /tmp/PatchOracleDialect.java（COMPUTE_MAXS + 手动 visitFrame！）
→ /tmp/PatchOracleSchemaPdb.java（COMPUTE_FRAMES，从干净基线打避免重复注入）

### 5. database-names[0] 必须 = PDB 名（后端）
discoverDataCollections 把 TableId 的 catalog 强制替换为 database.dbname（=database-names[0]），
而 readSchema 结果 catalog = pdb.name（resolveCatalogName 优先 pdb.name）→ 不一致
→ schemasByTableId.containsKey(参数 tableId) 失败 → "Can't obtain schema"。
修复：后端 database-names 改为 List.of(task.getSourceDb(), task.getSourceDb())。
同时 probeSchema 的 table 路径 catalog 也用 PDB 名（task.getSourceDb()）。

## 重建补丁 jar 的命令（从镜像基线 /tmp/imgjars/oracle-from-image.jar）
```
java -cp .:asm-9.5.jar PatchOraclePdb jar          # factory: database.pdb.name 注入
java -cp .:asm-9.5.jar PatchOracleVersion2 jar     # 版本查询单行 BANNER
java -cp .:asm-9.5.jar PatchOracleDialect jar      # openJdbcConnection 切 PDB(COMPUTE_MAXS+visitFrame)
java -cp .:asm-9.5.jar PatchOracleSchemaPdb jar    # readTableSchema 切 PDB(COMPUTE_FRAMES, classpath 需 oracle+cdc-base jar)
```
全部从干净基线按序打，勿在已打过的 jar 上重复（会重复注入）。

## 调试技巧
- System.out.println 进 docker logs（logback 文件里没有），slf4j 日志进文件
- 在容器里跑测试类：host javac --release 17 -cp dist jars → docker cp → java -cp 容器 jars
- 容器 JRE 无 javac；single-file source 启动不可用（无 jdk.compiler 模块）

## 示例任务（前端"同步任务"列表）
sync_task 表示例任务不 seed（DataInitializer 只 seed 数据源/job 体系），验证时通过 API 创建：
- SQLServer 数据源: `POST /api/datasources` {name:演示SQLServer, type:sqlserver, host:sqlserver, port:1433,
  databaseName:source_db, username:sa, password:DcpDemo@123456} → id=9
- Oracle 示例任务: `POST /api/sync/tasks` {name:Oracle客户同步, logicalTable:t_oracle_wide, datasourceId:8,
  sourceDb:FREEPDB1, sourceTable:T_ORACLE_USER, fieldMapping: 大写 srcField(ID_CARD_NO/NAME/PHONE[mask]/CITY)+seq,
  businessKey: merge(id_card_no)} → id=8
- SQLServer 示例任务: 同上(logicalTable:t_sqlserver_wide, datasourceId:9, sourceDb:source_db,
  sourceTable:t_sqlserver_user, 小写 srcField) → id=9
- 前端"启动采集"按钮走 `POST /api/sync/cdc-st/start/{logicalTable}`; Oracle 可真实启动(CDC 已验证),
  SQLServer 需先注册 cdc_st_task 且环境需 x86_64

## 数据资产模块接通(2026-09-01 12:xx)
前端 views/asset/{Catalog,Detail,Lineage}.vue 由 mock 改为真实后端 API:
- 后端新增: `sync/AssetController` + `sync/AssetService`(GET /api/assets, /api/assets/{table},
  /api/assets/{table}/preview?limit=, /api/assets/{table}/lineage)
- 数据来源: logical_table(资产) + cdc_st_task/datasource_config(来源) + lake_watermark(水位)
  + 目标分表 information_schema(字段类型) + 分表真实数据(预览)
- 敏感标记: field_mapping 含 mask 步骤 → sensitive=true; 血缘 = 字段映射链(源表.字段→模型字段)
- ⚠️ 踩坑: SyncConfig 若暴露 DataSource bean(targetDataSource) 会触发 DataSourceAutoConfiguration
  @ConditionalOnMissingBean 跳过主元库数据源 → 全库连到 target_db(元库查询全空)。
  必须保持"目标库连接内部自建"(AtomicUpsertDao/AssetService 各自 new DriverManagerDataSource)
- 前端 api/index.js 新增 listAssets/getAssetDetail/getAssetPreview/getAssetLineage

## 资产编目可维护化(2026-09-01 13:30-14:00)
需求: 资产目录应是"可维护、多层级、多维度"的(非纯派生视图)。
- **3 张新表**(schema.sql, mode=always 幂等):
  - asset_catalog_node(节点: dim维度/parent_id层级/name/sort)
  - asset_meta(资产元数据: 展示名/域/分层/密级/负责人/质量/标签, table_name 关联逻辑表)
  - asset_node_binding(资产↔节点挂载, 多维度)
- **4 维度**: biz业务域/src来源系统/sec密级/custom自定义; 默认根节点初始化(密级 4 子节点)
  - src 维度按来源系统**自动分组**(不落库); 其余维度用户维护节点+挂载
  - 未挂载资产自动进"未分类"虚拟节点
- **API**(AssetCatalogController): tree?dim= / assets / nodes CRUD / bind+unbind / assets/{table} PUT(元数据)
- **前端 Catalog.vue**: 四视图切换; 节点 hover 操作(新建子/重命名/删除); 选中节点后资产行"挂载";
  资产"编辑"弹元数据表单(展示名/域/分层/密级/负责人/质量/标签/描述)
- 踩坑: nodeMap 组装漏 parent_id → NPE; 树组装漏"子节点挂父节点" → 层级丢失(密级子节点不显示)

## 标签体系接通(2026-09-01 14:00)
- 表: tag_dict(多级标签字典: parent_id/name/category/granularity/source/sort/status)
- 默认初始化: 6 个一级(业务域/来源系统/密级/分层/敏感字段/自定义标签) + 常用二级(交易/财务/客户, MySQL/PG/Oracle/MongoDB, 密级4, 分层4, 手机号/金额)
- 资产打标: asset_meta.tags 存**标签 path 数组**(前端 el-tree-select 多选); 手动标签无需先建字典(允许任意 path)
- **系统标签自动统计**(不落库): 来源系统/X=dsName contains X 或 dsType equalsIgnoreCase X; 分层/X=layer 等值; 密级/X=secretLevel
- **父子聚合**: 父标签资产数=全部子孙前缀匹配(业务域=业务域/交易=交易下所有)
- API: GET /api/tags/tree(含 path/assets 数) / GET /api/tags/assets?path= / POST /api/tags / PUT /api/tags/{id} / DELETE /api/tags/{id}
  - ⚠️ path 含中文斜杠, 用 query 参数而非路径参数(避免 URL 编码/多段路由问题)
- 前端: tag/List.vue 接 API(树表格+新建/编辑/删除+点资产数看资产列表); Catalog 资产编辑的标签改 el-tree-select(从 tag_dict 加载)
- 踩坑: ①asset_meta.tags JSON 列 queryForMap 返回 String, 需 om.readValue 解析成 List(否则手动标签匹配不上) ②assetTagPaths 的 tagMap 键要用 "tag"(buildPath 取 tag), 写 name 会全 0
