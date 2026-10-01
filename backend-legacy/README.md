# backend-legacy —— 微服务拆分前的旧代码备份

> 本目录是 backend 单体拆分（方案 N9-N12）之前的旧代码**备份**，不参与编译与部署，与现有代码物理隔离。

## 备份内容

### com/dcp/sync/（9 个文件：迁出域的旧控制器/服务）

| 文件 | 说明 | 现行替代 |
|---|---|---|
| TagController / TagService | 打标域旧接口（`/api/tags`） | tagengine-service（`/api/tagengine/`） |
| AssetController / AssetService | 资产域旧接口（`/api/assets`） | asset-service（`/api/asset-catalog/`） |
| AssetCatalogController / AssetCatalogService | 资产目录旧接口 | asset-service |
| SubscriptionController / SubscriptionService | 订阅域旧接口（`/api/subscriptions`） | subscription-service（`/api/data-sub/`） |
| SyncLakeController | 湖同步旧接口（`/api/sync/lake`） | backend `module/lakesync` |

移出依据：① Java 代码 0 引用（仅旧控制器群内部闭环互引）；② 前端 0 调用；③ 与迁出服务接口重复（nginx 路由已被新服务截胡）。

### 已清空的空壳目录（无代码，仅 .DS_Store，未备份）

`module/assetcat`、`module/datasub`、`module/lineage`、`module/outlet`、`module/scheduler`、`module/tagengine`、`tagjob`——迁出后仅剩空目录树，已直接清理。

## 完整旧代码的双保险

git HEAD（e258e78）仍保留拆分前的全部历史代码，本目录仅作工作区层面的可读备份。
