# asset-service —— 资产目录 + 血缘域独立服务(方案 N12)

承载 `backend/module/assetcat` + `backend/module/lineage` 迁出的**资产与血缘域**全部能力。

## 职责

- **资产目录**：人工业务打标、标签树、资产元信息管理
- **血缘**：Neo4j 血缘图写入与查询、血缘重建任务
- 打标管理面走用户 JWT + `@perm.isAdmin`（与 backend 同套安全套件）；目录/血缘读接口与血缘重建也需登录（前端带 token）；仅 actuator permitAll

## 包结构

```
com/dcp/
├── asset/                 入口 AssetServiceApplication
├── module/assetcat/       资产目录(人工业务打标/标签树)
├── module/lineage/        血缘(Neo4j 图读写/重建)
├── module/system/         登录/用户体系
├── security/              JWT 过滤器与 @perm 权限套件
└── config/                数据源/MyBatis/Neo4j 装配
```

## 与其他模块的关系

- **Neo4j**：血缘图存储（因果集群；单机演示亦可）
- 血缘事件来源：同步/打标链路的变更经底座投影后触发血缘写入

## 构建与运行

```bash
mvn -q package -DskipTests
java -jar target/asset-service.jar   # 端口 8099
```
