# frontend —— 平台主前端（Vue3 + Vite）

数据采集平台的主前端：全部管理界面（数据源/建模/资产/订阅/打标/出口等），nginx 静态托管 + 路径反代。

## 目录结构

```
frontend/src/
├── api/           接口层(request.js 封装 + index.js/tagengine.js 各域 API)
├── layout/        布局(Layout.vue, 含 JWT 登录态)
├── router/        路由
├── store/         用户状态(user.js)
├── utils/         工具(dbLevels.js 等)
├── views/         页面(agent/asset/datasource/datasub/... 按域组织)
├── App.vue
└── main.js
```

## 接口接入方式

- 开发态：vite 代理到本地 backend
- 生产态：`nginx.conf` 按路径反代——`/api/outlet/ → outlet-service`、`/api/tagengine/ → tagengine-service`、`/api/data-sub/ → subscription-service`、`/api/asset-catalog|lineage/ → asset-service`、`/api/chg|outlet/contracts/ → delivery-service`、其余 `/api/ → backend`（上游写死容器名，治理化后改走 Nacos Gateway lb:// 路由）

## 构建与运行

```bash
npm install
npm run dev      # 开发
npm run build    # 构建 dist/ 供 nginx 托管
```
