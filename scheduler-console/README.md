# scheduler-console —— 调度控制台前端（Vue3 + Vite）

统一调度框架的运维控制台前端：分区调度、实例状态、任务大盘可视化。

## 目录结构

```
scheduler-console/
├── src/           页面与组件
├── nginx.conf     SPA 静态托管 + 反代调度接口
├── Dockerfile     容器化
└── dist/          构建产物
```

## 对接后端

- 调度接口由统一调度框架 REST 暴露（dcp-worker 内嵌 `com.dcp.scheduler.rest`，scanBasePackages 包含；控制面在 backend 的调度管理接口）
- 当前 `nginx.conf` 仅 SPA 静态托管；开发态由 vite 代理、生产态待治理化后经 Nacos Gateway 接入调度接口

## 构建与运行

```bash
npm install
npm run dev      # 开发
npm run build    # 构建 dist/ 供 nginx 托管
```
