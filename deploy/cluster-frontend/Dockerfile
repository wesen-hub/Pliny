# 集群版 frontend: 单 nginx 容器承载两个站点
# 80    = 主前端 SPA(frontend/dist)
# 8085  = scheduler-console SPA
# 两个站点的 /api/ 都转同机 gateway(host 网络, 127.0.0.1:8088)
FROM nginx:1.27-alpine

COPY frontend/dist /usr/share/nginx/html
COPY scheduler-console/dist /usr/share/nginx/html-console
COPY deploy/cluster-frontend/nginx.conf /etc/nginx/conf.d/default.conf
