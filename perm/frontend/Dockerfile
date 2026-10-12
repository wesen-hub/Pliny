# 宿主机本地执行 npm run build 生成 dist 后，Docker 仅打包静态产物
# （容器内 DNS 无法访问 npm registry，故构建在宿主机完成）
# 基础镜像：本机已有 perm-frontend:latest（nginx 形态）；
# 网络可达时可用 nginx:alpine 替换（docker hub 直连 TLS 超时）
FROM perm-frontend:latest
COPY dist /usr/share/nginx/html
COPY nginx.conf /etc/nginx/conf.d/default.conf
EXPOSE 80
CMD ["nginx", "-g", "daemon off;"]
