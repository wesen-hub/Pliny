#!/bin/bash
# 云机 x86 原生构建 10 个集群镜像(源码上下文已解压到 /opt/dcp/build)
set -e
cd /opt/dcp/build

for m in backend outlet-service subscription-service tagengine-service asset-service delivery-service dcp-worker dcp-gateway; do
  img="dcp-${m}:cluster-x86"
  [ "$m" = "dcp-worker" ] && img="dcp-worker:cluster-x86"
  [ "$m" = "dcp-gateway" ] && img="dcp-gateway:cluster-x86"
  echo "==> build $m -> $img"
  docker build -t "$img" "$m"
done

echo "==> build frontend(双站点 nginx)"
docker build -f deploy/cluster-frontend/Dockerfile -t dcp-frontend:cluster-x86 .
echo "==> build seatunnel(插件 + 发行版)"
docker build -t dcp-seatunnel:cluster-x86 seatunnel

echo "==> docker save(供传阿里云)"
docker save \
  dcp-backend:cluster-x86 dcp-outlet-service:cluster-x86 dcp-subscription-service:cluster-x86 \
  dcp-tagengine-service:cluster-x86 dcp-asset-service:cluster-x86 dcp-delivery-service:cluster-x86 \
  dcp-worker:cluster-x86 dcp-gateway:cluster-x86 dcp-frontend:cluster-x86 dcp-seatunnel:cluster-x86 \
  -o /opt/dcp/dcp-images-x86.tar

echo "==> 完成: $(ls -la /opt/dcp/dcp-images-x86.tar)"
