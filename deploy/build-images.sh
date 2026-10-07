#!/bin/bash
# 本机构建最小集群全部镜像(打 cluster 标签)并打包 tar 供 scp 传输
set -e
cd "$(dirname "$0")/.."
OUT=deploy/image-tars
mkdir -p "$OUT"

echo "==> 1/2 构建应用镜像(cluster 标签)"
for m in backend outlet-service subscription-service tagengine-service asset-service delivery-service dcp-worker dcp-gateway; do
  img="dcp-${m}:cluster"
  [ "$m" = "dcp-worker" ] && img="dcp-worker:cluster"
  [ "$m" = "dcp-gateway" ] && img="dcp-gateway:cluster"
  echo "  build $m -> $img"
  docker build -q -t "$img" "$m" >/dev/null
done

echo "==> 2/2 构建 frontend(双站点) 与 seatunnel(补丁)"
docker build -q -f deploy/cluster-frontend/Dockerfile -t dcp-frontend:cluster . >/dev/null
docker build -q -t dcp-seatunnel:cluster seatunnel >/dev/null

echo "==> 打包镜像 tar -> $OUT"
docker save \
  dcp-backend:cluster dcp-outlet-service:cluster dcp-subscription-service:cluster \
  dcp-tagengine-service:cluster dcp-asset-service:cluster dcp-delivery-service:cluster \
  dcp-worker:cluster dcp-gateway:cluster dcp-frontend:cluster dcp-seatunnel:cluster \
  -o "$OUT/dcp-images.tar"

echo "==> 完成: $OUT/dcp-images.tar ($(du -h "$OUT/dcp-images.tar" | cut -f1))"
echo "    传输: scp $OUT/dcp-images.tar ubuntu@<云机>:/opt/dcp/ && ssh <云机> docker load -i /opt/dcp/dcp-images.tar"
