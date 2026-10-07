#!/bin/bash
# 打包云机构建上下文(源码 + 预置 dist)并 xz 压缩, 供上传腾讯云构建
set -e
cd "$(dirname "$0")/.."
OUT=deploy/image-tars/dcp-src-context.tar.xz
rm -f "$OUT"

tar -cf - \
  --exclude='*/target' \
  --exclude='*/node_modules' \
  --exclude='apache-seatunnel-2.3.12-bin.tar.gz' \
  --exclude='*.jar.bak' \
  --exclude='*.bak' \
  backend outlet-service subscription-service tagengine-service asset-service \
  delivery-service dcp-worker dcp-gateway \
  seatunnel \
  frontend/dist scheduler-console/dist deploy/cluster-frontend \
  | xz -9 -T4 > "$OUT"

ls -la "$OUT"
