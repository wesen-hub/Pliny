#!/usr/bin/env bash
# 固化内部库 fat jar 产出(消除"内部库缺失"手工坑):
#   内部库按依赖序 install → 拷贝到各服务 dist/(Docker 镜像构建预置 jar 来源)
# 覆盖服务: outlet-service / dcp-worker / backend(仅 scheduler-api)
# 用法: ./scripts/build_outlet_dist.sh   →  然后 docker compose build outlet-service dcp-worker backend
set -euo pipefail
cd "$(dirname "$0")/.."

echo "[1/4] install 内部库(依赖序: scheduler-api → scheduler-core; outlet 系 sdk → control/worker → adapters)"
mvn -q -f dcp-scheduler/pom.xml install -DskipTests
mvn -q -f outlet-sdk/pom.xml install -DskipTests
mvn -q -f outlet-control/pom.xml install -DskipTests
mvn -q -f outlet-worker/pom.xml install -DskipTests
mvn -q -f outlet-adapters/pom.xml install -DskipTests

echo "[2/4] 预置 outlet-service/dist(outlet 系 + scheduler-api)"
mkdir -p outlet-service/dist
for a in outlet-sdk outlet-control outlet-worker outlet-adapters; do
  cp $a/target/dcp-$a-2.0.0-SNAPSHOT.jar outlet-service/dist/
  cp $a/pom.xml outlet-service/dist/dcp-$a-pom.xml
done
cp dcp-scheduler/dcp-scheduler-api/target/dcp-scheduler-api-0.1.0.jar outlet-service/dist/
cp dcp-scheduler/dcp-scheduler-api/pom.xml outlet-service/dist/dcp-scheduler-api-pom.xml

echo "[3/4] 预置 dcp-worker/dist(scheduler-api + scheduler-core + rclone 二进制)"
mkdir -p dcp-worker/dist
cp dcp-scheduler/dcp-scheduler-api/target/dcp-scheduler-api-0.1.0.jar dcp-worker/dist/
cp dcp-scheduler/dcp-scheduler-api/pom.xml dcp-worker/dist/dcp-scheduler-api-pom.xml
cp dcp-scheduler/dcp-scheduler-core/target/dcp-scheduler-core-0.1.0.jar dcp-worker/dist/
cp dcp-scheduler/dcp-scheduler-core/pom.xml dcp-worker/dist/dcp-scheduler-core-pom.xml
cp backend/dist/rclone dcp-worker/dist/rclone

echo "[4/4] 预置 backend/dist(仅 scheduler-api;tag-kie 与 rclone 保持既有预置)"
mkdir -p backend/dist
cp dcp-scheduler/dcp-scheduler-api/target/dcp-scheduler-api-0.1.0.jar backend/dist/
cp dcp-scheduler/dcp-scheduler-api/pom.xml backend/dist/dcp-scheduler-api-pom.xml

echo "完成。构建镜像: docker compose build outlet-service dcp-worker backend"
