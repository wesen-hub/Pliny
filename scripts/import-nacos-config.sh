#!/bin/bash
# ============================================================
# DCP 平台 — Nacos 配置发布脚本(M1: 配置中心外迁)
# 将 deploy/nacos-config/*.yml 以「文件名 = dataId」幂等发布到
# Nacos namespace=dcp(group=DEFAULT_GROUP, type=yaml)。
# 重复执行覆盖同名 dataId(发布即热更, 服务端 refresh 生效)。
#
# 用法: NACOS_ADDR=127.0.0.1:8848 bash scripts/import-nacos-config.sh
#       可选 NACOS_NAMESPACE 覆盖 namespace(默认 dcp, 与各服务 yml 一致)
#
# 前置: Nacos 已启动(compose: nacos 容器, 8848); 当前 compose 关闭鉴权
#       (NACOS_AUTH_ENABLE=false), 若开启鉴权需先 login 取 accessToken 追加
#       --data-urlencode "accessToken=xxx"。
# ============================================================
set -e
NACOS_ADDR=${NACOS_ADDR:-127.0.0.1:8848}
NS_ID=${NACOS_NAMESPACE:-dcp}
CONFIG_DIR="$(cd "$(dirname "$0")/../deploy/nacos-config" && pwd)"
BASE="http://${NACOS_ADDR}/nacos"

GREEN='\033[0;32m'; YELLOW='\033[1;33m'; CYAN='\033[0;36m'; NC='\033[0m'
info()  { echo -e "${CYAN}[nacos-config]${NC} $1"; }
ok()    { echo -e "${GREEN}  ✔ $1${NC}"; }
warn()  { echo -e "${YELLOW}  ⚠ $1${NC}"; }

# ---------- 1. 确保 namespace 存在(幂等) ----------
info "检查 Nacos ${NACOS_ADDR} namespace=${NS_ID}"
NS_LIST=$(curl -sf "${BASE}/v2/console/namespace/list" || true)
if echo "$NS_LIST" | grep -q "\"namespace\":\"${NS_ID}\""; then
  ok "namespace ${NS_ID} 已存在"
else
  info "创建 namespace=${NS_ID}"
  curl -sf -X POST "${BASE}/v2/console/namespace" \
    --data-urlencode "namespaceId=${NS_ID}" \
    --data-urlencode "namespaceName=${NS_ID}" \
    --data-urlencode "namespaceDesc=DCP platform" > /dev/null
  ok "namespace ${NS_ID} 已创建"
fi

# ---------- 2. 发布 deploy/nacos-config/*.yml ----------
CNT=0
for f in "$CONFIG_DIR"/*.yml; do
  [ -e "$f" ] || continue
  data_id=$(basename "$f")
  content=$(cat "$f")
  info "发布 ${data_id} → namespace=${NS_ID} group=DEFAULT_GROUP"
  curl -sf -X POST "${BASE}/v2/cs/config" \
    --data-urlencode "dataId=${data_id}" \
    --data-urlencode "group=DEFAULT_GROUP" \
    --data-urlencode "namespaceId=${NS_ID}" \
    --data-urlencode "type=yaml" \
    --data-urlencode "content=${content}" > /dev/null
  ok "${data_id} 已发布"
  CNT=$((CNT+1))
done
ok "全部完成: 共发布 ${CNT} 个配置(幂等, 可重复执行)"
