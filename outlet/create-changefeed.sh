#!/usr/bin/env bash
# ============================================================================
# 创建 L1 内部流 changefeed(chg-target-db)  —— B1
# 设计依据:《数据采集平台V3-变更流出口设计-2026-09-14》§5.2
#
# ★ 关键:必须显式指定 --start-ts。不指定 = 从"当前 TSO"开始,
#   重建/迁移时会静默跳过历史段(永久漏)。起点须与消费方最早需求对齐,
#   且所有消费方的快照边界 T0 必须 ≥ start_ts。
#
# 用法:
#   ./create-changefeed.sh                 # 用当前 TSO 作起点
#   ./create-changefeed.sh <TSO>           # 指定起点(与快照 boundary 对齐)
# ============================================================================
set -euo pipefail

TICDC_SERVER="${TICDC_SERVER:-http://localhost:18300}"
CHANGEFEED_ID="${CHANGEFEED_ID:-chg-target-db}"
KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP:-kafka1:9092,kafka2:9092,kafka3:9092}"
TOPIC="${TOPIC:-dcp.cdc.target_db}"
PARTITIONS="${PARTITIONS:-3}"
RETENTION_HINT_DAYS="${RETENTION_HINT_DAYS:-7}"

# 起点:默认取当前 TSO(优先"建流后再接入消费方",保证 T0 ≥ start_ts)
START_TS="${1:-}"

SINK_URI="kafka://${KAFKA_BOOTSTRAP}/${TOPIC}?protocol=canal-json&partition-num=${PARTITIONS}&replication-factor=3&required-acks=-1&enable-tidb-extension=true&max-message-bytes=10485760"

CMD=(cdc cli changefeed create
  --server="${TICDC_SERVER}"
  --changefeed-id="${CHANGEFEED_ID}"
  --sink-uri="${SINK_URI}"
  --config="$(dirname "$0")/changefeed-chg-target-db.toml")

if [[ -n "${START_TS}" ]]; then
  CMD+=(--start-ts="${START_TS}")
fi

echo "== 即将创建 changefeed =="
printf ' %q' "${CMD[@]}"; echo
echo "  · retention 提示:${RETENTION_HINT_DAYS} 天(决策 12;Kafka 侧同值)"

if [[ "${DRY_RUN:-0}" == "1" ]]; then
  echo "(DRY_RUN=1,未执行)"; exit 0
fi

# cdc 二进制:若本机未安装,用容器内 CLI 执行
if command -v cdc >/dev/null 2>&1; then
  "${CMD[@]}"
else
  echo "本机无 cdc CLI → 走容器:"
  docker exec dcp-outlet-ticdc1 /cdc "${CMD[@]:1}" \
    | sed "s#http://localhost:18300#http://ticdc1:8300#"
fi
