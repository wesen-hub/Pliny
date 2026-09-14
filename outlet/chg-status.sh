#!/usr/bin/env bash
# ============================================================================
# B3 · 起点与快照边界规则(出口接口 ① stream-status 原型 + 边界校验)
#
# 规则(设计 §6.5 / §6.3):
#   R1  T0 ≥ changefeed.start_ts         否则 → BOUNDARY_BELOW_STREAM_START
#   R2  parse(T0) ≥ GC safe_point        否则 → BOUNDARY_EXPIRED
#   R3  T0 ≤ latest(当前 TSO)           否则 → BOUNDARY_IN_FUTURE
# 不满足任一条 → 平台**明确报错**,绝不静默从当前水位开始。
#
# 用法:
#   ./chg-status.sh                    # 打印 stream-status 原型
#   ./chg-status.sh --check-t0 <TSO>   # 校验某个快照边界 T0
# ============================================================================
set -uo pipefail

TICDC_SERVER="${TICDC_SERVER:-http://ticdc1:8300}"
CHANGEFEED_ID="${CHANGEFEED_ID:-chg-target-db}"
TIDB_HOST="${TIDB_HOST:-dcp-outlet-tidb1}"
TIDB_PORT="${TIDB_PORT:-4000}"
RETENTION_HOURS="${RETENTION_HOURS:-168}"

q() { docker exec dcp-mysql mysql -h "${TIDB_HOST}" -P "${TIDB_PORT}" -uroot -N -e "$1" 2>/dev/null; }
tso_dt() { python3 -c "import datetime;print(datetime.datetime.fromtimestamp((int('$1')>>18)/1000).strftime('%Y-%m-%d %H:%M:%S'))"; }
dt_ms()  { python3 -c "
import sys,re,datetime
s=sys.argv[1].strip()
m=re.match(r'(\d{4})(\d{2})(\d{2})-(\d{2}):(\d{2}):(\d{2})', s)
if m:
    dt=datetime.datetime(*map(int,m.groups()), tzinfo=datetime.timezone.utc); print(int(dt.timestamp()*1000))
else:
    m=re.match(r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})', s)
    dt=datetime.datetime.strptime(m.group(1),'%Y-%m-%d %H:%M:%S') if m else datetime.datetime.now()
    print(int(dt.timestamp()*1000))
" "$1"; }

latest_ts=$(q "SHOW MASTER STATUS;" | awk '{print $2}')
gc_safe_point=$(q "SELECT variable_value FROM mysql.tidb WHERE variable_name='tikv_gc_safe_point';")
gc_life_time=$(q "SELECT @@tidb_gc_life_time;")

cf_json=$(docker exec dcp-outlet-ticdc1 /cdc cli changefeed query --server="${TICDC_SERVER}" --changefeed-id="${CHANGEFEED_ID}" 2>/dev/null)
read -r start_ts state checkpoint_ts <<<"$(printf '%s' "${cf_json}" | python3 -c "
import json,sys
try:
    d=json.load(sys.stdin); print(d.get('start_ts',0), d.get('state','unknown'), d.get('checkpoint_tso', d.get('checkpoint_ts',0)))
except Exception: print(0,'unavailable',0)")"

lag_s=$(python3 -c "print(round((int('${latest_ts}' or 0) - int('${checkpoint_ts}' or 0))/1000/262144,1))" 2>/dev/null || echo 0)

if [[ "${1:-}" == "--check-t0" ]]; then
  T0="${2:-}"
  [[ -z "${T0}" ]] && { echo "用法: $0 --check-t0 <TSO>"; exit 2; }
  t0ms=$(python3 -c "print(int('${T0}') >> 18)")
  spms=$(dt_ms "${gc_safe_point}")
  code=OK
  if   [[ "${T0}" -lt "${start_ts}"     ]]; then code=BOUNDARY_BELOW_STREAM_START
  elif [[ "${t0ms}" -lt "${spms}"       ]]; then code=BOUNDARY_EXPIRED
  elif [[ "${T0}" -gt "${latest_ts}"    ]]; then code=BOUNDARY_IN_FUTURE
  fi
  printf 'T0            = %s (%s)\nstart_ts      = %s (%s)\nlatest        = %s (%s)\ngc_safe_point = %s\ngc_life_time  = %s\n=> %s\n' \
    "${T0}" "$(tso_dt "${T0}")" "${start_ts}" "$(tso_dt "${start_ts}")" \
    "${latest_ts}" "$(tso_dt "${latest_ts}")" "${gc_safe_point}" "${gc_life_time}" "${code}"
  [[ "${code}" == "OK" ]] && exit 0 || exit 1
fi

cat <<EOF
{
  "changefeedId": "${CHANGEFEED_ID}",
  "state": "${state}",
  "epoch": 1,
  "startTs": ${start_ts},
  "earliest": ${start_ts},
  "checkpointTs": ${checkpoint_ts},
  "latest": ${latest_ts},
  "latestTime": "$(tso_dt "${latest_ts}")",
  "checkpointLagSeconds": ${lag_s},
  "gcSafePoint": "${gc_safe_point}",
  "gcLifeTime": "${gc_life_time}",
  "retentionHours": ${RETENTION_HOURS}
}
EOF
