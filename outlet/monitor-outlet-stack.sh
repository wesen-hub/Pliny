#!/usr/bin/env bash
# ============================================================================
# B4 · 监控打底(Prometheus 文本格式指标 + 告警判定)
#
# 指标:
#   chg_changefeed_up{id}                  state=normal → 1,否则 0
#   chg_changefeed_checkpoint_lag_seconds  checkpoint 落后当前 TSO 的秒数
#   chg_gc_remaining_seconds               GC 安全点剩余窗口 = life_time - (now - safe_point)
#   chg_kafka_isr_min / chg_kafka_replication_factor / chg_kafka_broker_up
#
# 告警阈值(环境变量可覆盖):LAG_WARN=300s / LAG_CRIT=3600s / GC_WARN=21600s(6h) / ISR < RF
# 退出码:0 = 全健康;1 = 有告警(便于自动化判火)
# 用法:./monitor-outlet-stack.sh [--json]
# ============================================================================
set -uo pipefail

TICDC_SERVER="${TICDC_SERVER:-http://ticdc1:8300}"
CHANGEFEED_ID="${CHANGEFEED_ID:-chg-target-db}"
TIDB_HOST="${TIDB_HOST:-dcp-outlet-tidb1}"
TIDB_PORT="${TIDB_PORT:-4000}"
TOPIC="${TOPIC:-dcp.cdc.target_db}"
LAG_WARN="${LAG_WARN:-300}"
LAG_CRIT="${LAG_CRIT:-3600}"
GC_WARN="${GC_WARN:-21600}"

q() { docker exec dcp-mysql mysql -h "${TIDB_HOST}" -P "${TIDB_PORT}" -uroot -N -e "$1" 2>/dev/null; }

latest_ts=$(q "SHOW MASTER STATUS;" | awk '{print $2}')
gc_safe_point=$(q "SELECT variable_value FROM mysql.tidb WHERE variable_name='tikv_gc_safe_point';")
gc_life_time=$(q "SELECT @@tidb_gc_life_time;")
cf_json=$(docker exec dcp-outlet-ticdc1 /cdc cli changefeed query --server="${TICDC_SERVER}" --changefeed-id="${CHANGEFEED_ID}" 2>/dev/null)

read -r state checkpoint_ts <<<"$(printf '%s' "${cf_json}" | python3 -c "
import json,sys
try:
    d=json.load(sys.stdin); print(d.get('state','unknown'), d.get('checkpoint_tso', d.get('checkpoint_ts',0)))
except Exception: print('unavailable',0)")"

lag_s=$(python3 -c "print(round((int('${latest_ts}' or 0) - int('${checkpoint_ts}' or 0))/1000/262144,1))" 2>/dev/null || echo 0)

gc_remaining=$(python3 - "$gc_safe_point" "$gc_life_time" <<'PY'
import sys, re, datetime
sp, life = sys.argv[1], sys.argv[2]
lt = 0
m = re.match(r'(?:(\d+)h)?(?:(\d+)m)?(?:(\d+)s)?', life.strip())
if m: lt = int(m.group(1) or 0)*3600 + int(m.group(2) or 0)*60 + int(m.group(3) or 0)
s = sp.strip()
m2 = re.match(r'(\d{4})(\d{2})(\d{2})-(\d{2}):(\d{2}):(\d{2})', s)
if m2:
    dt = datetime.datetime(*map(int, m2.groups()), tzinfo=datetime.timezone.utc).replace(tzinfo=None)
    print(round(lt - (datetime.datetime.utcnow() - dt).total_seconds()))
else:
    print(-1)
PY
)

desc=$(docker exec dcp-outlet-kafka1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka1:9092 --describe --topic "${TOPIC}" 2>/dev/null)
rf=$(echo "${desc}" | sed -n '1s/.*ReplicationFactor: \([0-9]*\).*/\1/p')
isr_min=$(echo "${desc}" | grep -o 'Isr: [0-9,]*' | awk '{n=split($2,a,","); print n}' | sort -n | head -1)
broker_up=0
for b in kafka1 kafka2 kafka3; do
  docker exec "dcp-outlet-${b}" /opt/kafka/bin/kafka-topics.sh --bootstrap-server "${b}:9092" --list >/dev/null 2>&1 && broker_up=$((broker_up+1))
done

alerts=()
[[ "${state}" == "normal" ]] || alerts+=("CRITICAL:changefeed=${state}")
if python3 -c "import sys;sys.exit(0 if float('${lag_s}') > ${LAG_CRIT} else 1)"; then
  alerts+=("CRITICAL:lag=${lag_s}s>${LAG_CRIT}s")
elif python3 -c "import sys;sys.exit(0 if float('${lag_s}') > ${LAG_WARN} else 1)"; then
  alerts+=("WARN:lag=${lag_s}s>${LAG_WARN}s")
fi
if [[ "${gc_remaining}" != "-1" ]]; then
  python3 -c "import sys;sys.exit(0 if int('${gc_remaining}') < ${GC_WARN} else 1)" && alerts+=("WARN:gcRemaining=${gc_remaining}s<${GC_WARN}s")
fi
if [[ -n "${rf:-}" && -n "${isr_min:-}" ]]; then
  python3 -c "import sys;sys.exit(0 if int('${isr_min}') < int('${rf}') else 1)" && alerts+=("CRITICAL:isr=${isr_min}<rf=${rf}")
fi
[[ "${broker_up}" == "3" ]] || alerts+=("WARN:brokersUp=${broker_up}/3")

if [[ "${1:-}" == "--json" ]]; then
  python3 - "$state" "$lag_s" "$gc_remaining" "$isr_min" "$rf" "$broker_up" "${alerts[*]-}" <<'PY'
import sys, json
st, lag, gc, isr, rf, bu, alerts = sys.argv[1:8]
print(json.dumps({"state": st, "checkpointLagSeconds": float(lag), "gcRemainingSeconds": int(gc),
                  "isrMin": int(isr), "rf": int(rf), "brokersUp": int(bu),
                  "alerts": alerts.split(), "healthy": 0 if alerts.strip() else 1}, ensure_ascii=False))
PY
else
  cat <<EOF
# HELP chg_changefeed_up changefeed state normal=1
# TYPE chg_changefeed_up gauge
chg_changefeed_up{id="${CHANGEFEED_ID}"} $( [[ "${state}" == "normal" ]] && echo 1 || echo 0 )
# HELP chg_changefeed_checkpoint_lag_seconds checkpoint lag vs current TSO
chg_changefeed_checkpoint_lag_seconds{id="${CHANGEFEED_ID}"} ${lag_s}
# HELP chg_gc_remaining_seconds GC safe point remaining window
chg_gc_remaining_seconds ${gc_remaining}
# HELP chg_kafka_isr_min minimum ISR across partitions
chg_kafka_isr_min{topic="${TOPIC}"} ${isr_min}
# HELP chg_kafka_replication_factor topic replication factor
chg_kafka_replication_factor{topic="${TOPIC}"} ${rf}
# HELP chg_kafka_broker_up live brokers
chg_kafka_broker_up ${broker_up}
EOF
  if [[ ${#alerts[@]} -eq 0 ]]; then echo "# ALERTS: none"; else printf '# ALERT: %s\n' "${alerts[@]}"; fi
fi

[[ ${#alerts[@]} -eq 0 ]] && exit 0 || exit 1
