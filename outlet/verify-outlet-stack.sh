#!/usr/bin/env bash
# ============================================================================
# 新数据面栈验证脚本(对应《开工前阻塞项清单》A1~A4 判据)
# 用法:./verify-outlet-stack.sh
# ============================================================================
set -uo pipefail

PD_HOST="${PD_HOST:-http://localhost:12379}"
TIDB_HOST="${TIDB_HOST:-dcp-outlet-tidb1}"
TIDB_PORT="${TIDB_PORT:-4000}"
TIDB_PWD="${TIDB_PWD:-}"
KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP:-kafka1:9092}"
MYSQL_CLI="${MYSQL_CLI:-docker exec dcp-mysql mysql}"

pass=0; fail=0
ok()   { echo "  ✅ $1"; pass=$((pass+1)); }
bad()  { echo "  ❌ $1"; fail=$((fail+1)); }

echo "=== A1 · PD 集群 ==="
members=$(curl -s --max-time 5 "${PD_HOST}/pd/api/v1/members" | python3 -c "import sys,json;print(len(json.load(sys.stdin).get('members',[])))")
[[ "${members}" == "3" ]] && ok "PD 成员数 = 3" || bad "PD 成员数 = ${members}(期望 3)"
health=$(curl -s --max-time 5 "${PD_HOST}/pd/api/v1/health" | grep -c '"health": true')
[[ "${health}" -ge 3 ]] && ok "PD healthy 成员 = ${health}" || bad "PD healthy 成员 = ${health}(期望 ≥3)"
stores=$(curl -s --max-time 5 "${PD_HOST}/pd/api/v1/stores" | python3 -c "import sys,json;print(sum(1 for s in json.load(sys.stdin).get('stores',[]) if s['store']['state_name']=='Up'))")
[[ "${stores}" == "3" ]] && ok "TiKV store Up 数 = 3" || bad "TiKV store Up 数 = ${stores}(期望 3)"

echo "=== A1 · TiDB 可连 ==="
out=$(${MYSQL_CLI} -h "${TIDB_HOST}" -P "${TIDB_PORT}" -uroot ${TIDB_PWD:+-p"${TIDB_PWD}"} -N -e \
  "SELECT VERSION(); SELECT CONCAT('tikv=',COUNT(*)) FROM information_schema.cluster_info WHERE TYPE='tikv'; SELECT CONCAT('tidb=',COUNT(*)) FROM information_schema.cluster_info WHERE TYPE='tidb';" 2>/dev/null | grep -v Warning)
echo "${out}" | grep -q 'TiDB-v7.5.0' && ok "TiDB 版本: $(echo "${out}" | head -1)" || bad "TiDB 版本异常: ${out}"
echo "${out}" | grep -q 'tikv=3' && ok "cluster_info: 3×TiKV" || bad "cluster_info: $(echo "${out}" | grep tikv=)"
echo "${out}" | grep -q 'tidb=2' && ok "cluster_info: 2×TiDB" || bad "cluster_info: $(echo "${out}" | grep tidb=)"

echo "=== A2 · TiCDC ==="
captures=$(docker exec dcp-outlet-ticdc1 /cdc cli capture list --server=http://ticdc1:8300 2>/dev/null | grep -c '"id"')
[[ "${captures}" -ge 2 ]] && ok "capture 数 = ${captures}" || bad "capture 数 = ${captures}(期望 ≥2)"

echo "=== A3 · Kafka(topic 参数待建 topic 后复查) ==="
docker exec dcp-outlet-kafka1 sh -c "/opt/kafka/bin/kafka-topics.sh --bootstrap-server ${KAFKA_BOOTSTRAP} --list" >/dev/null 2>&1 \
  && ok "Kafka 可连" || bad "Kafka 不可连"

echo "=== A4 · 窗口参数 ==="
gc=$(${MYSQL_CLI} -h "${TIDB_HOST}" -P "${TIDB_PORT}" -uroot ${TIDB_PWD:+-p"${TIDB_PWD}"} -N -e "SELECT @@tidb_gc_life_time;" 2>/dev/null | grep -v Warning)
[[ "${gc}" == "168h0m0s" ]] && ok "tidb_gc_life_time = ${gc}" || bad "tidb_gc_life_time = ${gc}(期望 168h)"

echo
echo "---- 结果:✅ ${pass} 项通过 / ❌ ${fail} 项失败 ----"
exit $(( fail > 0 ? 1 : 0 ))
