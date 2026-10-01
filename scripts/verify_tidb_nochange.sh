#!/usr/bin/env bash
# ============================================================================
# TiDB no-change 实测脚本(设计方案 §4.2 唯一待实测关键点)
#
# 目的: 验证 TiDB 对「ON DUPLICATE KEY UPDATE 赋回原值」是否零 TiKV 写 / 零 TiCDC 事件。
#   若零事件 → 流路径模板(4.2)可直接落地, 重放被 IF(号条件) 确定性吸收;
#   若产生事件 → 流路径模板需改为「条件 UPDATE 前置判断」(参照批路径 4.3), 并通知设计修订。
#
# 原理: TiCDC 只输出 TiKV 层真正产生变更的行。同值赋回若被 TiKV 优化为 no-op,
#   changefeed 下游 Kafka 将不新增消息。本脚本以 Kafka end-offset 差为观测口径:
#     新事件数 = 采样点间各分区 end-offset 之和的增量
#
# 环境: docker-compose.outlet.yml(TiDB×2 14000·14010 / TiCDC×2 18300·18301 / Kafka×3 19092)
#
# 用法:
#   scripts/verify_tidb_nochange.sh              # 检查环境后执行(容器未起则提示并退出)
#   scripts/verify_tidb_nochange.sh --up         # 自动拉起 outlet 集群并等待健康
#   scripts/verify_tidb_nochange.sh --reset      # 重建 changefeed 与数据(全量重测)
# ============================================================================
set -u

TIDB_C=dpc-outlet-tidb1
TICDC_C=dcp-outlet-ticdc1
KAFKA_C=dcp-outlet-kafka1
DB=t_nochange_verify
TOPIC=t_nochange_verify
CF_ID=cf-nochange-verify
KAFKA_BIN=/opt/kafka/bin
WAIT_CDC=6          # 每步后等待 CDC 传播的秒数
UP_MODE=0
RESET_MODE=0
for a in "$@"; do
  [ "$a" = "--up" ] && UP_MODE=1
  [ "$a" = "--reset" ] && RESET_MODE=1
done

log()  { printf '\033[1;34m[verify]\033[0m %s\n' "$*"; }
fail() { printf '\033[1;31m[verify][FAIL]\033[0m %s\n' "$*"; exit 1; }
pass() { printf '\033[1;32m[verify][PASS]\033[0m %s\n' "$*"; }

# ---------------------------------------------------------------- 0. 环境
[ "$(command -v docker)" ] || fail "docker 不可用"
COMPOSE_DIR="$(cd "$(dirname "$0")/.." && pwd)"

if [ $UP_MODE = 1 ]; then
  log "拉起 outlet 集群: docker compose -f docker-compose.outlet.yml up -d"
  docker compose -f "$COMPOSE_DIR/docker-compose.outlet.yml" up -d || fail "集群启动失败"
fi

if ! docker ps --format '{{.Names}}' | grep -qx "$TIDB_C"; then
  fail "容器 $TIDB_C 未运行; 请先执行: docker compose -f docker-compose.outlet.yml up -d(或本脚本加 --up)"
fi

log "等待 TiDB 就绪(最多 120s)..."
ready=0
for i in $(seq 1 60); do
  if docker exec "$TIDB_C" mysql -h127.0.0.1 -P4000 -uroot -e "SELECT 1" >/dev/null 2>&1; then
    ready=1; break
  fi
  sleep 2
done
[ $ready = 1 ] || fail "TiDB 60 次探测未就绪"
pass "TiDB 已就绪"

# mysql 执行入口(容器内自带 mysql client)
sql() { docker exec "$TIDB_C" mysql -h127.0.0.1 -P4000 -uroot --default-character-set=utf8mb4 -e "$1"; }

# Kafka 全分区 end-offset 之和(采样间差值 = 新事件数)
kafka_offsets() {
  docker exec "$KAFKA_C" "$KAFKA_BIN/kafka-get-offsets.sh" \
      --bootstrap-server kafka1:9092 --topic "$TOPIC" 2>/dev/null \
    | awk -F: '{s+=$3} END {print s+0}'
}

# changefeed 是否存在
cf_exists() {
  docker exec "$TICDC_C" /cdc cli changefeed list --server=http://127.0.0.1:8300 2>/dev/null \
    | grep -q "$CF_ID"
}

# ---------------------------------------------------------------- 1. 建库表(幂等)
log "建库建表($DB.t1)..."
sql "CREATE DATABASE IF NOT EXISTS $DB;"
sql "USE $DB; CREATE TABLE IF NOT EXISTS t1 (
       id BIGINT PRIMARY KEY,
       c1 VARCHAR(64),
       c2 VARCHAR(64),
       seq BIGINT NOT NULL DEFAULT 0
     );"
pass "表 $DB.t1 就绪"

# ---------------------------------------------------------------- 2. changefeed
if [ $RESET_MODE = 1 ] && cf_exists; then
  log "重置 changefeed: remove $CF_ID"
  docker exec "$TICDC_C" /cdc cli changefeed remove \
      --server=http://127.0.0.1:8300 --changefeed-id="$CF_ID" >/dev/null 2>&1 || true
  sleep 3
fi

if ! cf_exists; then
  log "创建 changefeed $CF_ID → kafka1:9092/$TOPIC"
  FILTER=/tmp/ticdc-filter-nochange.toml
  cat > /tmp/ticdc-filter-nochange.toml <<'EOF'
[filter]
rules = ['t_nochange_verify.*']
EOF
  docker cp "$FILTER" "$TICDC_C:$FILTER" || fail "filter 配置写入容器失败"
  docker exec "$TICDC_C" /cdc cli changefeed create \
      --server=http://127.0.0.1:8300 \
      --sink-uri="kafka://kafka1:9092/$TOPIC" \
      --config="$FILTER" \
      --changefeed-id="$CF_ID" >/dev/null 2>&1 || fail "changefeed 创建失败"
  log "等待 changefeed 初始化(15s)..."
  sleep 15
fi
pass "changefeed $CF_ID 就绪"

# ---------------------------------------------------------------- 3. 测试矩阵
# 流路径模板(设计方案 §4.2)的 SQL 形态: 同号/低号 → IF 赋回原值
# 批路径模板(设计方案 §4.3)的 SQL 形态: 条件 UPDATE, 号条件+值条件全 false → 0 行
prev=0
all_pass=0

# 每步: 执行 SQL → 等待 CDC 传播 → 采样 Kafka end-offset → 增量 vs 期望
assert_step() {  # $1=步骤名 $2=期望事件数 $3=SQL
  local name="$1" expect="$2"
  sql "$3" >/dev/null 2>&1 || fail "步骤 $name 执行失败: $3"
  sleep "$WAIT_CDC"
  local now; now=$(kafka_offsets)
  local delta=$(( now - prev ))
  prev=$now
  if [ "$delta" -eq "$expect" ]; then
    pass "$name: 事件数 $delta == 期望 $expect"
  else
    printf '\033[1;31m[verify][FAIL]\033[0m %s: 事件数 %s != 期望 %s\n' "$name" "$delta" "$expect"
    all_pass=1
  fi
}

# 数据复位(DELETE 产生的事件在等待期内消化, 不计入判定; prev 采样点设在复位后)
log "复位数据: DELETE FROM t1(采样点归零)..."
sql "USE $DB; DELETE FROM t1;"
sleep "$WAIT_CDC"
prev=$(kafka_offsets)
[ -n "$prev" ] || fail "无法采样 Kafka offset(topic $TOPIC 无数据)"

# --- T0 基线 INSERT(阳性对照: 必产生 1 条事件)
assert_step "T0 基线 INSERT(首次写)" 1 \
  "USE $DB; INSERT INTO t1 VALUES (1, 'a', 'b', 100);"

# --- T1 同号同值重放(核心: 流模板 SQL, 号条件 false → 全行赋回原值;
#        验证点 = TiDB 对「ON DUP 赋回原值」是否零 TiKV 写/零 TiCDC 事件)
assert_step "T1 同号同值重放(流模板)" 0 \
  "USE $DB; INSERT INTO t1 (id, c1, c2, seq) VALUES (1, 'a', 'b', 100)
     ON DUPLICATE KEY UPDATE
       c1  = IF(VALUES(seq) > seq, VALUES(c1),  c1),
       c2  = IF(VALUES(seq) > seq, VALUES(c2),  c2),
       seq = IF(VALUES(seq) > seq, VALUES(seq), seq);"

# --- T2 同号值变(号条件 false, 值变被挡 → 零事件)
assert_step "T2 同号值变被挡(流模板)" 0 \
  "USE $DB; INSERT INTO t1 (id, c1, c2, seq) VALUES (1, 'a2', 'b', 100)
     ON DUPLICATE KEY UPDATE
       c1  = IF(VALUES(seq) > seq, VALUES(c1),  c1),
       c2  = IF(VALUES(seq) > seq, VALUES(c2),  c2),
       seq = IF(VALUES(seq) > seq, VALUES(seq), seq);"

# --- T3 高号值变(阳性对照: 必产生 1 条事件)
assert_step "T3 高号值变(流模板, 阳性对照)" 1 \
  "USE $DB; INSERT INTO t1 (id, c1, c2, seq) VALUES (1, 'a2', 'b', 200)
     ON DUPLICATE KEY UPDATE
       c1  = IF(VALUES(seq) > seq, VALUES(c1),  c1),
       c2  = IF(VALUES(seq) > seq, VALUES(c2),  c2),
       seq = IF(VALUES(seq) > seq, VALUES(seq), seq);"

# --- T4 批模板同号重跑(号条件 false → 0 行, B 场景: 批重跑同号不写)
assert_step "T4 批模板同号重跑(号条件 false)" 0 \
  "USE $DB; UPDATE t1 SET c1 = 'a', seq = 200 WHERE id = 1 AND 200 > seq;"

# --- T5 批模板号进值同(号条件 true 但值条件 false → 0 行, E 场景: 全量重发值同零事件)
assert_step "T5 批模板号进值同(值条件 false)" 0 \
  "USE $DB; UPDATE t1 SET c1 = 'a2', seq = 400
     WHERE id = 1 AND 400 > seq AND NOT (c1 <=> 'a2');"

# ---------------------------------------------------------------- 5. 结论
log "========== 判定结论 =========="
if [ $all_pass = 0 ]; then
  cat <<'EOF'
[结论 A] 流路径模板(4.2)可直接落地:
  - T1 同号同值重放 → 零事件: TiDB 对「ON DUP 赋回原值」零 TiKV 写/零 TiCDC 事件,
    重放被 IF(号条件) 确定性吸收, 不产生内容重演;
  - T2 同号值变 → 零事件: 号条件挡倒退, 且不依赖引擎隐式行为。
[结论 B] 批路径模板(4.3)已确定性零写:
  - T4 同号重跑(号条件 false)→ 0 行零事件(B 场景);
  - T5 号进值同(值条件 false)→ 0 行零事件(E 场景: 全量重发同值不写)。
EOF
else
  cat <<'EOF'
[结论 C] 存在 no-change 被写成事件, 按失败步骤对症处理:
  1) T1/T2 失败 → TiDB 未优化「ON DUP 同值赋回」(产生 TiKV 写 → TiCDC 事件),
     流路径模板必须改为「条件 UPDATE 前置判断」(批路径 4.3 形态), 同步修订设计文档 §4.2;
  2) T0/T3 失败 → 阳性对照异常, 检查 changefeed 过滤与 Kafka 连通性;
  3) T4/T5 失败 → 批模板条件拼接有误, 检查 SQL 与数据状态(用 --reset 重测)。
EOF
fi
