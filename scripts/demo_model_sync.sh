#!/bin/bash
# ============================================================
# 数据采集平台 V3 — 「模型创建 → 数据同步」全链路演示
# 链路: 源表 bootstrap → 建模(建 logical_table + topic/分区 + 预建物理表)
#       → 注册 SeaTunnel CDC(全量快照+binlog 增量) → Kafka → 攒批写分片物理表
#       → 权威副本追平/lag → 源库增量 INSERT 验证
# 用法: bash scripts/demo_model_sync.sh   (可重复执行: 自动清理 t_oms_* 演示残留)
# 只动 t_oms_order / t_oms_wide, 不影响其它运行中的任务
# ============================================================
set -e
BASE=http://localhost:8080
GREEN='\033[0;32m'; YELLOW='\033[1;33m'; CYAN='\033[0;36m'; NC='\033[0m'
info() { echo -e "${CYAN}[模型-同步演示]${NC} $1"; }
ok()   { echo -e "${GREEN}  ✔ $1${NC}"; }
warn() { echo -e "${YELLOW}  ⚠ $1${NC}"; }

# ---------- 1. 登录 ----------
info "1/9 登录系统"
python3 - << 'PYEOF' > /tmp/dcp_mtk
import urllib.request, json
req = urllib.request.Request("http://localhost:8080/api/auth/login",
    data=json.dumps({"username":"admin","password":"admin123"}).encode(),
    headers={"Content-Type":"application/json"})
print(json.load(urllib.request.urlopen(req))["data"]["token"])
PYEOF
IFS= read -r TOKEN < /tmp/dcp_mtk
AUTH="Authorization: Bearer $TOKEN"
ok "登录成功"

# ---------- 2. 演示源表 bootstrap(source_db.t_oms_order) ----------
info "2/9 重置演示源表 source_db.t_oms_order(幂等)"
docker exec dcp-demo-db mysql -uroot -proot123 source_db -e "
DROP TABLE IF EXISTS t_oms_order;
CREATE TABLE t_oms_order (
  id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  order_no VARCHAR(32) NOT NULL,
  customer VARCHAR(64) NOT NULL,
  product  VARCHAR(64),
  qty INT DEFAULT 1,
  amount DECIMAL(12,2),
  status VARCHAR(16) DEFAULT 'NEW',
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  KEY idx_customer (customer)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
INSERT INTO t_oms_order (id, order_no, customer, product, qty, amount, status) VALUES
 (1001,'OM20260901001','王五','华为Mate60', 1, 6999.00,'PAID'),
 (1002,'OM20260901002','赵六','小米14Pro', 2, 8998.00,'PAID'),
 (1003,'OM20260901003','钱七','iPad Air',  1, 4799.00,'SHIPPED'),
 (1004,'OM20260901004','孙八','ThinkPad X1',1,12999.00,'NEW');" 2>&1 | grep -v "Warning" || true
ok "源表已就绪(4 行种子: 王五/赵六/钱七/孙八)"

# ---------- 3. 清理旧演示残留(best-effort, 不动其它任务) ----------
info "3/9 清理 t_oms_wide 旧演示残留(若存在)"
OLD=$(curl -s "$BASE/api/models" -H "$AUTH" | python3 -c "
import sys,json
for m in json.load(sys.stdin)['data']:
    if m['table_name']=='t_oms_wide': print(m['id']); break")
if [ -n "$OLD" ]; then
  # 若任务在跑先停
  RUN=$(curl -s "$BASE/api/sync/cdc-st/status" -H "$AUTH" | python3 -c "
import sys,json
for t in json.load(sys.stdin)['data']:
    if t['logicalTable']=='t_oms_wide' and t.get('running'): print('yes'); break" 2>/dev/null)
  [ "$RUN" = "yes" ] && { curl -s -X POST "$BASE/api/sync/cdc-st/stop/t_oms_wide" -H "$AUTH" > /dev/null; sleep 3; }
  curl -s -X DELETE "$BASE/api/models/$OLD" -H "$AUTH" > /dev/null
  docker exec dcp-mysql mysql -uroot -proot123 dcp -e "DELETE FROM cdc_st_task WHERE logical_table='t_oms_wide';" 2>/dev/null || true
  for t in $(docker exec dcp-demo-db mysql -uroot -proot123 -N -e "USE target_db; SHOW TABLES LIKE 't_oms_wide%';" 2>/dev/null); do
    # TRUNCATE 而非 DROP: backend 内 AtomicUpsertDao 的 ensured/knownCols 列缓存不随外部 DROP 失效,
    # DROP 后重建骨架表会导致 INSERT 报 bad SQL grammar(缓存认为列还在) → 保留表结构只清数据
    docker exec dcp-demo-db mysql -uroot -proot123 -e "TRUNCATE TABLE target_db.\`$t\`;" 2>/dev/null || true
  done
  ok "已清理旧演示"
else
  ok "无旧演示, 跳过"
fi

# ---------- 4. 数据源(复用演示源库-MySQL) ----------
DSID=$(curl -s "$BASE/api/datasources?pageSize=200" -H "$AUTH" | python3 -c "
import sys,json
for x in json.load(sys.stdin)['data']['list']:
    if x['type']=='mysql' and x['host']=='demo-db' and x.get('databaseName')=='source_db': print(x['id']); break")
ok "复用演示源库 datasourceId=$DSID"

# ---------- 5. 创建模型(建模即发布) ----------
info "4/9 创建模型「订单模型演示」→ 逻辑表 t_oms_wide"
RESP=$(curl -s -X POST "$BASE/api/models" -H "$AUTH" -H 'Content-Type: application/json' -d '{
  "name":"订单模型演示","layer":"DWD","domain":"交易","tableName":"t_oms_wide",
  "partitionCount":4,"tableCount":8,"keyColumns":["id"],
  "fields":[
    {"name":"id","type":"BIGINT","comment":"订单ID(业务键)"},
    {"name":"order_no","type":"VARCHAR(32)","comment":"订单号"},
    {"name":"customer","type":"VARCHAR(64)","comment":"客户"},
    {"name":"product","type":"VARCHAR(64)","comment":"商品"},
    {"name":"qty","type":"INT","comment":"数量"},
    {"name":"amount","type":"DECIMAL(12,2)","comment":"金额"},
    {"name":"status","type":"VARCHAR(16)","comment":"状态"},
    {"name":"created_at","type":"DATETIME","comment":"下单时间"}],
  "description":"模型创建→数据同步 全链路演示(源 source_db.t_oms_order)"}')
echo "$RESP" | python3 -c "import sys,json;d=json.load(sys.stdin);assert d['code']==0,d;print('  模型已发布 id='+str(d['data']['id'])+' tableName='+d['data']['tableName'])"
ok "模型发布成功"

# ---------- 6. 验证发布产物 ----------
info "5/9 验证发布产物(topic/分区 + 预建物理表)"
CFG=$(curl -s "$BASE/api/sync/logical-tables/t_oms_wide" -H "$AUTH")
echo "$CFG" | python3 -c "
import sys,json
d=json.load(sys.stdin)['data']
print(f\"  logical_table: topic={d['topic']} partition={d['partitionCount']} 物理表数={d['tableCount']} 键={d['keyColumns']}\")"
PTOPIC=$(curl -s "$BASE/api/sync/logical-tables/t_oms_wide" -H "$AUTH" | python3 -c "import sys,json;print(json.load(sys.stdin)['data']['topic'])")
KPART=$(docker exec dcp-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic "$PTOPIC" 2>/dev/null | grep -m1 -oE 'PartitionCount: [0-9]+' | awk '{print $2}')
echo "  Kafka topic $PTOPIC 分区数: $KPART"
SK=$(docker exec dcp-demo-db mysql -uroot -proot123 -N -e "USE target_db; SHOW TABLES LIKE 't_oms_wide%';" 2>/dev/null | grep -c . || true)
ok "预建物理表 $SK 张(t_oms_wide_00..07)"

# ---------- 7. 注册 SeaTunnel CDC 采集任务 ----------
info "6/9 注册 CDC 采集任务(source_db.t_oms_order → $PTOPIC)"
curl -s -X POST "$BASE/api/sync/cdc-st/task" -H "$AUTH" -H 'Content-Type: application/json' \
  -d "{\"logicalTable\":\"t_oms_wide\",\"datasourceId\":$DSID,\"sourceDb\":\"source_db\",\"sourceTable\":\"t_oms_order\",\"businessKeyFields\":[\"id\"],\"keyColumns\":[\"id\"],\"topic\":\"$PTOPIC\"}" \
  | python3 -c "import sys,json;d=json.load(sys.stdin);assert d['code']==0,d;print('  注册成功')"
ok "采集任务已注册"

# ---------- 8. 启动 SeaTunnel 作业 ----------
info "7/9 启动 SeaTunnel 作业(全量快照 + binlog 增量)"
RESP=$(curl -s -X POST "$BASE/api/sync/cdc-st/start/t_oms_wide" -H "$AUTH")
echo "$RESP" | python3 -c "import sys,json;d=json.load(sys.stdin);assert d['code']==0,d;print('  作业已提交 running='+str(d['data'].get('running')))"
# jobId 在 status 接口(启动响应不带)
for i in $(seq 1 10); do
  JOBID=$(curl -s "$BASE/api/sync/cdc-st/status" -H "$AUTH" | python3 -c "
import sys,json
for t in json.load(sys.stdin)['data']:
    if t['logicalTable']=='t_oms_wide': print(t.get('seaTunnelJobId') or ''); break" 2>/dev/null)
  [ -n "$JOBID" ] && break
  sleep 2
done
for i in $(seq 1 25); do
  ST=$(curl -s "$BASE/api/sync/cdc-st/status" -H "$AUTH" | python3 -c "
import sys,json
for t in json.load(sys.stdin)['data']:
    if t['logicalTable']=='t_oms_wide': print('RUNNING' if t.get('running') else 'STARTING'); break")
  [ "$ST" = "RUNNING" ] && break
  sleep 3
done
[ "$ST" = "RUNNING" ] && ok "作业 RUNNING(jobId=$JOBID)" || warn "作业状态 ${ST:-未知}(继续等待落库)"

# ---------- 9. 全量落库验证 ----------
info "8/9 等待全量快照落分片物理表(约 15-25 秒)"
ROWS=0
for i in $(seq 1 12); do
  sleep 5
  ROWS=$(for t in $(docker exec dcp-demo-db mysql -uroot -proot123 -N -e "USE target_db; SHOW TABLES LIKE 't_oms_wide_%';" 2>/dev/null); do docker exec dcp-demo-db mysql -uroot -proot123 -N -e "USE target_db; SELECT COUNT(*) FROM \`$t\`;" 2>/dev/null; done | awk '{s+=$1} END {print s+0}')
  [ "$ROWS" -ge 4 ] && break
done
echo "  分片物理表总行数: $ROWS"
echo "  --- 分片数据示例 ---"
for t in $(docker exec dcp-demo-db mysql -uroot -proot123 -N -e "USE target_db; SHOW TABLES LIKE 't_oms_wide_%';" 2>/dev/null); do
  docker exec dcp-demo-db mysql -uroot -proot123 -N -e "USE target_db; SELECT CONCAT('$t'), id, order_no, customer, product, amount, seq FROM \`$t\` ORDER BY id;" 2>/dev/null | while IFS=$'\t' read -r tb id no cust prod amt seq; do [ -n "$id" ] && echo "    $tb: id=$id $cust $prod ¥$amt seq=$seq"; done
done
[ "$ROWS" -ge 4 ] && ok "全量快照 4 行全部落库" || warn "落库行数 $ROWS < 4, 检查上方状态"

# ---------- 10. 增量验证 ----------
info "9/9 源库插入新订单(id=1005)验证增量"
docker exec dcp-demo-db mysql -uroot -proot123 source_db -e "INSERT INTO t_oms_order (id, order_no, customer, product, qty, amount, status) VALUES (1005,'OM20260901005','周九','MacBook Pro',1,18999.00,'PAID');" 2>&1 | grep -v Warning || true
sleep 12
FOUND=""
for t in $(docker exec dcp-demo-db mysql -uroot -proot123 -N -e "USE target_db; SHOW TABLES LIKE 't_oms_wide_%';" 2>/dev/null); do
  R=$(docker exec dcp-demo-db mysql -uroot -proot123 -N -e "USE target_db; SELECT CONCAT(id,'|',customer,'|',amount,'|',seq) FROM \`$t\` WHERE id=1005;" 2>/dev/null)
  [ -n "$R" ] && FOUND="$t: $R"
done
if [ -n "$FOUND" ]; then
  ok "增量行已同步 → $FOUND"
else
  warn "id=1005 未找到, 再等 10 秒..."
  sleep 10
  for t in $(docker exec dcp-demo-db mysql -uroot -proot123 -N -e "USE target_db; SHOW TABLES LIKE 't_oms_wide_%';" 2>/dev/null); do
    R=$(docker exec dcp-demo-db mysql -uroot -proot123 -N -e "USE target_db; SELECT CONCAT(id,'|',customer,'|',amount,'|',seq) FROM \`$t\` WHERE id=1005;" 2>/dev/null)
    [ -n "$R" ] && FOUND="$t: $R"
  done
  [ -n "$FOUND" ] && ok "增量行已同步 → $FOUND" || warn "仍未找到, 请查后端日志"
fi
LAG=$(curl -s "$BASE/api/sync/scale/lag/t_oms_wide" -H "$AUTH" | python3 -c "import sys,json;d=json.load(sys.stdin)['data'];print(d.get('totalLag', d))" 2>/dev/null || echo "-")
echo "  Kafka lag: $LAG"
echo ""
info "✅ 演示完成. 可到 http://localhost:8080 → 模型管理 看「订单模型演示」; 数据同步 → 同步任务 → t_oms_wide 看运行状态/历史"
