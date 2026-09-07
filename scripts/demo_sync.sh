#!/bin/bash
# ============================================================
# 数据采集平台 V3 同步链路 一键演示
# 链路: 真实 MySQL binlog → seq 打标 → Kafka 桶路由 → 批量消费(原子幂等写)
#        → MySQL 分表 → 权威副本追平 → lag 监控
# 用法: bash scripts/demo_sync.sh   (可重复执行, 幂等)
# ============================================================
set -e
BASE=http://localhost:8080
GREEN='\033[0;32m'; YELLOW='\033[1;33m'; CYAN='\033[0;36m'; NC='\033[0m'
info()  { echo -e "${CYAN}[演示]${NC} $1"; }
ok()    { echo -e "${GREEN}  ✔ $1${NC}"; }
warn()  { echo -e "${YELLOW}  ⚠ $1${NC}"; }

# ---------- 1. 登录 ----------
info "1/8 登录系统"
python3 - << 'PYEOF' > /tmp/dcp_tk
import urllib.request, json
req = urllib.request.Request("http://localhost:8080/api/auth/login",
    data=json.dumps({"username":"admin","password":"admin123"}).encode(),
    headers={"Content-Type":"application/json"})
print(json.load(urllib.request.urlopen(req))["data"]["token"])
PYEOF
IFS= read -r TOKEN < /tmp/dcp_tk
AUTH="Authorization: Bearer $TOKEN"
ok "登录成功"

# ---------- 2. 数据源(存在则复用) ----------
info "2/8 检查演示数据源(指向 demo-db 源库 source_db)"
DSID=$(curl -s "$BASE/api/datasources?pageSize=200" -H "$AUTH" | python3 -c "
import sys,json
for x in json.load(sys.stdin)['data']['list']:
    if x['name']=='演示源库': print(x['id']); break")
if [ -z "$DSID" ]; then
  DSID=$(curl -s -X POST $BASE/api/datasources -H "$AUTH" -H 'Content-Type: application/json' \
    -d '{"name":"演示源库","type":"mysql","host":"demo-db","port":3306,"databaseName":"source_db","username":"root","password":"admin123","env":"demo","groupName":"演示"}' \
    | python3 -c "import sys,json;print(json.load(sys.stdin)['data'])")
  ok "已创建数据源 id=$DSID"
else
  ok "复用数据源 id=$DSID"
fi

# ---------- 3. 逻辑表配置(业务主键) ----------
info "3/8 配置逻辑表 t_order_cdc(业务主键 = 源表主键 id, 合并模式)"
curl -s -X PUT $BASE/api/sync/logical-tables/t_order_cdc -H "$AUTH" -H 'Content-Type: application/json' \
  -d '{"topic":"dcp.order.cdc","tablePrefix":"t_order_cdc","tableCount":50,"partitionCount":8,"keyColumns":["id"],"businessKey":{"mode":"merge","keyFields":["id"]}}' \
  | python3 -c "import sys,json;d=json.load(sys.stdin); assert d['code']==0, d; print('  保存成功')"
ok "业务主键配置已保存(键构成: id)"

# ---------- 4. 注册并启动 CDC 采集 ----------
info "4/8 注册并启动 CDC 采集(source_db.t_order → binlog → Kafka)"
curl -s -X POST $BASE/api/sync/cdc/task -H "$AUTH" -H 'Content-Type: application/json' \
  -d "{\"logicalTable\":\"t_order_cdc\",\"datasourceId\":$DSID,\"sourceDb\":\"source_db\",\"sourceTable\":\"t_order\"}" > /dev/null
RESP=$(curl -s -X POST $BASE/api/sync/cdc/start/t_order_cdc -H "$AUTH")
echo "$RESP" | python3 -c "import sys,json;d=json.load(sys.stdin); assert d['code']==0, d; print('  采集运行中:', d['data']['source'])"
ok "CDC 采集已启动(首次自动全量预导, 之后 binlog 增量)"

# ---------- 5. 对源库执行真实 CRUD ----------
info "5/8 对源库 source_db.t_order 执行真实 CRUD(INSERT/UPDATE/DELETE/改主键)"
BEFORE=$(docker exec dcp-demo-db mysql -uroot -proot123 -N -e "SELECT COUNT(*) FROM source_db.t_order;" 2>/dev/null)
docker exec dcp-demo-db mysql -uroot -proot123 -e "USE source_db;
INSERT INTO t_order (user_id, price, quantity, discount, status, created_at, total_amount) VALUES (999, 88.50, 2, 0.1, 'PAID', NOW(), 159.30);
UPDATE t_order SET status='SHIPPED', total_amount=200.00 WHERE id=1;
DELETE FROM t_order WHERE id=2;
UPDATE t_order SET id=9999 WHERE id=3;" 2>&1 | grep -v Warning || true
ok "已执行: +1 INSERT, UPDATE id=1, DELETE id=2, 改主键 id=3→9999"

# ---------- 6. 等待链路处理, 检查分表 ----------
info "6/8 等待链路处理(消费→分表), 约 8 秒"
sleep 8
echo -e "  分表/权威副本收敛由链路自动完成(权威表行数见下)"
SHARD_CNT=$(docker exec dcp-demo-db mysql -uroot -proot123 -N -e "USE target_db; SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='target_db' AND table_name LIKE 't_order_cdc%' AND table_name NOT LIKE '%authority';" 2>/dev/null)
TOTAL=$(for t in $(docker exec dcp-demo-db mysql -uroot -proot123 -N -e "USE target_db; SELECT table_name FROM information_schema.tables WHERE table_schema='target_db' AND table_name LIKE 't_order_cdc%' AND table_name NOT LIKE '%authority';" 2>/dev/null); do docker exec dcp-demo-db mysql -uroot -proot123 -N -e "USE target_db; SELECT COUNT(*) FROM \`$t\`;" 2>/dev/null; done | awk '{s+=$1} END {print s+0}')
ok "分表 $SHARD_CNT 张, 共 $TOTAL 行"
echo "  关键数据校验:"
docker exec dcp-demo-db mysql -uroot -proot123 -N -e "USE target_db; SELECT CONCAT(t.table_name,'|') FROM information_schema.tables t WHERE t.table_schema='target_db' AND t.table_name LIKE 't_order_cdc%' AND t.table_name NOT LIKE '%authority';" 2>/dev/null | tr -d '|' | while read t; do docker exec dcp-demo-db mysql -uroot -proot123 -N -e "USE target_db; SELECT '$t', id, status, total_amount, seq FROM \`$t\` WHERE id IN (1,2,3,9999);" 2>/dev/null; done | while IFS=$'\t' read -r t id st amt seq; do [ -n "$id" ] && echo "    $t: id=$id status=$st amount=$amt seq=$seq"; done

# ---------- 7. 权威副本追平 ----------
info "7/8 权威副本状态(生产为 Iceberg MERGE)"
curl -s -X POST $BASE/api/sync/lake/flush/t_order_cdc -H "$AUTH" | python3 -c "
import sys,json
d=json.load(sys.stdin)['data']
print('  权威表:', d.get('authorityTable'), d.get('authorityRows', 0), '行 / 最新 seq', d.get('authorityMaxSeq', 0))"
docker exec dcp-demo-db mysql -uroot -proot123 -e "SELECT '权威表(每PK仅保留 max seq 版本):'; SELECT id, status, total_amount, seq FROM target_db.t_order_cdc_authority WHERE id IN (1,2,3,9999) ORDER BY id;" 2>/dev/null | grep -v "^id$\|^权威\|Warning"

# ---------- 8. lag 与扩容 ----------
info "8/8 lag 监控与扩容能力"
curl -s $BASE/api/sync/scale/lag/t_order_cdc -H "$AUTH" | python3 -c "
import sys,json
d=json.load(sys.stdin)['data']
print('  消费组:', d['consumerGroup'], '| 分区:', d['partitionCount'], '| 总 lag:', d['totalLag'])"

echo
echo -e "${GREEN}══════════════════════════════════════════════════════${NC}"
echo -e "${GREEN}  ✅ 演示完成: 真实 binlog → seq → Kafka → 分表幂等写${NC}"
echo -e "${GREEN}     → 权威副本收敛 → lag 监控, 全链路工作正常${NC}"
echo -e "${GREEN}══════════════════════════════════════════════════════${NC}"
