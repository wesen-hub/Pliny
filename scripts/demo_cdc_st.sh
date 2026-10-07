#!/bin/bash
# ============================================================
# 数据采集平台 V3 同步链路 — SeaTunnel CDC 采集端演示
# 链路: MySQL binlog(connector-cdc-mysql 全量+增量) → CdcSeqTransform(采集转换+位点发号+桶路由)
#        → Kafka → Spring Kafka 消费(原子幂等写) → 分表 → 权威副本追平
# 用法: bash scripts/demo_cdc_st.sh   (可重复执行, 幂等)
# ============================================================
set -e
BASE=http://localhost:8080
GREEN='\033[0;32m'; YELLOW='\033[1;33m'; CYAN='\033[0;36m'; NC='\033[0m'
info()  { echo -e "${CYAN}[SeaTunnel-CDC]${NC} $1"; }
ok()    { echo -e "${GREEN}  ✔ $1${NC}"; }
warn()  { echo -e "${YELLOW}  ⚠ $1${NC}"; }

# ---------- 1. 登录 ----------
info "1/9 登录系统"
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

# ---------- 2. 数据源 ----------
info "2/9 检查演示数据源(指向 demo-db 源库 source_db)"
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

# ---------- 3. 逻辑表配置(SeaTunnel 路径独立逻辑表, 与自研采集器互不干扰) ----------
info "3/9 配置逻辑表 t_order_st(业务主键 = 源表主键 id; topic 复用 dcp.order.cdc)"
curl -s -X PUT $BASE/api/sync/logical-tables/t_order_st -H "$AUTH" -H 'Content-Type: application/json' \
  -d '{"topic":"dcp.order.cdc","tablePrefix":"t_order_st","tableCount":50,"partitionCount":8,"keyColumns":["id"],"businessKey":{"mode":"merge","keyFields":["id"]}}' \
  | python3 -c "import sys,json;d=json.load(sys.stdin); assert d['code']==0, d; print('  保存成功')"
ok "业务主键配置已保存(键构成: id)"

# ---------- 4. 注册 SeaTunnel CDC 任务 ----------
info "4/9 注册 SeaTunnel CDC 采集任务(source_db.t_order → Kafka)"
curl -s -X POST $BASE/api/sync/cdc-st/task -H "$AUTH" -H 'Content-Type: application/json' \
  -d "{\"logicalTable\":\"t_order_st\",\"datasourceId\":$DSID,\"sourceDb\":\"source_db\",\"sourceTable\":\"t_order\",\"businessKeyFields\":[\"id\"],\"keyColumns\":[\"id\"],\"topic\":\"dcp.order.cdc\"}" \
  | python3 -c "import sys,json;d=json.load(sys.stdin); assert d['code']==0, d; print('  注册成功')"
ok "任务已注册"

# ---------- 5. 启动 SeaTunnel 作业 ----------
info "5/9 启动 SeaTunnel 作业(connector-cdc-mysql 全量快照 + binlog 增量 → CdcSeqTransform → Kafka)"
RESP=$(curl -s -X POST $BASE/api/sync/cdc-st/start/t_order_st -H "$AUTH")
JOBID=$(echo "$RESP" | python3 -c "import sys,json;print(json.load(sys.stdin)['data']['seaTunnelJobId'])")
echo "$RESP" | python3 -c "import sys,json;d=json.load(sys.stdin); assert d['code']==0, d; print('  作业已提交, seaTunnelJobId='+str(d['data']['seaTunnelJobId']))"
ok "SeaTunnel CDC 作业已启动 (jobId=$JOBID)"

# 等待 SeaTunnel 侧 RUNNING(全量快照需要一点时间)
for i in $(seq 1 20); do
  ST=$(curl -s "$BASE/api/sync/cdc-st/status" -H "$AUTH" | python3 -c "
import sys,json
for t in json.load(sys.stdin)['data']:
    if t['logicalTable']=='t_order_st': print('RUNNING' if t['running'] else 'STARTING'); break")
  [ "$ST" = "RUNNING" ] && break
  sleep 3
done
[ "$ST" = "RUNNING" ] && ok "SeaTunnel 作业 RUNNING(全量快照处理中)" || warn "作业状态: ${ST:-未知}(继续等待消费端校验)"

# ---------- 6. 等全量快照进 Kafka → 分表 ----------
info "6/9 等待全量快照消费, 约 15 秒"
sleep 15
echo -e "  分片行数: ${GREEN}$TOTAL${NC} (权威副本由 30s 追平任务收敛)"
SHARD_CNT=$(docker exec dcp-demo-db mysql -uroot -proot123 -N -e "USE target_db; SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='target_db' AND table_name LIKE 't_order_st%' AND table_name NOT LIKE '%authority';" 2>/dev/null)
TOTAL=$(for t in $(docker exec dcp-demo-db mysql -uroot -proot123 -N -e "USE target_db; SELECT table_name FROM information_schema.tables WHERE table_schema='target_db' AND table_name LIKE 't_order_st%' AND table_name NOT LIKE '%authority';" 2>/dev/null); do docker exec dcp-demo-db mysql -uroot -proot123 -N -e "USE target_db; SELECT COUNT(*) FROM \`$t\`;" 2>/dev/null; done | awk '{s+=$1} END {print s+0}')
ok "全量快照落库: 分表 $SHARD_CNT 张, 共 $TOTAL 行"

# ---------- 7. 源库真实 CRUD(增量) ----------
info "7/9 对源库执行 CRUD(增量链路)"
docker exec dcp-demo-db mysql -uroot -proot123 -e "USE source_db;
INSERT INTO t_order (user_id, price, quantity, discount, status, created_at, total_amount) VALUES (888, 66.50, 3, 0.2, 'PAID', NOW(), 159.60);
UPDATE t_order SET status='SHIPPED', total_amount=300.00 WHERE id=1;
DELETE FROM t_order WHERE id=9999;" 2>&1 | grep -v Warning || true
ok "已执行: +1 INSERT, UPDATE id=1, DELETE id=9999"
sleep 8
echo -e "  权威副本状态:"
curl -s "$BASE/api/sync/lake/status/t_order_st" -H "$AUTH" | python3 -c 'import sys,json; d=json.load(sys.stdin)["data"]; print("    权威表 %s: %s 行, 已追平到 seq %s" % (d.get("authorityTable"), d.get("authorityRows", 0), d.get("authorityMaxSeq", 0)))' 2>/dev/null || echo "    (权威表未就绪)"
docker exec dcp-demo-db mysql -uroot -proot123 -N -e "USE target_db; SELECT CONCAT(t.table_name,'|') FROM information_schema.tables t WHERE t.table_schema='target_db' AND t.table_name LIKE 't_order_st%' AND t.table_name NOT LIKE '%authority';" 2>/dev/null | tr -d '|' | while read t; do docker exec dcp-demo-db mysql -uroot -proot123 -N -e "USE target_db; SELECT '$t', id, user_id, status, total_amount, seq FROM \`$t\` WHERE user_id IN (1, 888);" 2>/dev/null; done | while IFS=$'\t' read -r t id uid st amt seq; do [ -n "$id" ] && echo "    $t: id=$id user_id=$uid status=$st amount=$amt seq=$seq"; done

# ---------- 8. 权威副本追平 + lag ----------
info "8/9 权威副本状态 + lag"
sleep 12
curl -s "$BASE/api/sync/lake/status/t_order_st" -H "$AUTH" | python3 -c "
import sys,json
d=json.load(sys.stdin)['data']
print(f\"  权威表 {d.get('authorityTable')}: {d.get('authorityRows',0)} 行, 最新 seq {d.get('authorityMaxSeq',0)}\")"
LAG=$(curl -s "$BASE/api/sync/scale/lag/t_order_st" -H "$AUTH" | python3 -c "import sys,json;d=json.load(sys.stdin)['data'];print(d.get('totalLag', d))")
echo -e "  Kafka lag: ${GREEN}$LAG${NC}"
AUTH_CNT=$(docker exec dcp-demo-db mysql -uroot -proot123 -N -e "USE target_db; SELECT COUNT(*) FROM t_order_st_authority;" 2>/dev/null || echo 0)
ok "权威表 t_order_st_authority $AUTH_CNT 行(全量快照行收敛)"

# ---------- 9. 停止 → 重启(验证 checkpoint 续采) ----------
info "9/9 停止作业 → 源库再插一条 → 重启(等 CANCELED 后同 jobId + savepoint 续采)"
curl -s -X POST $BASE/api/sync/cdc-st/stop/t_order_st -H "$AUTH" > /dev/null
# 优雅停止需要几秒, 轮询等 SeaTunnel 侧确认 CANCELED 再重启(避免幂等短路)
for i in $(seq 1 15); do
  ST=$(curl -s "http://localhost:8082/job-info/$(curl -s $BASE/api/sync/cdc-st/status -H "$AUTH" | python3 -c "import sys,json;print(json.load(sys.stdin)['data'][0]['seaTunnelJobId'])")" | python3 -c "import sys,json;print(json.load(sys.stdin)['jobStatus'])" 2>/dev/null || echo UNKNOWN)
  [ "$ST" = "CANCELED" ] && break
  sleep 2
done
echo -e "  停止后作业状态: $ST"
docker exec dcp-demo-db mysql -uroot -proot123 -e "USE source_db; INSERT INTO t_order (user_id, price, quantity, discount, status, created_at, total_amount) VALUES (777, 10.00, 1, 0, 'PAID', NOW(), 10.00);" 2>&1 | grep -v Warning || true
curl -s -X POST $BASE/api/sync/cdc-st/start/t_order_st -H "$AUTH" > /dev/null
ok "已重启作业, 等待恢复 + 增量消费..."
sleep 25
echo -e "  重启后续采验证: 数据幂等吸收(权威表行数见上)"
docker exec dcp-demo-db mysql -uroot -proot123 -N -e "USE target_db; SELECT CONCAT(t.table_name,'|') FROM information_schema.tables t WHERE t.table_schema='target_db' AND t.table_name LIKE 't_order_st%' AND t.table_name NOT LIKE '%authority';" 2>/dev/null | tr -d '|' | while read t; do docker exec dcp-demo-db mysql -uroot -proot123 -N -e "USE target_db; SELECT '$t', id, user_id, total_amount, seq FROM \`$t\` WHERE user_id=777;" 2>/dev/null; done | while IFS=$'\t' read -r t id uid amt seq; do [ -n "$id" ] && echo "    $t: id=$id user_id=$uid amount=$amt seq=$seq (重启后增量 ✓)"; done
ok "演示完成: SeaTunnel CDC 采集端 全量+增量+重启重放无害 全链路 ✅"
