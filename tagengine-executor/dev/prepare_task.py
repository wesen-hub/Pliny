#!/usr/bin/env python3
# E2E 准备: 建演示任务(绑定规则流#4 风控优先决策链)→ 发布 → 触发 run(simulate) → 取 task-json 落盘(补湖参数)
import json, time, urllib.request, urllib.error, sys

BASE = "http://localhost:8081"
OUT = "/tmp/tag-e2e/tasks/task.json"

def call(method, path, body=None, token=None, retries=30):
    for _ in range(retries):
        try:
            req = urllib.request.Request(BASE + path, method=method)
            req.add_header("Content-Type", "application/json")
            if token:
                req.add_header("Authorization", "Bearer " + token)
            data = json.dumps(body).encode() if body is not None else None
            with urllib.request.urlopen(req, data=data, timeout=8) as r:
                return json.loads(r.read().decode())
        except urllib.error.HTTPError as e:
            return json.loads(e.read().decode())
        except Exception:
            time.sleep(2)
    raise SystemExit("backend not ready")

auth = call("POST", "/api/auth/login", {"username": "admin", "password": "admin123"})
token = ((auth.get("data") or {}).get("token") or "").strip()
if not token:
    print("login fail", auth); sys.exit(1)

# 用规则流 4(风控优先决策链, 其快照闭包自动带出 黑名单+金额档); 数据表 customer_wide 需含 amount/status/region/customer_id/name
t = call("POST", "/api/tagengine/tasks", {
    "name": "E2E风控金额档批任务", "mode": "batch", "dataset": "customer_wide", "entityKey": "customer_id",
    "datasetFields": ["customer_id", "name", "region", "status", "amount"],
    "derived": [],
    "bindings": [{"kind": "flow", "id": 4, "ver": 1, "name": "风控优先决策链"}],
    "schedule": "none"}, token)
tid = (t.get("data") or {}).get("id")
if not tid:
    print("task create fail", t); sys.exit(1)
pub = call("POST", "/api/tagengine/tasks/%s/publish" % tid, None, token)
if (pub.get("data") or {}).get("status") != "published":
    print("publish fail", pub); sys.exit(1)
run = call("POST", "/api/tagengine/tasks/%s/run?trigger=manual" % tid, None, token)
rid = (run.get("data") or {}).get("runId")
if not rid:
    print("run fail", run); sys.exit(1)
g = call("GET", "/api/tagengine/runs/%s" % rid, None, token)
payload = json.loads((g.get("data") or {}).get("payloadText"))
# 补湖参数(dev hadoop catalog 本地 warehouse; 生产由平台/湖环境配置下发, 见设计文档 §13)
payload["iceberg"].update({"warehouse": "/opt/warehouse", "catalogType": "hadoop", "catalog": "demo", "namespace": "dcp", "table": "customer_wide"})
payload["tagFact"].update({"catalog": "demo", "namespace": "dcp", "table": "tag_fact", "partitionByTask": True})
with open(OUT, "w") as fp:
    json.dump(payload, fp, ensure_ascii=False, indent=2)
print("task-id", tid, "run-id", rid, "json ->", OUT)
print("assets:", [a["name"] for a in payload["assets"]])
print("bindings:", [(b["kind"], b["id"]) for b in payload["bindings"]])
