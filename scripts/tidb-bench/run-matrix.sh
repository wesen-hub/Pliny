#!/bin/bash
# TiDB 写入压测矩阵: 复刻同步链路攒批形态(batch=500 多值 INSERT ON DUPLICATE KEY UPDATE)
# 环境: docker-compose.outlet.yml TiDB 集群(TiKV×3 各 2C/2g mem_limit, TiDB×2 各 1C)
cd /Users/wesen/Desktop/data-collect-platform
CP="scripts/tidb-bench:seatunnel/dist/mysql-connector-j-8.3.0.jar"
URL="jdbc:mysql://127.0.0.1:14000/target_db?useSSL=false&allowPublicKeyRetrieval=true"
OUT=scripts/tidb-bench/result.txt
: > "$OUT"

# 监控函数: 采样 TiKV/TiDB CPU
snap() {
  docker stats --no-stream --format "{{.Name}} {{.CPUPerc}} {{.MemUsage}}" \
    dcp-outlet-tikv1 dcp-outlet-tikv2 dcp-outlet-tikv3 dcp-outlet-tidb1 dcp-outlet-tidb2 2>/dev/null
}

run_case() {
  local name="$1"; shift
  echo "===== $name =====" | tee -a "$OUT"
  java -cp "$CP" TiDbWriteBench "$URL" "$@" 2>&1 | tee -a "$OUT"
  echo "--- 资源水位 ---" | tee -a "$OUT"
  snap | tee -a "$OUT"
  echo | tee -a "$OUT"
}

javac -cp seatunnel/dist/mysql-connector-j-8.3.0.jar -d scripts/tidb-bench scripts/tidb-bench/TiDbWriteBench.java

run_case "A1 纯INSERT 1线程"  1 500 600000 0 1000
run_case "A2 纯INSERT 2线程"  2 500 600000 0 1000
run_case "A3 纯INSERT 4线程"  4 500 600000 0 1000
run_case "A4 纯INSERT 8线程"  8 500 600000 0 1000
run_case "B1 全冲突UPSERT 4线程" 4 500 300000 100 1000
run_case "B2 全冲突UPSERT 8线程" 8 500 300000 100 1000
echo "DONE"
