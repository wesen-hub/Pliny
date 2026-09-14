#!/usr/bin/env bash
# ============================================================================
# 由**单一来源**渲染 Prometheus 告警规则(2026-09-15)
#
#   源:backend/src/main/resources/application.yml → dcp.outlet.monitor.*
#   模板:deploy/prometheus/rules/outlet-alerts.yml.tmpl
#   产物:deploy/prometheus/rules/outlet-alerts.yml(已提交,便于 review 与直接挂载)
#
# 用法:./outlet/render-alert-rules.sh [--check]
#   --check  : 只渲染到临时文件并与已提交产物比对,有漂移则 exit 1(给 CI/收口用)
# ============================================================================
set -euo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=thresholds-lib.sh
source "$DIR/thresholds-lib.sh"
load_thresholds

CHECK=0
[ "${1:-}" = "--check" ] && CHECK=1

TMP="$(mktemp)"
trap 'rm -f "$TMP"' EXIT

sed -e "s/@@LAG_WARN@@/${LAG_WARN}/g" \
    -e "s/@@LAG_CRIT@@/${LAG_CRIT}/g" \
    -e "s/@@GC_WARN@@/${GC_WARN}/g" \
    -e "s/@@EXPECTED_BROKERS@@/${EXPECTED_BROKERS}/g" \
    -e "s/@@COLLECT_STALE_SECONDS@@/${COLLECT_STALE_SECONDS}/g" \
    -e "s/@@COLLECT_STALE_WINDOW@@/${COLLECT_STALE_WINDOW}/g" \
    -e "s/@@COLLECT_UP_WINDOW@@/${COLLECT_UP_WINDOW}/g" \
    "$RULES_TMPL" > "$TMP"

if [ "$CHECK" = "1" ]; then
  if ! diff -u "$RULES_YML" "$TMP" >/dev/null 2>&1; then
    echo "❌ 告警规则与单一来源不一致(有漂移):"
    diff -u "$RULES_YML" "$TMP" | head -40
    echo "   → 运行 ./outlet/render-alert-rules.sh 重新生成"
    exit 1
  fi
  echo "✅ 告警规则与单一来源一致(无漂移)"
  exit 0
fi

cp "$TMP" "$RULES_YML"
echo "✅ 已渲染 $RULES_YML"
echo "   阈值来源:${APP_YML}"
echo "   LAG_WARN=${LAG_WARN}s  LAG_CRIT=${LAG_CRIT}s  GC_WARN=${GC_WARN}s  EXPECTED_BROKERS=${EXPECTED_BROKERS}"
echo "   采集器窗口:up=${COLLECT_UP_WINDOW}  stale=${COLLECT_STALE_WINDOW}(${COLLECT_STALE_SECONDS}s)"
echo "   → 让 Prometheus 生效:curl -X POST http://localhost:9090/-/reload(或重启容器)"
