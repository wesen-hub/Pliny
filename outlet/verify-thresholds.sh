#!/usr/bin/env bash
# ============================================================================
# 出口监控阈值 · 一致性校验(2026-09-15)
#
# 目的:阈值只有**一处可改**(application.yml 的 dcp.outlet.monitor.*),
#       其余三个落地点必须与它一致 —— 本脚本把这件事变成可执行校验(不一致 exit 1)。
#
#   ① 后端运行时判定     OutletProperties.java 字段默认值(同源必须是同一份)
#   ② 现场巡检脚本       outlet/monitor-outlet-stack.sh 的 env 默认值
#   ③ Prometheus 告警规则 deploy/prometheus/rules/outlet-alerts.yml(由模板渲染)
#
# 用法:./outlet/verify-thresholds.sh     (CI/收尾必跑)
# ============================================================================
set -uo pipefail
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=thresholds-lib.sh
source "$DIR/thresholds-lib.sh"

fail=0
chk() { # name source_val other_val place
  if [ "$2" = "$3" ]; then printf '  ✅ %-16s %s\n' "$1" "$2"
  else printf '  ❌ %-16s 源=%s  %s=%s\n' "$1" "$2" "$4" "$3"; fail=$((fail+1)); fi
}

if ! load_thresholds; then echo "❌ 无法从单一来源解析阈值"; exit 1; fi

echo "=== 单一来源:${APP_YML} → dcp.outlet.monitor.* ==="
echo "  LAG_WARN=${LAG_WARN}s  LAG_CRIT=${LAG_CRIT}s  GC_WARN=${GC_WARN}s  EXPECTED_BROKERS=${EXPECTED_BROKERS}  INTERVAL=${COLLECT_INTERVAL_MS}ms"

echo "=== ① 后端 OutletProperties.java 默认值 ==="
chk lag-warn-seconds "$LAG_WARN"  "$(java_default lagWarnSeconds)"        "java lagWarnSeconds"
chk lag-crit-seconds "$LAG_CRIT"  "$(java_default lagCritSeconds)"        "java lagCritSeconds"
chk gc-warn-seconds  "$GC_WARN"   "$(java_default gcWarnSeconds)"         "java gcWarnSeconds"
chk expected-brokers "$EXPECTED_BROKERS" "$(java_default expectedBrokers)" "java expectedBrokers"

echo "=== ② 巡检脚本 monitor-outlet-stack.sh 默认值 ==="
chk LAG_WARN "$LAG_WARN" "$(sh_default LAG_WARN)" "sh LAG_WARN"
chk LAG_CRIT "$LAG_CRIT" "$(sh_default LAG_CRIT)" "sh LAG_CRIT"
chk GC_WARN  "$GC_WARN"  "$(sh_default GC_WARN)"  "sh GC_WARN"

echo "=== ③ Prometheus 告警规则(模板渲染 + 与已提交产物比对) ==="
if grep -q '@@' "$RULES_YML" 2>/dev/null; then
  echo "  ❌ $RULES_YML 里还有未替换的 @@ 占位符"; fail=$((fail+1))
else
  echo "  ✅ 无未替换占位符"
fi
if "$DIR/render-alert-rules.sh" --check >/dev/null 2>&1; then
  echo "  ✅ 渲染产物与单一来源一致(无漂移)"
else
  echo "  ❌ 渲染产物与单一来源有漂移 → 跑 ./outlet/render-alert-rules.sh"; fail=$((fail+1))
fi
# 关键规则必须引用渲染后的真实数值
for pair in "OutletChangefeedLagWarn:> ${LAG_WARN}" "OutletChangefeedLagCritical:> ${LAG_CRIT}" "OutletGcWindowShrinking:< ${GC_WARN}" "OutletKafkaBrokerDown:< ${EXPECTED_BROKERS}"; do
  name="${pair%%:*}"; lit="${pair#*:}"
  if grep -A1 "alert: ${name}$" "$RULES_YML" | grep -qF "expr:" && grep -A1 "alert: ${name}$" "$RULES_YML" | grep -qF "$lit"; then
    echo "  ✅ ${name} 表达式含 ${lit}"
  else
    echo "  ❌ ${name} 表达式未含 ${lit}"; fail=$((fail+1))
  fi
done

echo
if [ "$fail" -eq 0 ]; then echo "---- 结果:✅ 阈值三处一致(单一来源:${APP_YML}) ----"; exit 0; fi
echo "---- 结果:❌ ${fail} 项不一致 ----"; exit 1
