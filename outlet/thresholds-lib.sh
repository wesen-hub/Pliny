#!/usr/bin/env bash
# ============================================================================
# 出口监控阈值 · 单一来源读取库(2026-09-15)
#
# ★ 唯一源(Single Source of Truth):
#     backend/src/main/resources/application.yml 的 dcp.outlet.monitor.*(默认值)
#   即:后端 OutletProperties.monitor 运行时生效的那份。其余落地点全部由它派生:
#     ① 后端运行时判定       OutletAlertEvaluator 直接读 OutletProperties.monitor(同源,天然一致)
#     ② Prometheus 告警规则  deploy/prometheus/rules/outlet-alerts.yml  ← 由 .yml.tmpl 渲染生成
#     ③ 现场巡检脚本        outlet/monitor-outlet-stack.sh 的默认值(env 可覆盖)
#
#   改阈值:改 application.yml → 运行 outlet/render-alert-rules.sh → 运行 outlet/verify-thresholds.sh
#   用法: source "$(dirname "$0")/thresholds-lib.sh"; load_thresholds; echo "$LAG_WARN"
# ============================================================================

REPO_ROOT="${REPO_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
APP_YML="${DCP_APP_YML:-$REPO_ROOT/backend/src/main/resources/application.yml}"
PROPS_JAVA="${DCP_PROPS_JAVA:-$REPO_ROOT/backend/src/main/java/com/dcp/module/outlet/OutletProperties.java}"
SCRIPT_SH="${DCP_SCRIPT_SH:-$REPO_ROOT/outlet/monitor-outlet-stack.sh}"
RULES_YML="${DCP_RULES_YML:-$REPO_ROOT/deploy/prometheus/rules/outlet-alerts.yml}"
RULES_TMPL="${DCP_RULES_TMPL:-$REPO_ROOT/deploy/prometheus/rules/outlet-alerts.yml.tmpl}"

# 从 application.yml 的 dcp.outlet.monitor 段取某键的**默认值**(形如 `key: ${ENV:300}`)
yml_default() {
  local key="$1"
  sed -n "s/^ *${key}: *\${[A-Z_0-9]*:\([0-9][0-9]*\)}.*$/\1/p" "$APP_YML" | head -1
}

# 从 monitor-outlet-stack.sh 取环境变量默认值(形如 `LAG_WARN="${LAG_WARN:-300}"`)
sh_default() {
  local var="$1"
  sed -n "s/^${var}=\"\${${var}:-\([0-9][0-9]*\)}\".*$/\1/p" "$SCRIPT_SH" | head -1
}

# 从 OutletProperties.java 取 Java 字段默认值(形如 `private long lagWarnSeconds = 300L;` / `private int expectedBrokers = 3;`)
java_default() {
  local field="$1"
  sed -nE "s/^ *private (long|int) ${field} = ([0-9][0-9]*)L*;.*$/\2/p" "$PROPS_JAVA" | head -1
}

load_thresholds() {
  LAG_WARN="$(yml_default 'lag-warn-seconds')"
  LAG_CRIT="$(yml_default 'lag-crit-seconds')"
  GC_WARN="$(yml_default 'gc-warn-seconds')"
  EXPECTED_BROKERS="$(yml_default 'expected-brokers')"
  COLLECT_INTERVAL_MS="$(yml_default 'interval-ms')"

  for v in LAG_WARN LAG_CRIT GC_WARN EXPECTED_BROKERS COLLECT_INTERVAL_MS; do
    if [[ -z "${!v}" ]]; then
      echo "FATAL: 单一来源 $APP_YML 中 dcp.outlet.monitor 缺键(或格式不符),无法解析 $v" >&2
      return 1
    fi
  done

  COLLECT_INTERVAL_S=$(( COLLECT_INTERVAL_MS / 1000 ))
  # 采集器 up 规则:连续 2 个采集周期;时间戳停滞规则:3 个采集周期
  COLLECT_UP_WINDOW="${COLLECT_UP_WINDOW:-$(( COLLECT_INTERVAL_S * 2 ))s}"
  COLLECT_STALE_WINDOW="${COLLECT_STALE_WINDOW:-$(( COLLECT_INTERVAL_S * 3 ))s}"
  COLLECT_STALE_SECONDS=$(( COLLECT_INTERVAL_S * 3 ))
  export REPO_ROOT APP_YML PROPS_JAVA SCRIPT_SH RULES_YML RULES_TMPL
  export LAG_WARN LAG_CRIT GC_WARN EXPECTED_BROKERS COLLECT_INTERVAL_MS COLLECT_INTERVAL_S
  export COLLECT_UP_WINDOW COLLECT_STALE_WINDOW COLLECT_STALE_SECONDS
}
