package com.dcp.module.outlet.metrics;

import com.dcp.module.outlet.OutletProperties;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 出口告警判定(2026-09-14):**与 {@code outlet/prometheus/rules/outlet-alerts.yml} 同一套阈值口径**。
 *
 * <p>为什么要两份:Prometheus 负责"时序 + 通知",而消费任务详情页要在**没有 Prometheus**的情况下
 * 也能显示"当前是否命中阈值" —— 所以判定逻辑必须能进程内直接跑(本类),两边阈值同源(都取
 * {@code dcp.outlet.monitor.*},默认值 = 脚本口径 LAG_WARN=300 / LAG_CRIT=3600 / GC_WARN=21600 / ISR&lt;RF)。
 *
 * <p>判定项:
 * <ol>
 *   <li>changefeed 探针失败(采集不到)→ CRIT(不知道状态本身就是严重问题,绝不当作"正常");</li>
 *   <li>changefeed state ≠ normal → CRIT;</li>
 *   <li>checkpoint lag &gt; LAG_CRIT → CRIT; &gt; LAG_WARN → WARN;</li>
 *   <li>GC 剩余 &lt; GC_WARN → WARN;</li>
 *   <li>Kafka 最小 ISR &lt; RF → CRIT;</li>
 *   <li>broker 少于 3 → WARN;</li>
 *   <li>消费方位点滞后:&gt; LAG_CRIT → CRIT; &gt; LAG_WARN → WARN;</li>
 *   <li>批作业失败:最近窗口内有失败 → WARN;失败占比 &gt; 50%(且样本 ≥ 5)→ CRIT。</li>
 * </ol>
 */
@Component
public class OutletAlertEvaluator {

    public static final String LEVEL_WARN = "WARN";
    public static final String LEVEL_CRIT = "CRIT";

    private final OutletProperties props;

    public OutletAlertEvaluator(OutletProperties props) {
        this.props = props;
    }

    public OutletProperties.Monitor monitor() {
        return props.getMonitor() == null ? new OutletProperties.Monitor() : props.getMonitor();
    }

    /* ------------------------------------------------------------- 共享栈(上游)告警 */

    /** 依据采集快照判定栈级告警(上游故障会让所有消费方一起受影响,详情页需要看到)。 */
    @SuppressWarnings("unchecked")
    public List<Map<String, Object>> evaluateStack(Map<String, Object> snapshot) {
        List<Map<String, Object>> out = new ArrayList<>();
        OutletProperties.Monitor m = monitor();
        if (snapshot == null || snapshot.isEmpty()) {
            out.add(alert(LEVEL_WARN, "stack_collector_no_data",
                    "尚无采集快照(采集器未跑或刚启动)", null, null));
            return out;
        }
        Map<String, Object> cf = (Map<String, Object>) snapshot.get("changefeed");
        if (cf != null) {
            boolean probeOk = Boolean.TRUE.equals(cf.get("probeOk"));
            String state = String.valueOf(cf.get("state"));
            double lag = num(cf.get("lagSeconds"));
            if (!probeOk) {
                out.add(alert(LEVEL_CRIT, "changefeed_probe_failed",
                        "changefeed 采集失败(指标登记 -1,状态未知≠正常)", null, null));
            } else if (!"normal".equals(state)) {
                out.add(alert(LEVEL_CRIT, "changefeed_not_normal",
                        "changefeed state=" + state, state, "normal"));
            }
            if (lag >= 0) {
                if (lag > m.getLagCritSeconds()) {
                    out.add(alert(LEVEL_CRIT, "changefeed_lag_crit",
                            "checkpoint 滞后 " + lag + "s > " + m.getLagCritSeconds() + "s", lag, m.getLagCritSeconds()));
                } else if (lag > m.getLagWarnSeconds()) {
                    out.add(alert(LEVEL_WARN, "changefeed_lag_warn",
                            "checkpoint 滞后 " + lag + "s > " + m.getLagWarnSeconds() + "s", lag, m.getLagWarnSeconds()));
                }
            }
        }
        Map<String, Object> gc = (Map<String, Object>) snapshot.get("gc");
        if (gc != null) {
            double remaining = num(gc.get("remainingSeconds"));
            if (remaining >= 0 && remaining < m.getGcWarnSeconds()) {
                out.add(alert(LEVEL_WARN, "gc_window_shrinking",
                        "GC 剩余窗口 " + remaining + "s < " + m.getGcWarnSeconds() + "s(快照续拉将不可用)",
                        remaining, m.getGcWarnSeconds()));
            }
        }
        Map<String, Object> kafka = (Map<String, Object>) snapshot.get("kafka");
        if (kafka != null) {
            boolean probeOk = Boolean.TRUE.equals(kafka.get("probeOk"));
            double isrMin = num(kafka.get("isrMin"));
            double rf = num(kafka.get("rf"));
            double brokers = num(kafka.get("brokersUp"));
            if (!probeOk) {
                out.add(alert(LEVEL_CRIT, "kafka_probe_failed", "Kafka 采集失败(ISR/RF 未知)", null, null));
            } else {
                if (isrMin >= 0 && rf > 0 && isrMin < rf) {
                    out.add(alert(LEVEL_CRIT, "kafka_isr_below_rf",
                            "最小 ISR " + (long) isrMin + " < RF " + (long) rf + "(写可用性风险)", isrMin, rf));
                }
                if (brokers >= 0 && brokers < 3) {
                    out.add(alert(LEVEL_WARN, "kafka_broker_down",
                            "存活 broker " + (long) brokers + "/3", brokers, 3));
                }
            }
        }
        return out;
    }

    /* ----------------------------------------------------------------- 消费方级告警 */

    /**
     * @param lags       每个流的滞后(秒;-1 = 无法判定)
     * @param recentJobs 最近批作业环形缓冲(用于失败率判定;Prometheus 那边用 rate() 窗口)
     */
    public List<Map<String, Object>> evaluateConsumer(List<Map<String, Object>> lags,
                                                      List<Map<String, Object>> recentJobs) {
        List<Map<String, Object>> out = new ArrayList<>();
        OutletProperties.Monitor m = monitor();
        if (lags != null) {
            for (Map<String, Object> l : lags) {
                double lag = num(l.get("lagSeconds"));
                String stream = String.valueOf(l.get("streamId"));
                if (lag < 0) {
                    out.add(alert(LEVEL_WARN, "consumer_lag_unknown",
                            "stream=" + stream + " 位点滞后无法判定(无水位线或采集失败)", -1, null));
                    continue;
                }
                if (lag > m.getLagCritSeconds()) {
                    out.add(alert(LEVEL_CRIT, "consumer_lag_crit",
                            "stream=" + stream + " 位点滞后 " + lag + "s > " + m.getLagCritSeconds() + "s",
                            lag, m.getLagCritSeconds()));
                } else if (lag > m.getLagWarnSeconds()) {
                    out.add(alert(LEVEL_WARN, "consumer_lag_warn",
                            "stream=" + stream + " 位点滞后 " + lag + "s > " + m.getLagWarnSeconds() + "s",
                            lag, m.getLagWarnSeconds()));
                }
            }
        }
        int total = recentJobs == null ? 0 : recentJobs.size();
        int fails = 0;
        if (recentJobs != null) {
            for (Map<String, Object> j : recentJobs) {
                if (OutletMetrics.RESULT_FAIL.equals(j.get("result"))) {
                    fails++;
                }
            }
        }
        if (total > 0 && fails > 0) {
            double ratio = (double) fails / total;
            if (ratio > 0.5 && total >= 5) {
                out.add(alert(LEVEL_CRIT, "batch_fail_ratio_crit",
                        "最近 " + total + " 次批作业失败 " + fails + " 次(占比 " + Math.round(ratio * 100) + "% > 50%)",
                        ratio, 0.5));
            } else {
                out.add(alert(LEVEL_WARN, "batch_failed_recently",
                        "最近 " + total + " 次批作业失败 " + fails + " 次", fails, 0));
            }
        }
        return out;
    }

    /** 整体等级:CRIT &gt; WARN &gt; OK */
    public String worst(List<Map<String, Object>> alerts) {
        if (alerts == null || alerts.isEmpty()) {
            return "OK";
        }
        return alerts.stream().anyMatch(a -> LEVEL_CRIT.equals(a.get("level"))) ? LEVEL_CRIT : LEVEL_WARN;
    }

    private Map<String, Object> alert(String level, String key, String message, Object value, Object threshold) {
        Map<String, Object> a = new LinkedHashMap<>();
        a.put("level", level);
        a.put("key", key);
        a.put("message", message);
        a.put("value", value);
        a.put("threshold", threshold);
        return a;
    }

    private static double num(Object o) {
        return o instanceof Number n ? n.doubleValue() : -1d;
    }
}
