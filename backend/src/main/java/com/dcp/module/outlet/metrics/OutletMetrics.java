package com.dcp.module.outlet.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Component;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 出口模块的**业务指标出口**(2026-09-14 出口监控)。
 *
 * <p>一句话职责:所有出口指标只在这里落地 —— 埋点方(批通道 / 消费方 SDK / 快照任务 / 栈采集器)
 * 只管调用语义化方法,**指标名与标签口径只此一处**,避免口径漂移。
 *
 * <p>设计要点:
 * <ul>
 *   <li><b>标签即查询维度</b>:{@code consumer_id} / {@code stream_id} / {@code job_type} / {@code kind}
 *       / {@code result} —— 消费任务详情页按消费方读;</li>
 *   <li><b>双去向</b>:①Micrometer → {@code /api/actuator/prometheus}(时序/告警用的历史);
 *       ②本类自持"当前值"(Gauge 的 AtomicReference + 最近批作业环形缓冲)→
 *       {@code GET /api/chg/consumers/{id}/metrics} 即使没有 Prometheus 也能返回当前值(对账用);</li>
 *   <li><b>采集失败不静默</b>:不可用一律登记 {@link #UNAVAILABLE}(-1)并留痕,绝不"假装是 0"。</li>
 * </ul>
 *
 * <p>Micrometer 名 → Prometheus 暴露名(实测见部署记录 §18):Counter {@code outlet_batch_job}
 * → {@code outlet_batch_job_total};Timer {@code outlet_batch_duration_seconds}
 * → {@code outlet_batch_duration_seconds_{count,sum,max}}。
 */
@Slf4j
@Component
public class OutletMetrics {

    /* ------------------------------------------------------------------ 常量(标签值口径) */

    /** 批作业成功(真跑了执行器并发布) */
    public static final String RESULT_OK = "ok";
    /** 命中幂等复用(产物已在,**未重复搬数**) */
    public static final String RESULT_REUSED = "reuse";
    /** 失败(异常;位点不推进) */
    public static final String RESULT_FAIL = "fail";
    /** 空转(无新数据 lo ≥ hi、mixed 短路等——不是失败) */
    public static final String RESULT_NOOP = "noop";

    /** TSO 的低 18 位是逻辑位,高 46 位是毫秒物理时钟 */
    public static final double TSO_TICKS_PER_SECOND = 262144d * 1000d;

    /** 采集失败 / 无法判定(与 outlet/monitor-outlet-stack.sh 的 -1 口径一致) */
    public static final double UNAVAILABLE = -1d;

    /** 每消费方保留的最近批作业条数(详情页"最近 N 次") */
    private static final int RECENT_MAX = 50;

    private final MeterRegistry registry;

    /** 消费方位点滞后(秒):{@code consumer::stream → 当前值} */
    private final Map<String, AtomicReference<Double>> consumerLag = new ConcurrentHashMap<>();
    /** 栈指标(chg_*)当前值 */
    private final Map<String, AtomicReference<Double>> stack = new ConcurrentHashMap<>();
    /** 最近批作业(消费方 → 定长双端队列,新值在队首) */
    private final Map<String, Deque<Map<String, Object>>> recentJobs = new ConcurrentHashMap<>();

    public OutletMetrics(ObjectProvider<MeterRegistry> registryProvider) {
        MeterRegistry real = registryProvider.getIfAvailable();
        if (real == null) {
            log.warn("[outlet-metrics] 未发现 MeterRegistry → 退回 SimpleMeterRegistry(指标只在进程内,不影响业务)");
            real = new SimpleMeterRegistry();
        }
        this.registry = real;
    }

    /* ============================================================== ① 批作业(执行器 B) */

    /**
     * 记一次批作业(消费任务详情页最主要的指标)。
     *
     * @param result {@link #RESULT_OK} / {@link #RESULT_REUSED} / {@link #RESULT_FAIL} / {@link #RESULT_NOOP}
     * @param rows   本次作业产出/可见的行数(复用命中时是"产物行数",不是新搬的行数)
     * @param seconds 耗时(秒)
     */
    public void recordBatchJob(String consumerId, String streamId, String jobType, String result,
                               long rows, double seconds, String note) {
        String r = result == null ? RESULT_OK : result;
        String job = jobType == null ? "unknown" : jobType;
        try {
            counter("outlet_batch_job", "consumer_id", consumerId, "stream_id", streamId,
                    "job_type", job, "result", r).increment();
            if (rows > 0) {
                counter("outlet_batch_rows", "consumer_id", consumerId, "stream_id", streamId,
                        "job_type", job, "result", r).increment(rows);
            }
            timer("outlet_batch_duration_seconds", "consumer_id", consumerId, "stream_id", streamId,
                    "job_type", job, "result", r).record((long) Math.max(0d, seconds * 1000d), TimeUnit.MILLISECONDS);
        } catch (RuntimeException e) {
            log.warn("[outlet-metrics] 批作业指标登记失败(不影响作业):{}", e.getMessage());
        }
        pushRecent(consumerId, jobRecord(job, streamId, r, rows, seconds, note));
    }

    /* ============================================================= ② 消费方 SDK(执行器 A) */

    /**
     * 记一个 SDK 批次(快照段/增量段分开计数)。
     *
     * @param kind SNAPSHOT | CHANGE
     */
    public void recordSdkBatch(String consumerId, String streamId, String kind, int rows, int events) {
        String k = kind == null ? "unknown" : kind;
        try {
            counter("outlet_sdk_batches", "consumer_id", consumerId, "stream_id", streamId, "kind", k).increment();
            if (rows > 0) {
                counter("outlet_sdk_rows", "consumer_id", consumerId, "stream_id", streamId, "kind", k).increment(rows);
            }
            if (events > 0) {
                counter("outlet_sdk_events", "consumer_id", consumerId, "stream_id", streamId, "kind", k).increment(events);
            }
        } catch (RuntimeException e) {
            log.warn("[outlet-metrics] SDK 指标登记失败(不影响消费):{}", e.getMessage());
        }
    }

    /* ================================================================= ③ 快照任务(snapshot_job) */

    /** 记一次快照任务状态迁移(READY/PAGING/DONE/EXPIRED/FAILED)。 */
    public void recordSnapshotJob(String consumerId, String streamId, String status) {
        try {
            counter("outlet_snapshot_job", "consumer_id", consumerId, "stream_id", streamId,
                    "status", status == null ? "UNKNOWN" : status).increment();
        } catch (RuntimeException e) {
            log.warn("[outlet-metrics] 快照任务指标登记失败(不影响快照):{}", e.getMessage());
        }
    }

    /* ================================================== ④ 位点滞后(追平判据)+ 栈指标 Gauge */

    /**
     * 登记/更新消费方位点滞后(秒)。**这是"追平"判据**:0 表示已追平,-1 表示无法判定
     * (无位点或无水位线)。
     */
    public void setConsumerLag(String consumerId, String streamId, double lagSeconds) {
        AtomicReference<Double> ref = consumerLag.computeIfAbsent(key(consumerId, streamId), k -> {
            AtomicReference<Double> r = new AtomicReference<>(UNAVAILABLE);
            gauge("outlet_consumer_offset_lag_seconds", r, "consumer_id", consumerId, "stream_id", streamId);
            return r;
        });
        ref.set(lagSeconds);
    }

    /** 读当前滞后值(不依赖 Prometheus;无记录返回 null) */
    public Double consumerLag(String consumerId, String streamId) {
        AtomicReference<Double> ref = consumerLag.get(key(consumerId, streamId));
        return ref == null ? null : ref.get();
    }

    /** 登记/更新一个栈指标(chg_*)Gauge。{@code tags} 为 key/value 交替,可为空。 */
    public void setStackGauge(String name, Map<String, String> tags, double value) {
        AtomicReference<Double> ref = stack.computeIfAbsent(stackKey(name, tags), k -> {
            AtomicReference<Double> r = new AtomicReference<>(value);
            String[] kv = new String[tags == null ? 0 : tags.size() * 2];
            if (tags != null) {
                int i = 0;
                for (Map.Entry<String, String> e : tags.entrySet()) {
                    kv[i++] = e.getKey();
                    kv[i++] = e.getValue();
                }
            }
            gauge(name, r, kv);
            return r;
        });
        ref.set(value);
    }

    /** 读当前栈指标值(不依赖 Prometheus;无记录返回 null) */
    public Double stackGauge(String name, Map<String, String> tags) {
        AtomicReference<Double> ref = stack.get(stackKey(name, tags));
        return ref == null ? null : ref.get();
    }

    /* ============================================================ ⑤ 读回(给详情页/对账用) */

    /** 最近 N 次批作业(新→旧);{@code consumerId} 为空则返回全部消费方的合并视图。 */
    public List<Map<String, Object>> recentBatchJobs(String consumerId, int limit) {
        int n = limit <= 0 ? 10 : Math.min(limit, RECENT_MAX);
        List<Map<String, Object>> out = new ArrayList<>();
        if (consumerId == null || consumerId.isBlank()) {
            recentJobs.values().forEach(q -> {
                synchronized (q) {
                    out.addAll(q);
                }
            });
            out.sort((a, b) -> Long.compare(num(b.get("atMillis")), num(a.get("atMillis"))));
        } else {
            Deque<Map<String, Object>> q = recentJobs.get(consumerId);
            if (q != null) {
                synchronized (q) {
                    out.addAll(q);
                }
            }
        }
        return out.size() > n ? out.subList(0, n) : out;
    }

    /** 某消费方某结果标签的批作业累计数(从 MeterRegistry 读,与 Prometheus 同源) */
    public double batchJobCount(String consumerId, String result) {
        Collection<Counter> cs = registry.find("outlet_batch_job")
                .tag("consumer_id", consumerId).tag("result", result).counters();
        if (cs.isEmpty()) {
            return countRecent(consumerId, result);
        }
        return cs.stream().mapToDouble(Counter::count).sum();
    }

    /** 某消费方的批作业累计行数 */
    public double batchRows(String consumerId) {
        return registry.find("outlet_batch_rows").tag("consumer_id", consumerId).counters()
                .stream().mapToDouble(Counter::count).sum();
    }

    /** 某消费方 SDK 累计批次/行/事件 */
    public Map<String, Double> sdkTotals(String consumerId) {
        Map<String, Double> m = new LinkedHashMap<>();
        m.put("batches", sum("outlet_sdk_batches", consumerId));
        m.put("rows", sum("outlet_sdk_rows", consumerId));
        m.put("events", sum("outlet_sdk_events", consumerId));
        return m;
    }

    /** 批作业耗时统计(秒):count/sum/max */
    public Map<String, Double> batchDurations(String consumerId) {
        Map<String, Double> m = new LinkedHashMap<>();
        Collection<Timer> ts = registry.find("outlet_batch_duration_seconds").tag("consumer_id", consumerId).timers();
        m.put("count", ts.stream().mapToDouble(Timer::count).sum());
        m.put("sumSeconds", ts.stream().mapToDouble(t -> t.totalTime(TimeUnit.SECONDS)).sum());
        m.put("maxSeconds", ts.stream().mapToDouble(t -> t.max(TimeUnit.SECONDS)).max().orElse(0d));
        return m;
    }

    /* -------------------------------------------------------------------- 内部小事 */

    private double sum(String name, String consumerId) {
        return registry.find(name).tag("consumer_id", consumerId).counters()
                .stream().mapToDouble(Counter::count).sum();
    }

    private double countRecent(String consumerId, String result) {
        return recentBatchJobs(consumerId, RECENT_MAX).stream()
                .filter(r -> result.equals(r.get("result")))
                .count();
    }

    private void pushRecent(String consumerId, Map<String, Object> rec) {
        if (consumerId == null || consumerId.isBlank()) {
            return;
        }
        Deque<Map<String, Object>> q = recentJobs.computeIfAbsent(consumerId, k -> new ArrayDeque<>());
        synchronized (q) {
            q.addFirst(rec);
            while (q.size() > RECENT_MAX) {
                q.removeLast();
            }
        }
    }

    private Map<String, Object> jobRecord(String jobType, String streamId, String result,
                                          long rows, double seconds, String note) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("atMillis", System.currentTimeMillis());
        m.put("jobType", jobType);
        m.put("streamId", streamId);
        m.put("result", result);
        m.put("rows", rows);
        m.put("durationSeconds", Math.round(seconds * 1000d) / 1000d);
        m.put("note", note == null ? "" : note);
        return m;
    }

    private Counter counter(String name, String... tags) {
        return Counter.builder(name).tags(tags).register(registry);
    }

    private Timer timer(String name, String... tags) {
        return Timer.builder(name).tags(tags).register(registry);
    }

    private void gauge(String name, AtomicReference<Double> ref, String... tags) {
        Gauge.builder(name, ref, r -> r.get() == null ? UNAVAILABLE : r.get())
                .tags(tags)
                .strongReference(true)
                .register(registry);
    }

    private static String key(String consumerId, String streamId) {
        return consumerId + "::" + streamId;
    }

    private static String stackKey(String name, Map<String, String> tags) {
        return name + (tags == null || tags.isEmpty() ? "" : tags.toString());
    }

    private static long num(Object o) {
        return o instanceof Number n ? n.longValue() : 0L;
    }
}
