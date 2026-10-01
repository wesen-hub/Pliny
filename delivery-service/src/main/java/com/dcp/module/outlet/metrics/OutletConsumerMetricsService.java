package com.dcp.module.outlet.metrics;

import com.dcp.common.BizException;
import com.dcp.module.outlet.OutletErrorCode;
import com.dcp.module.outlet.OutletRegistry;
import com.dcp.module.outlet.SnapshotJobService;
import com.dcp.module.outlet.model.Contract;
import com.dcp.module.outlet.source.ChangeStreamSource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 按消费方聚合出口指标(2026-09-14):给**消费任务详情页**用的一站式读数。
 *
 * <p>硬约束:**不依赖 Prometheus**。所有"当前值"都现场读 ——
 * 位点读元库 {@code consumer_offset}、水位读 {@link ChangeStreamSource}、快照任务读
 * {@link SnapshotJobService}、批作业读 {@link OutletMetrics} 的环形缓冲 + MeterRegistry。
 * Prometheus 只负责时序与告警通知,不是本接口的数据来源。
 *
 * <p>返回结构(见部署记录 §18 的样例 JSON):
 * <pre>
 * consumerId / contractId / streams[]                    身份与授权范围
 * streams[]: streamId, epoch, positionTso, watermarkTso, lagSeconds, caughtUp, lastUpdatedAt
 * caughtUp: 全部流是否都追平(位点滞后 ≤ 0)
 * batch:   { jobs{ok,reuse,fail,noop}, rowsTotal, durations{count,sumSeconds,maxSeconds}, recent[] }
 * sdk:     { batches, rows, events }
 * snapshotJobs: { PAGING, DONE, EXPIRED, ... }            当前存量(按状态)
 * alerts / alertLevel / stackAlerts                       当前阈值命中项(CONSUMER / STACK 两级)
 * collectedAtMillis                                       快照时间
 * </pre>
 */
@Slf4j
@Service
public class OutletConsumerMetricsService {

    private final OutletRegistry registry;
    private final JdbcTemplate metaJdbc;
    private final ObjectProvider<ChangeStreamSource> sourceProvider;
    private final SnapshotJobService snapshotJobs;
    private final OutletMetrics metrics;
    private final OutletAlertEvaluator alerts;
    private final ObjectProvider<OutletStackCollector> collectorProvider;

    public OutletConsumerMetricsService(OutletRegistry registry,
                                        ObjectProvider<JdbcTemplate> metaJdbcProvider,
                                        ObjectProvider<ChangeStreamSource> sourceProvider,
                                        SnapshotJobService snapshotJobs,
                                        OutletMetrics metrics,
                                        OutletAlertEvaluator alerts,
                                        ObjectProvider<OutletStackCollector> collectorProvider) {
        this.registry = registry;
        this.metaJdbc = metaJdbcProvider.getIfAvailable();
        this.sourceProvider = sourceProvider;
        this.snapshotJobs = snapshotJobs;
        this.metrics = metrics;
        this.alerts = alerts;
        this.collectorProvider = collectorProvider;
    }

    public Map<String, Object> forConsumer(String consumerId, int recentLimit) {
        if (consumerId == null || consumerId.isBlank()) {
            throw new BizException(com.dcp.common.ErrorCode.PARAM_ERROR, "consumerId 必填");
        }
        if (registry.consumer(consumerId) == null) {
            throw new BizException(OutletErrorCode.CONTRACT_FORBIDDEN, "消费方不存在:" + consumerId);
        }
        Contract contract = registry.activeContractOf(consumerId);
        ChangeStreamSource source = sourceProvider.getIfAvailable();

        Map<String, Object> out = new LinkedHashMap<>();
        out.put("consumerId", consumerId);
        out.put("contractId", contract == null ? null : contract.getContractId());
        out.put("contractStreams", contract == null ? List.of() : new ArrayList<>(contract.getStreams()));

        /* ① 位点 / 水位 / 滞后 / 是否追平 */
        List<Map<String, Object>> lags = new ArrayList<>();
        Set<String> streams = new LinkedHashSet<>();
        if (contract != null && contract.getStreams() != null) {
            streams.addAll(contract.getStreams());
        }
        Map<String, Map<String, Object>> offsets = loadOffsets(consumerId);
        streams.addAll(offsets.keySet());

        for (String streamId : streams) {
            Map<String, Object> off = offsets.get(streamId);
            Map<String, Object> item = new LinkedHashMap<>();
            item.put("streamId", streamId);
            item.put("epoch", off == null ? null : off.get("epoch"));
            item.put("positionTso", off == null ? null : off.get("positionTso"));
            item.put("lastUpdatedAt", off == null ? null : off.get("updatedAt"));
            Long watermark = null;
            if (source != null) {
                try {
                    watermark = source.watermark(streamId);
                } catch (RuntimeException e) {
                    log.warn("[outlet-metrics] 读水位失败 consumer={} stream={} err={}", consumerId, streamId, e.toString());
                }
            }
            item.put("watermarkTso", watermark);
            Double lag = null;
            if (off != null && watermark != null && watermark > 0) {
                long position = toLong(off.get("positionTso"));
                lag = Math.max(0d, Math.round((watermark - position) / OutletMetrics.TSO_TICKS_PER_SECOND * 10d) / 10d);
            }
            item.put("lagSeconds", lag == null ? OutletMetrics.UNAVAILABLE : lag);
            item.put("caughtUp", lag != null && lag <= 0d);
            lags.add(item);
        }
        out.put("streams", lags);
        out.put("caughtUp", !lags.isEmpty() && lags.stream().allMatch(s -> Boolean.TRUE.equals(s.get("caughtUp"))));

        /* ② 批作业(累计 + 最近 N 次) */
        List<Map<String, Object>> recent = metrics.recentBatchJobs(consumerId, recentLimit);
        Map<String, Object> batch = new LinkedHashMap<>();
        Map<String, Object> jobCounts = new LinkedHashMap<>();
        jobCounts.put("ok", metrics.batchJobCount(consumerId, OutletMetrics.RESULT_OK));
        jobCounts.put("reuse", metrics.batchJobCount(consumerId, OutletMetrics.RESULT_REUSED));
        jobCounts.put("fail", metrics.batchJobCount(consumerId, OutletMetrics.RESULT_FAIL));
        jobCounts.put("noop", metrics.batchJobCount(consumerId, OutletMetrics.RESULT_NOOP));
        batch.put("jobs", jobCounts);
        batch.put("rowsTotal", metrics.batchRows(consumerId));
        batch.put("durations", metrics.batchDurations(consumerId));
        batch.put("recent", recent);
        out.put("batch", batch);

        /* ③ 消费方 SDK */
        out.put("sdk", metrics.sdkTotals(consumerId));

        /* ④ 快照任务(当前存量,按状态) */
        out.put("snapshotJobs", snapshotJobs.countsByStatus(consumerId));

        /* ⑤ 告警判定(消费方级 + 上游栈级) */
        List<Map<String, Object>> consumerAlerts = alerts.evaluateConsumer(lags, recent);
        OutletStackCollector collector = collectorProvider.getIfAvailable();
        Map<String, Object> stackSnapshot = collector == null ? Map.of() : collector.lastSnapshot();
        List<Map<String, Object>> stackAlerts = alerts.evaluateStack(stackSnapshot);
        out.put("alerts", consumerAlerts);
        out.put("alertLevel", alerts.worst(consumerAlerts));
        out.put("stackAlerts", stackAlerts);
        out.put("stackAlertLevel", alerts.worst(stackAlerts));
        out.put("stackCollectedAtMillis", stackSnapshot.get("collectedAtMillis"));
        out.put("thresholds", Map.of(
                "lagWarnSeconds", alerts.monitor().getLagWarnSeconds(),
                "lagCritSeconds", alerts.monitor().getLagCritSeconds(),
                "gcWarnSeconds", alerts.monitor().getGcWarnSeconds()));
        out.put("collectedAtMillis", System.currentTimeMillis());
        out.put("note", "当前值现场读(不依赖 Prometheus);Prometheus 只负责时序与告警通知");
        return out;
    }

    /** 位点:读元库 consumer_offset(演示/内部消费方;生产位点在消费方自己的库,此处为观测口径) */
    private Map<String, Map<String, Object>> loadOffsets(String consumerId) {
        Map<String, Map<String, Object>> m = new LinkedHashMap<>();
        if (metaJdbc == null) {
            return m;
        }
        try {
            List<Map<String, Object>> rows = metaJdbc.queryForList(
                    "SELECT stream_id, epoch, position_tso, updated_at FROM consumer_offset WHERE consumer_id = ?",
                    consumerId);
            for (Map<String, Object> r : rows) {
                Map<String, Object> v = new LinkedHashMap<>();
                v.put("epoch", toLong(r.get("epoch")));
                v.put("positionTso", toLong(r.get("position_tso")));
                v.put("updatedAt", r.get("updated_at") == null ? null : String.valueOf(r.get("updated_at")));
                m.put(String.valueOf(r.get("stream_id")), v);
            }
        } catch (RuntimeException e) {
            log.warn("[outlet-metrics] 读 consumer_offset 失败 consumer={} err={}", consumerId, e.toString());
        }
        return m;
    }

    private static long toLong(Object o) {
        if (o == null) {
            return 0L;
        }
        if (o instanceof Number n) {
            return n.longValue();
        }
        try {
            return Long.parseLong(String.valueOf(o));
        } catch (NumberFormatException e) {
            return 0L;
        }
    }
}
