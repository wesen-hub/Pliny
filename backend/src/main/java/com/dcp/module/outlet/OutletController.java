package com.dcp.module.outlet;

import com.dcp.common.ApiResponse;
import com.dcp.module.outlet.model.ChangeBatch;
import com.dcp.module.outlet.model.SnapshotPage;
import com.dcp.module.outlet.model.StreamStatus;
import com.dcp.module.outlet.source.MockChangeStreamSource;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.web.bind.annotation.*;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 变更流出口 API(L3,设计 §6.9 六个接口)。
 *
 * <pre>
 * GET  /api/chg/{stream}/stream-status        → {epoch, earliest, latest, watermarkTs, retentionHours}
 * GET  /api/chg/{stream}/snapshot             → {boundary, rows, pageToken, done}       (快照段)
 * GET  /api/chg/{stream}/changes?after=&limit=→ {events, nextPosition, hasMore, watermarkTs} (增量段)
 * GET  /api/chg/{stream}/key/{bizKey}?asOf=   → 按主键回查(兜底,不用于常规消费)
 * GET  /api/chg/{stream}/position-at?time=    → 时间戳 → 位点
 * POST /api/chg/{stream}/commit?position=     → 上报位点(平台仅观测)
 *
 * 鉴权(骨架):请求头 X-Dcp-Token 或 query consumerId;生产改为契约 token 解析消费方身份(§7.5)。
 * 另附调试接口:POST /_mock/append(仅 mock 数据源)、GET /_debug/registry。
 * 监控:GET /consumers/{consumerId}/metrics(消费任务详情页用;现场读数,不依赖 Prometheus)。
 * </pre>
 */
@RestController
@RequestMapping("/api/chg")
@RequiredArgsConstructor
public class OutletController {

    private final ChangeOutletService service;
    private final OutletRegistry registry;
    private final ObjectProvider<MockChangeStreamSource> mockSource;
    private final com.dcp.module.outlet.batch.BatchJobService batchJobs;
    private final com.dcp.module.outlet.sdk.OutletSdkRunner sdkRunner;
    private final com.dcp.module.outlet.metrics.OutletConsumerMetricsService consumerMetrics;

    private String consumer(String token, String consumerId) {
        return (token != null && !token.isBlank()) ? token : consumerId;
    }

    @GetMapping("/{stream}/stream-status")
    public ApiResponse<StreamStatus> streamStatus(@PathVariable String stream,
                                                  @RequestHeader(value = "X-Dcp-Token", required = false) String token,
                                                  @RequestParam(required = false) String consumerId) {
        return ApiResponse.ok(service.streamStatus(consumer(token, consumerId), stream));
    }

    @GetMapping("/{stream}/snapshot")
    public ApiResponse<SnapshotPage> snapshot(@PathVariable String stream,
                                              @RequestHeader(value = "X-Dcp-Token", required = false) String token,
                                              @RequestParam(required = false) String consumerId,
                                              @RequestParam(required = false) Long boundaryTso,
                                              @RequestParam(required = false) String pageToken,
                                              @RequestParam(defaultValue = "1000") int limit) {
        return ApiResponse.ok(service.snapshot(consumer(token, consumerId), stream, boundaryTso, pageToken, limit));
    }

    /** 估算快照行数(v2 批阶段切片/执行体选择用;未知返回 -1)。 */
    @GetMapping("/{stream}/count")
    public ApiResponse<Long> count(@PathVariable String stream,
                                   @RequestHeader(value = "X-Dcp-Token", required = false) String token,
                                   @RequestParam(required = false) String consumerId,
                                   @RequestParam(required = false) Long boundaryTso) {
        return ApiResponse.ok(service.estimateRows(consumer(token, consumerId), stream, boundaryTso));
    }

    @GetMapping("/{stream}/changes")
    public ApiResponse<ChangeBatch> changes(@PathVariable String stream,
                                            @RequestHeader(value = "X-Dcp-Token", required = false) String token,
                                            @RequestParam(required = false) String consumerId,
                                            @RequestParam(required = false) String after,
                                            @RequestParam(defaultValue = "500") int limit) {
        return ApiResponse.ok(service.changes(consumer(token, consumerId), stream, after, limit));
    }

    @GetMapping("/{stream}/key/{bizKey}")
    public ApiResponse<Map<String, Object>> key(@PathVariable String stream,
                                                @PathVariable String bizKey,
                                                @RequestHeader(value = "X-Dcp-Token", required = false) String token,
                                                @RequestParam(required = false) String consumerId,
                                                @RequestParam(required = false) Long asOf) {
        Map<String, Object> k = new LinkedHashMap<>();
        k.put("id", bizKey);
        return ApiResponse.ok(service.keyLookup(consumer(token, consumerId), stream, k, asOf));
    }

    @GetMapping("/{stream}/position-at")
    public ApiResponse<Map<String, Object>> positionAt(@PathVariable String stream,
                                                       @RequestHeader(value = "X-Dcp-Token", required = false) String token,
                                                       @RequestParam(required = false) String consumerId,
                                                       @RequestParam long time) {
        return ApiResponse.ok(service.positionAt(consumer(token, consumerId), stream, time));
    }

    @PostMapping("/{stream}/commit")
    public ApiResponse<Map<String, Object>> commit(@PathVariable String stream,
                                                   @RequestHeader(value = "X-Dcp-Token", required = false) String token,
                                                   @RequestParam(required = false) String consumerId,
                                                   @RequestParam String position) {
        return ApiResponse.ok(service.commit(consumer(token, consumerId), stream, position));
    }

    /* ---------------------------------------------------------- 调试接口(仅 mock 数据源) */

    /**
     * 模拟一次数据面变更(仅当 {@code dcp.outlet.source=mock} 时可用),
     * 用于在没有 TiDB 的环境里验证"写 → 事件 → 消费"链路。
     */
    @PostMapping("/{stream}/_mock/append")
    public ApiResponse<Map<String, Object>> mockAppend(@PathVariable String stream,
                                                       @RequestParam(defaultValue = "INSERT") String op,
                                                       @RequestBody Map<String, Object> row) {
        MockChangeStreamSource src = mockSource.getIfAvailable();
        if (src == null) {
            return ApiResponse.error(OutletErrorCode.OUTLET_INTERNAL, "当前数据源非 mock,不可模拟写入");
        }
        var e = src.append(stream, op.toUpperCase(), row, null);
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("tso", e.getTso());
        m.put("op", e.getOp());
        m.put("row", e.getRow());
        return ApiResponse.ok(m);
    }

    /** 查看已注册消费方/契约(调试) */
    @GetMapping("/_debug/registry")
    public ApiResponse<Map<String, Object>> registry() {
        return ApiResponse.ok(registry.snapshotForDebug());
    }

    /** 查看平台侧位点观测(调试) */
    @GetMapping("/{stream}/_debug/seen")
    public ApiResponse<Map<String, Object>> seen(@PathVariable String stream,
                                                 @RequestParam String consumerId) {
        return ApiResponse.ok(service.seen(consumerId, stream));
    }

    /* ------------------------------------------------------------------ 监控(消费任务详情页) */

    /**
     * 按消费方读出口指标(2026-09-14 出口监控)。
     *
     * <p>返回:当前位点/水位/滞后秒/是否追平、最近 N 次批作业(类型/行数/耗时/结果)、
     * 快照任务存量、当前告警判定(命中 warn/crit 阈值的项)。
     * <b>所有值现场读(位点表 + MeterRegistry + 采集器快照),不依赖 Prometheus</b>;
     * Prometheus 只负责时序与告警通知。
     *
     * @param recent 最近批作业条数(≤50)
     */
    @GetMapping("/consumers/{consumerId}/metrics")
    public ApiResponse<Map<String, Object>> consumerMetrics(@PathVariable String consumerId,
                                                            @RequestParam(defaultValue = "10") int recent) {
        return ApiResponse.ok(consumerMetrics.forConsumer(consumerId, recent));
    }

    /**
     * 手动触发一次批作业(执行器 B 的调试入口;生产应由调度器触发)。
     * <p>用途:在没有 Spark/调度器的情况下，用 {@code LocalBatchExecutor} 跑通
     * 「定边界 → 校验 → 执行 + 原子发布 → 位点收口」四步，验证批通道语义。
     */
    @PostMapping("/_debug/batch-run")
    public ApiResponse<com.dcp.module.outlet.batch.BatchJobOutcome> batchRun(
            @RequestParam String consumerId,
            @RequestParam String stream,
            @RequestParam(defaultValue = "bootstrap") String jobType,
            @RequestParam(required = false) String target,
            @RequestParam(required = false) String transform,
            @RequestParam(required = false) Long boundaryTso,
            @RequestParam(required = false) Long hiTso) {
        var spec = new com.dcp.module.outlet.batch.BatchJobSpec();
        spec.setConsumerId(consumerId);
        spec.setStreamId(stream);
        spec.setJobType(jobType);
        spec.setTarget(target == null ? "staging://batch-debug/" + stream : target);
        spec.setTransform(transform);
        spec.setBoundaryTso(boundaryTso);
        spec.setHiTso(hiTso);
        return ApiResponse.ok(batchJobs == null
                ? null : batchJobs.run(spec));
    }

    /**
     * 手动触发一次「真跑 SDK」消费(demo 消费方进程)。
     * <p>用途:把消费方 SDK 从单测推到真跑——按 SDK 与真实消费方完全一致的方式
     *（open → poll → commitWithBusinessWrite)消费真实出口,位点落元库 consumer_offset,
     * 可跨进程/跨重启续跑;返回批次数、处理行/事件数、位点、是否追平。
     */
    @PostMapping("/_debug/sdk-run")
    public ApiResponse<Map<String, Object>> sdkRun(
            @RequestParam String consumerId,
            @RequestParam List<String> streams,
            @RequestParam(defaultValue = "SNAPSHOT") String startPolicy,
            @RequestParam(defaultValue = "20") int maxBatches,
            @RequestParam(defaultValue = "15000") long maxMillis) {
        return ApiResponse.ok(sdkRunner.runOnce(consumerId, streams, startPolicy, maxBatches, maxMillis));
    }
}
