package com.dcp.module.outlet.batch;

import com.dcp.common.BizException;
import com.dcp.common.ErrorCode;
import com.dcp.module.outlet.OutletErrorCode;
import com.dcp.module.outlet.entity.SnapshotManifestEntity;
import com.dcp.module.outlet.metrics.OutletMetrics;
import com.dcp.module.outlet.model.StreamStatus;
import com.dcp.module.outlet.sdk.ConsumerOffsetStore;
import com.dcp.module.outlet.sdk.OutletApi;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;

/**
 * 执行器 B 的作业编排(设计 §6.3):**消费方只声明,平台代跑**。
 *
 * <p>四步固定流程(对应 §6.3 的模板):
 * <pre>
 * ① 定边界: bootstrap 取 T0;incr_batch 取 [lo,hi](lo = 消费方位点,hi = WATERMARK)
 * ② 边界校验: T0 ≥ earliest(GC 可读起点)、≤ latest(§6.4.2 三规则)
 * ③ 执行 + **原子发布**(产物先写不可变 staging,再置 manifest PUBLISHED)
 * ④ **位点收口**: 发布之后才提交位点(位点推进 ⇔ 数据已可见)
 * </pre>
 *
 * <p>幂等:同一 (consumer, stream, jobType, 边界/区间) 已发布 → 直接复用产物并保证位点收口,
 * **不重复搬数**(设计 §6.4.1 衔接契约 4:重跑必须幂等)。
 */
@Slf4j
public class BatchJobService {

    private final OutletApi api;
    private final BatchExecutor executor;
    private final ManifestService manifests;
    private final ConsumerOffsetStore offsets;
    /** 指标登记(可为 null:单测/无监控场景 → 只跳过埋点,不影响语义) */
    private final OutletMetrics metrics;

    public BatchJobService(OutletApi api, BatchExecutor executor, ManifestService manifests,
                           ConsumerOffsetStore offsets) {
        this(api, executor, manifests, offsets, null);
    }

    @Autowired
    public BatchJobService(OutletApi api, BatchExecutor executor, ManifestService manifests,
                           ConsumerOffsetStore offsets, OutletMetrics metrics) {
        this.api = api;
        this.executor = executor;
        this.manifests = manifests;
        this.offsets = offsets;
        this.metrics = metrics;
    }

    public BatchJobOutcome run(BatchJobSpec spec) {
        long startedAt = System.nanoTime();
        List<String> errs = spec.validate();
        if (!errs.isEmpty()) {
            record(spec, OutletMetrics.RESULT_FAIL, 0, startedAt, "参数校验失败:" + String.join("; ", errs));
            throw new BizException(ErrorCode.PARAM_ERROR.getCode(), String.join("; ", errs));
        }
        try {
            StreamStatus st = api.streamStatus(spec.getStreamId());
            BatchJobOutcome out = switch (spec.getJobType()) {
                case SnapshotManifestEntity.TYPE_BOOTSTRAP -> bootstrap(spec, st);
                case SnapshotManifestEntity.TYPE_INCR_BATCH -> incr(spec, st);
                case SnapshotManifestEntity.TYPE_MIXED -> mixed(spec, st);
                default -> throw new BizException(ErrorCode.PARAM_ERROR.getCode(), "jobType 非法:" + spec.getJobType());
            };
            record(spec, classify(out), out == null ? 0 : out.getRowsWritten(), startedAt,
                    out == null ? "无返回" : out.getNote());
            return out;
        } catch (RuntimeException e) {
            record(spec, OutletMetrics.RESULT_FAIL, 0, startedAt, String.valueOf(e.getMessage()));
            throw e;
        }
    }

    /**
     * 结果分类(口径写入文档 §18):
     * <ul>
     *   <li>{@code ok} —— 真执行并发布(staging 已写、manifest PUBLISHED);</li>
     *   <li>{@code reuse} —— 命中幂等复用(产物已在,**未重复搬数**);</li>
     *   <li>{@code noop} —— 空转(无新数据 lo ≥ hi / mixed 短路),不是失败;</li>
     *   <li>{@code fail} —— 异常或未发布(位点不推进)。</li>
     * </ul>
     */
    static String classify(BatchJobOutcome out) {
        if (out == null) {
            return OutletMetrics.RESULT_FAIL;
        }
        if (out.isReused()) {
            return OutletMetrics.RESULT_REUSED;
        }
        if (out.isPublished()) {
            return OutletMetrics.RESULT_OK;
        }
        return out.getRowsWritten() > 0 ? OutletMetrics.RESULT_FAIL : OutletMetrics.RESULT_NOOP;
    }

    private void record(BatchJobSpec spec, String result, long rows, long startedAt, String note) {
        if (metrics == null) {
            return;
        }
        double seconds = (System.nanoTime() - startedAt) / 1_000_000_000d;
        metrics.recordBatchJob(spec.getConsumerId(), spec.getStreamId(), spec.getJobType(), result, rows, seconds, note);
    }

    private BatchJobOutcome mixed(BatchJobSpec spec, StreamStatus st) {
        // mixed 语义:首次(bootstrap 未打过底)先 bootstrap,之后一律走增量批接力
        long[] saved = offsets.load(spec.getConsumerId(), spec.getStreamId());
        if (saved == null) {
            BatchJobOutcome boot = bootstrap(spec, st);
            BatchJobOutcome inc = incr(spec, api.streamStatus(spec.getStreamId()));
            if (!inc.isPublished() && boot.isPublished()) {
                boot.setNote("mixed: bootstrap " + (boot.isReused() ? "复用" : "完成") + ";" + inc.getNote());
                return boot;
            }
            return inc;
        }
        return incr(spec, st);
    }

    /* ------------------------------------------------------------------ bootstrap */

    private BatchJobOutcome bootstrap(BatchJobSpec spec, StreamStatus st) {
        long t0 = spec.getBoundaryTso() != null ? spec.getBoundaryTso() : st.getLatest();
        validateBoundary(st, t0);

        SnapshotManifestEntity exist = manifests.findPublished(spec.getConsumerId(), spec.getStreamId(),
                SnapshotManifestEntity.TYPE_BOOTSTRAP, t0, null, null);
        if (exist != null) {
            // ★ 幂等复用:产物已在,只**保证位点不落后**,绝不回退
            //   (实测坑:bootstrap 的位点=T0,而复用前消费方可能已通过增量批跑到更大的位点)
            saveOffsetIfAhead(spec.getConsumerId(), spec.getStreamId(), st.getEpoch(), t0);
            log.info("[outlet-batch] bootstrap 复用已发布产物 staging={}(重跑幂等)", exist.getStagingPath());
            return new BatchJobOutcome(exist.getManifestId(), exist.getJobType(), t0, null, null,
                    exist.getStagingPath(), exist.getRowsWritten() == null ? 0 : exist.getRowsWritten(),
                    true, true, readOffset(spec.getConsumerId(), spec.getStreamId()), "复用已发布产物(未重复搬数)");
        }

        SnapshotManifestEntity m = manifests.start(spec.getConsumerId(), spec.getStreamId(),
                SnapshotManifestEntity.TYPE_BOOTSTRAP, t0, null, null, st.getEpoch());
        ExecutionResult r;
        try {
            r = executor.bootstrap(spec, t0, st.getEpoch());
            manifests.publish(m, r);
        } catch (RuntimeException e) {
            log.error("[outlet-batch] bootstrap 作业失败 id={} err={}", m.getManifestId(), e.getMessage(), e);
            manifests.fail(m, String.valueOf(e.getMessage()));
            throw e;
        }
        // ★ 位点收口:发布之后才提交(位点推进 ⇔ 数据已可见)
        offsets.save(spec.getConsumerId(), spec.getStreamId(), st.getEpoch(), t0);
        return new BatchJobOutcome(m.getManifestId(), m.getJobType(), t0, null, null,
                r.getStagingPath(), r.getRowsWritten(), true, false, t0, "bootstrap 完成");
    }

    /* ------------------------------------------------------------------ incr_batch */

    private BatchJobOutcome incr(BatchJobSpec spec, StreamStatus st) {
        long[] saved = offsets.load(spec.getConsumerId(), spec.getStreamId());
        long lo = saved != null ? saved[1] : st.getEarliest();
        if (lo < st.getEarliest()) {
            throw new BizException(OutletErrorCode.POSITION_EXPIRED,
                    "位点 " + lo + " 早于 earliest " + st.getEarliest() + " → 需重做 bootstrap");
        }
        long hi = spec.getHiTso() != null ? spec.getHiTso()
                : (st.getWatermarkTs() != null ? st.getWatermarkTs() : st.getLatest());
        if (hi > st.getLatest()) {
            throw new BizException(OutletErrorCode.BOUNDARY_IN_FUTURE, "hi=" + hi + " > latest=" + st.getLatest());
        }
        if (lo >= hi) {
            return new BatchJobOutcome(null, SnapshotManifestEntity.TYPE_INCR_BATCH, null, lo, hi,
                    null, 0, false, false, lo, "无新数据(lo ≥ hi)");
        }

        SnapshotManifestEntity exist = manifests.findPublished(spec.getConsumerId(), spec.getStreamId(),
                SnapshotManifestEntity.TYPE_INCR_BATCH, null, lo, hi);
        if (exist != null) {
            saveOffsetIfAhead(spec.getConsumerId(), spec.getStreamId(), st.getEpoch(), hi);
            return new BatchJobOutcome(exist.getManifestId(), exist.getJobType(), null, lo, hi,
                    exist.getStagingPath(), exist.getRowsWritten() == null ? 0 : exist.getRowsWritten(),
                    true, true, readOffset(spec.getConsumerId(), spec.getStreamId()), "复用已发布产物(未重复搬数)");
        }

        SnapshotManifestEntity m = manifests.start(spec.getConsumerId(), spec.getStreamId(),
                SnapshotManifestEntity.TYPE_INCR_BATCH, null, lo, hi, st.getEpoch());
        ExecutionResult r;
        try {
            r = executor.incrBatch(spec, lo, hi, st.getEpoch());
            manifests.publish(m, r);
        } catch (RuntimeException e) {
            log.error("[outlet-batch] incr_batch 作业失败 id={} err={}", m.getManifestId(), e.getMessage(), e);
            manifests.fail(m, String.valueOf(e.getMessage()));
            throw e;
        }
        offsets.save(spec.getConsumerId(), spec.getStreamId(), st.getEpoch(), hi);   // ★ 发布之后
        return new BatchJobOutcome(m.getManifestId(), m.getJobType(), null, lo, hi,
                r.getStagingPath(), r.getRowsWritten(), true, false, hi, "incr_batch 完成");
    }

    /* ------------------------------------------------------------------ helpers */

    /** 位点只前不退:已有位点更大(或 epoch 不同步)时不动(实测坑:bootstrap 复用会把 hi 回退成 T0) */
    private void saveOffsetIfAhead(String consumerId, String streamId, int epoch, long tso) {
        long[] cur = offsets.load(consumerId, streamId);
        if (cur == null || tso > cur[1]) {
            offsets.save(consumerId, streamId, epoch, tso);
        }
    }

    private long readOffset(String consumerId, String streamId) {
        long[] cur = offsets.load(consumerId, streamId);
        return cur == null ? 0 : cur[1];
    }

    /** 边界三规则(设计 §6.4.2) */
    private void validateBoundary(StreamStatus st, long t0) {
        if (t0 < st.getEarliest()) {
            throw new BizException(OutletErrorCode.BOUNDARY_BELOW_STREAM_START,
                    "T0=" + t0 + " < earliest=" + st.getEarliest() + "(该时点不在流覆盖范围内)");
        }
        if (t0 > st.getLatest()) {
            throw new BizException(OutletErrorCode.BOUNDARY_IN_FUTURE,
                    "T0=" + t0 + " > latest=" + st.getLatest());
        }
    }
}
