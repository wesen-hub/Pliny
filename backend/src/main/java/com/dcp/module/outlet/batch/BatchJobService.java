package com.dcp.module.outlet.batch;

import com.dcp.common.BizException;
import com.dcp.common.ErrorCode;
import com.dcp.module.outlet.OutletErrorCode;
import com.dcp.module.outlet.entity.SnapshotManifestEntity;
import com.dcp.module.outlet.model.StreamStatus;
import com.dcp.module.outlet.sdk.ConsumerOffsetStore;
import com.dcp.module.outlet.sdk.OutletApi;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

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
@RequiredArgsConstructor
public class BatchJobService {

    private final OutletApi api;
    private final BatchExecutor executor;
    private final ManifestService manifests;
    private final ConsumerOffsetStore offsets;

    public BatchJobOutcome run(BatchJobSpec spec) {
        List<String> errs = spec.validate();
        if (!errs.isEmpty()) {
            throw new BizException(ErrorCode.PARAM_ERROR.getCode(), String.join("; ", errs));
        }
        StreamStatus st = api.streamStatus(spec.getStreamId());
        return switch (spec.getJobType()) {
            case SnapshotManifestEntity.TYPE_BOOTSTRAP -> bootstrap(spec, st);
            case SnapshotManifestEntity.TYPE_INCR_BATCH -> incr(spec, st);
            case SnapshotManifestEntity.TYPE_MIXED -> {
                // mixed 语义:首次(bootstrap 未打过底)先 bootstrap,之后一律走增量批接力
                long[] saved = offsets.load(spec.getConsumerId(), spec.getStreamId());
                if (saved == null) {
                    BatchJobOutcome boot = bootstrap(spec, st);
                    BatchJobOutcome inc = incr(spec, api.streamStatus(spec.getStreamId()));
                    if (!inc.isPublished() && boot.isPublished()) {
                        boot.setNote("mixed: bootstrap " + (boot.isReused() ? "复用" : "完成") + ";" + inc.getNote());
                        yield boot;
                    }
                    yield inc;
                }
                yield incr(spec, st);
            }
            default -> throw new BizException(ErrorCode.PARAM_ERROR.getCode(), "jobType 非法:" + spec.getJobType());
        };
    }

    /* ------------------------------------------------------------------ bootstrap */

    private BatchJobOutcome bootstrap(BatchJobSpec spec, StreamStatus st) {
        long t0 = spec.getBoundaryTso() != null ? spec.getBoundaryTso() : st.getLatest();
        validateBoundary(st, t0);

        SnapshotManifestEntity exist = manifests.findPublished(spec.getConsumerId(), spec.getStreamId(),
                SnapshotManifestEntity.TYPE_BOOTSTRAP, t0, null, null);
        if (exist != null) {
            offsets.save(spec.getConsumerId(), spec.getStreamId(), st.getEpoch(), t0);
            log.info("[outlet-batch] bootstrap 复用已发布产物 staging={}(重跑幂等)", exist.getStagingPath());
            return new BatchJobOutcome(exist.getManifestId(), exist.getJobType(), t0, null, null,
                    exist.getStagingPath(), exist.getRowsWritten() == null ? 0 : exist.getRowsWritten(),
                    true, true, t0, "复用已发布产物(未重复搬数)");
        }

        SnapshotManifestEntity m = manifests.start(spec.getConsumerId(), spec.getStreamId(),
                SnapshotManifestEntity.TYPE_BOOTSTRAP, t0, null, null, st.getEpoch());
        ExecutionResult r;
        try {
            r = executor.bootstrap(spec, t0, st.getEpoch());
            manifests.publish(m, r);
        } catch (RuntimeException e) {
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
            offsets.save(spec.getConsumerId(), spec.getStreamId(), st.getEpoch(), hi);
            return new BatchJobOutcome(exist.getManifestId(), exist.getJobType(), null, lo, hi,
                    exist.getStagingPath(), exist.getRowsWritten() == null ? 0 : exist.getRowsWritten(),
                    true, true, hi, "复用已发布产物(未重复搬数)");
        }

        SnapshotManifestEntity m = manifests.start(spec.getConsumerId(), spec.getStreamId(),
                SnapshotManifestEntity.TYPE_INCR_BATCH, null, lo, hi, st.getEpoch());
        ExecutionResult r;
        try {
            r = executor.incrBatch(spec, lo, hi, st.getEpoch());
            manifests.publish(m, r);
        } catch (RuntimeException e) {
            manifests.fail(m, String.valueOf(e.getMessage()));
            throw e;
        }
        offsets.save(spec.getConsumerId(), spec.getStreamId(), st.getEpoch(), hi);   // ★ 发布之后
        return new BatchJobOutcome(m.getManifestId(), m.getJobType(), null, lo, hi,
                r.getStagingPath(), r.getRowsWritten(), true, false, hi, "incr_batch 完成");
    }

    /* ------------------------------------------------------------------ helpers */

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
