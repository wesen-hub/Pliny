package com.dcp.module.outlet;

import com.dcp.common.BizException;
import com.dcp.module.outlet.model.*;
import com.dcp.module.outlet.source.ChangeStreamSource;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 出口服务(设计 §5.5 出口职责 / §6 统一消费模型)。
 *
 * <p>一句话职责:把 L1 内部流,按**契约 + 权限 + 位点**变成对消费方可见的
 * "一条带位点的流":快照段与增量段对消费方是同一条流,差别只在内部路由。
 *
 * <p>本类**不含**任何业务表直读:所有数据都经 {@link ChangeStreamSource} 拿;
 * 所有下发都过 {@link OutletPermissionEngine};所有配额都过 {@link OutletQuotaLimiter};
 * 每次读都留 {@link OutletAuditService} 轨迹。
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class ChangeOutletService {

    private final OutletRegistry registry;
    private final OutletPermissionEngine permission;
    private final OutletQuotaLimiter quota;
    private final OutletAuditService audit;
    private final ChangeStreamSource source;

    /**
     * 平台侧位点观测(设计 §6.8:consumer_offset_seen)。
     * <b>仅监控/审计用,不作为交付依据</b>;权威位点在消费方自己的 consumer_offset 表里。
     */
    private final Map<String, long[]> seenOffset = new ConcurrentHashMap<>();

    /* ============================================================== 接口 ① stream-status */

    public StreamStatus streamStatus(String consumerId, String streamId) {
        Ctx ctx = context(consumerId);
        permission.checkStream(ctx.consumer, ctx.contract, streamId);
        return source.status(streamId);
    }

    /* ==================================================================== 接口 ② snapshot */

    /**
     * 快照分页。{@code boundaryTso} 为空表示"由平台发放新边界"(= 当前 lastest)。
     * 发放前强制校验边界(设计 §6.4.2):不合法**直接报错**,绝不静默从当前水位开始。
     */
    public SnapshotPage snapshot(String consumerId, String streamId, Long boundaryTso, String pageToken, int limit) {
        Ctx ctx = context(consumerId);
        permission.checkStream(ctx.consumer, ctx.contract, streamId);
        quota.acquire(ctx.consumer);
        try {
            StreamStatus st = source.status(streamId);
            long t0 = boundaryTso == null ? st.getLatest() : boundaryTso;
            validateBoundary(st, t0);

            SnapshotPage page = source.snapshot(streamId, t0, st.getEpoch(), pageToken, limit);
            OutletPermissionEngine.ProjectionPlan plan =
                    permission.plan(ctx.consumer, ctx.contract, streamId, source.availableColumns(streamId));
            // ★ 铁律 3:快照段与增量段**同一套判定**。
            //   增量段对不满足行条件的行发 DELETE 墓碑(消费方可能已有该行);
            //   快照段则直接**不返回**这些行(消费方还没有)。两者终态一致。
            page.setRows(page.getRows().stream()
                    .filter(plan.rowFilter)
                    .map(r -> permission.projectRow(plan, r))
                    .toList());
            // 说明:真实实现必须在**发第一页时就登记 snapshot_job(boundary + TTL)**,
            //       否则 GC 推进后同一 boundary 不可再续(见 DDL 的 snapshot_job 表)。
            quota.addRows(ctx.consumer, page.getRows().size());
            audit.recordRead(consumerId, ctx.contract.getContractId(), streamId,
                    t0, t0, page.getRows().size(), plan.trimmedColumns, plan.maskedColumns);
            return page;
        } finally {
            quota.release(ctx.consumer);
        }
    }

    /* ===================================================================== 接口 ③ changes */

    /**
     * 增量拉取(补齐段与常驻段共用同一接口)。
     *
     * @param afterPosition 不透明位点({@code v1.{epoch}.{tso}});为空则从 earliest 开始
     */
    public ChangeBatch changes(String consumerId, String streamId, String afterPosition, int limit) {
        Ctx ctx = context(consumerId);
        permission.checkStream(ctx.consumer, ctx.contract, streamId);
        quota.acquire(ctx.consumer);
        try {
            StreamStatus st = source.status(streamId);
            PositionKey pos = parsePosition(afterPosition, st);
            if (pos.getEpoch() != st.getEpoch()) {
                // 流被重建(或 topic 重建)→ 老位点即使数值有效也不可信
                throw new BizException(OutletErrorCode.EPOCH_MISMATCH,
                        "位点 epoch=" + pos.getEpoch() + ", 当前 epoch=" + st.getEpoch());
            }
            if (pos.getTso() < st.getEarliest()) {
                throw new BizException(OutletErrorCode.POSITION_EXPIRED,
                        "位点 " + pos.getTso() + " 早于 earliest " + st.getEarliest() + ", 请重做快照");
            }

            ChangeBatch batch = source.changes(streamId, pos.getTso(), st.getEpoch(), limit);
            OutletPermissionEngine.ProjectionPlan plan =
                    permission.plan(ctx.consumer, ctx.contract, streamId, source.availableColumns(streamId));
            batch.setEvents(batch.getEvents().stream().map(e -> permission.projectEvent(plan, e)).toList());

            seenOffset.put(key(consumerId, streamId), new long[]{st.getEpoch(), batch.getScannedTo()});
            quota.addRows(ctx.consumer, batch.getEvents().size());
            audit.recordRead(consumerId, ctx.contract.getContractId(), streamId,
                    batch.getScannedFrom(), batch.getScannedTo(), batch.getEvents().size(),
                    plan.trimmedColumns, plan.maskedColumns);
            return batch;
        } finally {
            quota.release(ctx.consumer);
        }
    }

    /* ================================================================= 接口 ④ key?asOf= */

    /** 按主键回查(**仅兜底对账/补漏**;常规消费不回表 —— 设计 §6.3) */
    public Map<String, Object> keyLookup(String consumerId, String streamId, Map<String, Object> bizKey, Long asOfTso) {
        Ctx ctx = context(consumerId);
        permission.checkStream(ctx.consumer, ctx.contract, streamId);
        quota.acquire(ctx.consumer);
        try {
            Map<String, Object> row = source.keyLookup(streamId, bizKey, asOfTso);
            OutletPermissionEngine.ProjectionPlan plan =
                    permission.plan(ctx.consumer, ctx.contract, streamId, source.availableColumns(streamId));
            audit.recordRead(consumerId, ctx.contract.getContractId(), streamId, asOfTso, asOfTso,
                    row == null ? 0 : 1, plan.trimmedColumns, plan.maskedColumns);
            Map<String, Object> projected = permission.projectRow(plan, row);
            if (projected != null && !plan.rowFilter.test(row)) {
                return null;   // 行条件不满足 → 对消费方等价"不存在"
            }
            return projected;
        } finally {
            quota.release(ctx.consumer);
        }
    }

    /* ============================================================== 接口 ⑤ position-at */

    public Map<String, Object> positionAt(String consumerId, String streamId, long timeMs) {
        Ctx ctx = context(consumerId);
        permission.checkStream(ctx.consumer, ctx.contract, streamId);
        long tso = source.positionAt(streamId, timeMs);
        StreamStatus st = source.status(streamId);
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("streamId", streamId);
        m.put("time", timeMs);
        m.put("tso", tso);
        m.put("position", PositionKey.of(st.getEpoch(), tso));
        return m;
    }

    /* =================================================================== 接口 ⑥ commit */

    /**
     * 上报位点(可选接口)。
     * <b>平台只把它记进 consumer_offset_seen 用于监控/审计,不作为交付依据</b>(决策 1)。
     */
    public Map<String, Object> commit(String consumerId, String streamId, String position) {
        Ctx ctx = context(consumerId);
        permission.checkStream(ctx.consumer, ctx.contract, streamId);
        StreamStatus st = source.status(streamId);
        PositionKey pos = PositionKey.decode(position);
        seenOffset.put(key(consumerId, streamId), new long[]{pos.getEpoch(), pos.getTso()});
        long lagTso = Math.max(0, st.getLatest() - pos.getTso());
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("consumerId", consumerId);
        m.put("streamId", streamId);
        m.put("ackedPosition", position);
        m.put("lagTs", lagTso);
        m.put("remark", "平台仅观测(非权威位点)");
        return m;
    }

    /** 观测位点(调试/监控用) */
    public Map<String, Object> seen(String consumerId, String streamId) {
        long[] v = seenOffset.get(key(consumerId, streamId));
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("consumerId", consumerId);
        m.put("streamId", streamId);
        if (v != null) {
            m.put("epoch", v[0]);
            m.put("positionTso", v[1]);
        }
        return m;
    }

    /* ====================================================================== helpers */

    private record Ctx(ConsumerAccount consumer, Contract contract) {}

    private Ctx context(String consumerId) {
        ConsumerAccount consumer = registry.consumer(consumerId);
        if (consumer == null) {
            throw new BizException(OutletErrorCode.CONTRACT_FORBIDDEN, "消费方不存在:" + consumerId);
        }
        Contract contract = registry.activeContractOf(consumerId);
        if (contract == null) {
            throw new BizException(OutletErrorCode.CONTRACT_FORBIDDEN, "无生效契约:" + consumerId);
        }
        return new Ctx(consumer, contract);
    }

    private PositionKey parsePosition(String afterPosition, StreamStatus st) {
        if (afterPosition == null || afterPosition.isBlank()) {
            return new PositionKey(st.getEpoch(), st.getEarliest());
        }
        try {
            return PositionKey.decode(afterPosition);
        } catch (IllegalArgumentException e) {
            throw new BizException(com.dcp.common.ErrorCode.PARAM_ERROR, e.getMessage());
        }
    }

    /** 边界三规则(设计 §6.4.2) */
    private void validateBoundary(StreamStatus st, long t0) {
        if (t0 < st.getEarliest()) {
            throw new BizException(OutletErrorCode.BOUNDARY_BELOW_STREAM_START,
                    "T0=" + t0 + " < earliest=" + st.getEarliest());
        }
        if (t0 > st.getLatest()) {
            throw new BizException(OutletErrorCode.BOUNDARY_IN_FUTURE,
                    "T0=" + t0 + " > latest=" + st.getLatest());
        }
        // GC 过期判定:骨架数据源无 safe point → 由 Kafka/TiDB 版数据源在 status() 里把
        // earliest 收敛为 max(changefeed.start_ts, GC 可读起点),此处即自动生效(R2)。
    }

    private String key(String consumerId, String streamId) {
        return consumerId + "::" + streamId;
    }
}
