package com.dcp.module.outlet.sdk;

import com.dcp.module.outlet.model.ChangeBatch;
import com.dcp.module.outlet.model.ChangeEvent;
import com.dcp.module.outlet.model.PositionKey;
import com.dcp.module.outlet.model.SnapshotPage;
import com.dcp.module.outlet.model.StreamStatus;
import lombok.extern.slf4j.Slf4j;

import java.util.*;

/**
 * 执行器 A 的消费方 SDK(设计 §6.1 / §6.2):**消费方只写一个 handler**。
 *
 * <pre>
 * try (OutletConsumer c = OutletConsumer.builder(api, offsets).consumerId("...").streams(List.of("t_order")).build()) {
 *     c.open(StartPolicy.SNAPSHOT, batch -> { ...写自己的目标库... });
 *     while (true) {
 *         SdkBatch b = c.poll();
 *         if (b == null) { Thread.sleep(200); continue; }
 *         c.handle(b);                       // ← 或由 open 的 handler 统一处理
 *         c.commitWithBusinessWrite(b, () -> businessWrite(b));   // 位点与业务写同事务
 *     }
 * }
 * </pre>
 *
 * <p>SDK 统一承担:位点持久化、快照段/增量段路由、退避重试、死信、**过期/epoch 变化自动重快照**、
 * WATERMARK 追平判定、多输入最小水位对齐。
 */
@Slf4j
public class OutletConsumer {

    public enum StartPolicy { SNAPSHOT, LATEST }

    /** 消费方唯一要实现的东西 */
    public interface Handler {
        void handle(SdkBatch batch);
    }

    /** 每个流的运行态 */
    private static final class State {
        String position;          // 不透明位点(增量段用)
        Integer epoch;
        Long boundary;            // 快照边界(快照段用)
        String pageToken;         // 快照分页令牌
        boolean snapshotPhase;
        Long watermark;
        long processedTso;
        long resnapshotCount;
    }

    private final String consumerId;
    private final List<String> streams;
    private final OutletApi api;
    private final ConsumerOffsetStore offsets;
    private final RetryPolicy retry;
    private final DeadLetterSink dlq;
    private final int snapshotLimit;
    private final int batchLimit;
    private final TxRunner tx;
    private final Map<String, State> states = new LinkedHashMap<>();

    private OutletConsumer(Builder b) {
        this.consumerId = b.consumerId;
        this.streams = b.streams;
        this.api = b.api;
        this.offsets = b.offsets;
        this.retry = b.retry == null ? RetryPolicy.defaults() : b.retry;
        this.dlq = b.dlq == null ? new LoggingDeadLetterSink() : b.dlq;
        this.snapshotLimit = b.snapshotLimit;
        this.batchLimit = b.batchLimit;
        this.tx = b.tx;
    }

    public static Builder builder(OutletApi api, ConsumerOffsetStore offsets) {
        return new Builder(api, offsets);
    }

    /* ------------------------------------------------------------------ 启动/open */

    /**
     * 定起点(设计 §6.4.2:位点存在则续;否则按策略取快照边界或 latest)。
     * 幂等:重复 open 不会重置已有位点。
     */
    public void open(StartPolicy policy) {
        for (String streamId : streams) {
            State st = states.computeIfAbsent(streamId, k -> new State());
            if (st.position != null || st.snapshotPhase) {
                continue;   // 已在运行
            }
            long[] saved = offsets.load(consumerId, streamId);
            if (saved != null) {
                st.epoch = (int) saved[0];
                st.position = PositionKey.of((int) saved[0], saved[1]);
                st.processedTso = saved[1];
                log.info("[outlet-sdk] {} 从已存位点续跑:stream={} epoch={} tso={}",
                        consumerId, streamId, saved[0], saved[1]);
                continue;
            }
            if (policy == StartPolicy.LATEST) {
                StreamStatus status = retry.execute("stream-status", () -> api.streamStatus(streamId));
                st.epoch = status.getEpoch();
                st.position = PositionKey.of(status.getEpoch(), status.getLatest());
                st.processedTso = status.getLatest();
                st.watermark = status.getWatermarkTs();
                log.info("[outlet-sdk] {} 以 latest 起步:stream={} tso={}", consumerId, streamId, status.getLatest());
            } else {
                st.snapshotPhase = true;   // 交由 poll() 取首页并发放 boundary
            }
        }
    }

    /* ------------------------------------------------------------------ 读取/poll */

    /** 取下一个批次;无数据返回 null(调用方可 sleep 后重试) */
    public SdkBatch poll() {
        for (String streamId : streams) {
            State st = states.get(streamId);
            if (st == null) {
                continue;
            }
            SdkBatch batch = next(streamId, st);
            if (batch != null) {
                return batch;
            }
        }
        return null;
    }

    private SdkBatch next(String streamId, State st) {
        try {
            return st.snapshotPhase ? pollSnapshot(streamId, st) : pollChanges(streamId, st);
        } catch (OutletApiException e) {
            if (e.needsResnapshot()) {
                // ★ 位点过期 / epoch 变化 → 自动重快照(设计 §6.8:绝不静默从当前水位开始)
                st.resnapshotCount++;
                log.warn("[outlet-sdk] {} stream={} 需要重快照({}),已丢弃旧位点 {}",
                        consumerId, streamId, e.getMessage(), st.position);
                resetToSnapshot(st);
                return pollSnapshot(streamId, st);
            }
            dlq.accept(streamId, null, e);
            throw e;
        } catch (RuntimeException e) {
            dlq.accept(streamId, null, e);
            throw e;
        }
    }

    private SdkBatch pollSnapshot(String streamId, State st) {
        SnapshotPage page = retry.execute("snapshot", () -> api.snapshot(streamId, st.boundary, st.pageToken, snapshotLimit));
        st.boundary = page.getBoundaryTso();
        st.epoch = page.getEpoch();
        st.pageToken = page.getPageToken();

        SdkBatch b = new SdkBatch();
        b.setKind(SdkBatch.Kind.SNAPSHOT);
        b.setStreamId(streamId);
        b.setEpoch(page.getEpoch());
        b.setRows(page.getRows());
        b.setLastTso(page.getBoundaryTso());
        b.setNextPosition(page.getNextPosition() != null
                ? page.getNextPosition()
                : PositionKey.of(page.getEpoch(), page.getBoundaryTso()));
        b.setHasMore(!page.isDone());
        b.setSnapshotDone(page.isDone());
        if (page.isDone()) {
            // 快照段结束 → 转常驻增量,位点 = boundary(衔接点,设计 §6.4.1)
            st.snapshotPhase = false;
            st.position = b.getNextPosition();
            st.processedTso = st.boundary;
            st.boundary = null;
            st.pageToken = null;
        }
        return b;
    }

    private SdkBatch pollChanges(String streamId, State st) {
        ChangeBatch cb = retry.execute("changes", () -> api.changes(streamId, st.position, batchLimit));
        st.watermark = cb.getWatermarkTs();
        if (cb.getEvents().isEmpty()) {
            return null;   // 无新事件(水位已记录)
        }
        SdkBatch b = new SdkBatch();
        b.setKind(SdkBatch.Kind.CHANGE);
        b.setStreamId(streamId);
        b.setEpoch(cb.getEpoch());
        b.setEvents(cb.getEvents());
        b.setLastTso(cb.getScannedTo() == null ? st.processedTso : cb.getScannedTo());
        b.setNextPosition(cb.getNextPosition());
        b.setWatermarkTs(cb.getWatermarkTs());
        b.setHasMore(cb.isHasMore());
        return b;
    }

    /* ------------------------------------------------------------------ 提交/commit */

    /** 推进位点(无业务写时使用) */
    public void commit(SdkBatch batch) {
        State st = states.get(batch.getStreamId());
        if (st == null || batch.getNextPosition() == null) {
            return;
        }
        offsets.save(consumerId, batch.getStreamId(), batch.getEpoch(), batch.getLastTso());
        st.position = batch.getNextPosition();
        st.processedTso = batch.getLastTso();
        if (batch.getWatermarkTs() != null) {
            st.watermark = batch.getWatermarkTs();
        }
    }

    /**
     * **推荐用法**:业务写 + 位点推进同事务(位点推进 ⟺ 数据已落地)。
     * 未注入 {@link TxRunner} 时退化为"先写数据、后推进位点"(跨库场景的正确顺序)。
     */
    public void commitWithBusinessWrite(SdkBatch batch, Runnable businessWrite) {
        if (tx != null) {
            tx.inTx(() -> {
                businessWrite.run();
                commit(batch);
                return null;
            });
            return;
        }
        businessWrite.run();
        commit(batch);
    }

    /* ------------------------------------------------------------------ 水位/追平 */

    /** 多输入对齐:所有输入流的最小水位(凡 ≤ 该水位的数据都已到齐)——设计 §6.2 打标 */
    public Long safeWatermark() {
        Long min = null;
        for (State st : states.values()) {
            if (st.watermark == null) {
                return null;   // 任一输入尚无水位 → 不可判定"到齐"
            }
            min = min == null ? st.watermark : Math.min(min, st.watermark);
        }
        return min;
    }

    /** 多输入对齐:所有输入已处理位点的最小值(可安全处理的上界) */
    public long safeProcessedTso() {
        long min = Long.MAX_VALUE;
        for (State st : states.values()) {
            min = Math.min(min, st.processedTso);
        }
        return min == Long.MAX_VALUE ? 0 : min;
    }

    /** 是否已追平(位点已到 watermark) */
    public boolean caughtUp(String streamId) {
        State st = states.get(streamId);
        return st != null && st.watermark != null && st.processedTso >= st.watermark;
    }

    /** 观测:每个流的状态(位点/水位/重快照次数)——供 lag 上报 */
    public Map<String, Object> stats() {
        Map<String, Object> m = new LinkedHashMap<>();
        states.forEach((k, st) -> {
            Map<String, Object> s = new LinkedHashMap<>();
            s.put("position", st.position);
            s.put("epoch", st.epoch);
            s.put("processedTso", st.processedTso);
            s.put("watermark", st.watermark);
            s.put("snapshotPhase", st.snapshotPhase);
            s.put("resnapshotCount", st.resnapshotCount);
            m.put(k, s);
        });
        return m;
    }

    private void resetToSnapshot(State st) {
        st.position = null;
        st.processedTso = 0;
        st.boundary = null;
        st.pageToken = null;
        st.snapshotPhase = true;
    }

    /* ------------------------------------------------------------------ builder */

    public static final class Builder {
        private final OutletApi api;
        private final ConsumerOffsetStore offsets;
        private String consumerId = "consumer";
        private List<String> streams = new ArrayList<>();
        private RetryPolicy retry;
        private DeadLetterSink dlq;
        private int snapshotLimit = 1000;
        private int batchLimit = 500;
        private TxRunner tx;

        private Builder(OutletApi api, ConsumerOffsetStore offsets) {
            this.api = api;
            this.offsets = offsets;
        }

        public Builder consumerId(String id) {
            this.consumerId = id;
            return this;
        }

        public Builder streams(List<String> streams) {
            this.streams = streams;
            return this;
        }

        public Builder retry(RetryPolicy retry) {
            this.retry = retry;
            return this;
        }

        public Builder deadLetterSink(DeadLetterSink dlq) {
            this.dlq = dlq;
            return this;
        }

        public Builder snapshotLimit(int limit) {
            this.snapshotLimit = limit;
            return this;
        }

        public Builder batchLimit(int limit) {
            this.batchLimit = limit;
            return this;
        }

        public Builder txRunner(TxRunner tx) {
            this.tx = tx;
            return this;
        }

        public OutletConsumer build() {
            return new OutletConsumer(this);
        }
    }
}
