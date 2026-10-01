package com.dcp.module.outlet.sdk;

import com.dcp.module.outlet.OutletErrorCode;
import com.dcp.module.outlet.model.ChangeBatch;
import com.dcp.module.outlet.model.ChangeEvent;
import com.dcp.module.outlet.model.PositionKey;
import com.dcp.module.outlet.model.SnapshotPage;
import com.dcp.module.outlet.model.StreamStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 消费方 SDK(执行器 A)的语义锁(设计 §6.1/§6.2/§6.4.1)。
 *
 * <p>锁八件事:快照→增量衔接、按已存位点续跑、**位点过期自动重快照**、瞬时故障退避重试、
 * 配额超限重试、业务写与位点同事务(失败则不推进)、多输入最小水位对齐、追平判定。
 */
class OutletConsumerSdkTest {

    private static final String A = "t_a";
    private static final String B = "t_b";
    private static final int EPOCH = 1;

    private FakeApi api;
    private MemoryOffsets offsets;

    @BeforeEach
    void setUp() {
        api = new FakeApi();
        offsets = new MemoryOffsets();
    }

    private OutletConsumer consumer(List<String> streams) {
        return OutletConsumer.builder(api, offsets)
                .consumerId("demo-sub")
                .streams(streams)
                .retry(new RetryPolicy(3, 0, 0, ms -> { }))   // 单测不真等
                .snapshotLimit(2)
                .batchLimit(10)
                .build();
    }

    @Test
    @DisplayName("① 快照→增量衔接:同一 boundary 作为增量起点,不丢不重")
    void snapshotThenIncrementalHandoff() {
        OutletConsumer c = consumer(List.of(A));
        c.open(OutletConsumer.StartPolicy.SNAPSHOT);

        // 快照分页(每页 2 行,3 行 → 2 页)
        SdkBatch p1 = c.poll();
        assertEquals(SdkBatch.Kind.SNAPSHOT, p1.getKind());
        assertEquals(2, p1.getRows().size());
        assertFalse(p1.isSnapshotDone());
        c.commit(p1);

        SdkBatch p2 = c.poll();
        assertEquals(1, p2.getRows().size());
        assertTrue(p2.isSnapshotDone(), "最后一页应标记快照段结束");
        c.commit(p2);

        long boundary = api.lastBoundary;
        assertEquals(boundary, p2.getLastTso(), "快照批次的位点 = boundary(T0)");

        // 快照后写入一条变更 → 应转到增量段(TSO 必须晚于 boundary)
        api.append(A, boundary + 10, "INSERT", Map.of("id", "9"));
        SdkBatch inc = c.poll();
        assertEquals(SdkBatch.Kind.CHANGE, inc.getKind());
        assertEquals(1, inc.getEvents().size());
        assertEquals(boundary + 10, inc.getLastTso());
        c.commit(inc);
        // 已推进到位点:再拉无数据
        assertNull(c.poll());
    }

    @Test
    @DisplayName("② 已有位点 → 直接续增量(不重做快照)")
    void resumeFromSavedOffset() {
        offsets.save("demo-sub", A, EPOCH, 50L);
        OutletConsumer c = consumer(List.of(A));
        c.open(OutletConsumer.StartPolicy.SNAPSHOT);
        assertNull(c.poll(), "位点已是最新 → 无数据(而不是重新快照)");
        api.append(A, 60L, "INSERT", Map.of("id", "1"));
        SdkBatch b = c.poll();
        assertEquals(SdkBatch.Kind.CHANGE, b.getKind());
        assertEquals(60L, b.getLastTso());
    }

    @Test
    @DisplayName("③ 位点过期 → 自动重快照(绝不静默从当前水位开始)")
    void autoResnapshotOnPositionExpired() {
        offsets.save("demo-sub", A, EPOCH, 10L);
        api.throwOnce = new OutletApiException(OutletErrorCode.POSITION_EXPIRED, "位点超出保留窗口");
        OutletConsumer c = consumer(List.of(A));
        c.open(OutletConsumer.StartPolicy.SNAPSHOT);

        SdkBatch b = c.poll();
        assertNotNull(b, "不应抛异常,而应自动回落重快照");
        assertEquals(SdkBatch.Kind.SNAPSHOT, b.getKind());
        @SuppressWarnings("unchecked")
        Map<String, Object> st = (Map<String, Object>) c.stats().get(A);
        assertEquals(1, ((Number) st.get("resnapshotCount")).intValue());
    }

    @Test
    @DisplayName("④ 瞬时故障 → 退避重试后成功")
    void retryTransientFailure() {
        api.transientFailures = 2;
        OutletConsumer c = consumer(List.of(A));
        c.open(OutletConsumer.StartPolicy.LATEST);
        api.append(A, 200L, "INSERT", Map.of("id", "2"));
        SdkBatch b = c.poll();
        assertEquals(SdkBatch.Kind.CHANGE, b.getKind());
        assertEquals(0, api.transientFailures, "两次瞬时失败应被重试吸收");
    }

    @Test
    @DisplayName("⑤ 配额超限 → 退避重试(不是重快照)")
    void retryQuotaExceeded() {
        api.quotaFailures = 1;
        OutletConsumer c = consumer(List.of(A));
        c.open(OutletConsumer.StartPolicy.LATEST);
        api.append(A, 300L, "INSERT", Map.of("id", "3"));
        SdkBatch b = c.poll();
        assertEquals(SdkBatch.Kind.CHANGE, b.getKind());
    }

    @Test
    @DisplayName("⑥ 业务写与位点同事务:业务写失败 → 位点不推进")
    void businessWriteFailureKeepsOffset() {
        OutletConsumer c = consumer(List.of(A));
        c.open(OutletConsumer.StartPolicy.SNAPSHOT);
        SdkBatch p1 = c.poll();
        assertThrows(IllegalStateException.class,
                () -> c.commitWithBusinessWrite(p1, () -> { throw new IllegalStateException("业务写失败"); }));

        assertNull(offsets.load("demo-sub", A), "业务写失败时位点不得推进");
        // 重放:同一批再来一次仍然可读(位点未动)
        OutletConsumer c2 = consumer(List.of(A));
        c2.open(OutletConsumer.StartPolicy.SNAPSHOT);
        assertNotNull(c2.poll(), "位点未推进 → 重放应能再拿到快照数据");
    }

    @Test
    @DisplayName("⑦ 多输入对齐:最小水位 / 最小已处理位点")
    void multiInputStreamsAlignment() {
        OutletConsumer c = consumer(List.of(A, B));
        c.open(OutletConsumer.StartPolicy.LATEST);
        api.watermark.put(A, 1000L);
        api.watermark.put(B, 800L);
        api.append(A, 900L, "INSERT", Map.of("id", "1"));
        api.append(B, 700L, "INSERT", Map.of("id", "1"));

        SdkBatch ba = c.poll();
        c.commit(ba);
        SdkBatch bb = c.poll();
        if (bb != null) {
            c.commit(bb);
        }

        assertEquals(800L, c.safeWatermark(), "多输入水位取最小");
        assertEquals(700L, c.safeProcessedTso(), "多输入已处理位点取最小");
        assertFalse(c.caughtUp(A), "A 位点 900 < 水位 1000 → 未追平");
    }

    @Test
    @DisplayName("⑧ 追平判定:位点 ≥ watermark 才算追上")
    void caughtUpDetection() {
        OutletConsumer c = consumer(List.of(A));
        c.open(OutletConsumer.StartPolicy.LATEST);
        api.watermark.put(A, 500L);
        api.append(A, 500L, "INSERT", Map.of("id", "1"));
        SdkBatch b = c.poll();
        c.commit(b);
        assertTrue(c.caughtUp(A), "位点 500 ≥ watermark 500 → 已追平");
    }

    /* ------------------------------------------------------------------ 测试替身 */

    /** 内存假实现:语义对齐出口服务(快照分页 / 增量按位点 / 水位 / 可注入故障) */
    static class FakeApi implements OutletApi {
        final Map<String, List<Map<String, Object>>> rows = new LinkedHashMap<>(Map.of(
                A, new ArrayList<>(List.of(Map.of("id", "1"), Map.of("id", "2"), Map.of("id", "3"))),
                B, new ArrayList<>(List.of(Map.of("id", "1"), Map.of("id", "2")))));
        final Map<String, List<ChangeEvent>> events = new LinkedHashMap<>();
        final Map<String, Long> watermark = new LinkedHashMap<>();
        long lastBoundary;
        OutletApiException throwOnce;
        int transientFailures;
        int quotaFailures;
        private final AtomicInteger pageCall = new AtomicInteger();

        void append(String stream, long tso, String op, Map<String, Object> row) {
            ChangeEvent e = new ChangeEvent();
            e.setOp(op);
            e.setStreamId(stream);
            e.setRow(new LinkedHashMap<>(row));
            e.setKey(new LinkedHashMap<>(row));
            e.setTso(tso);
            e.setEpoch(EPOCH);
            events.computeIfAbsent(stream, k -> new ArrayList<>()).add(e);
            watermark.put(stream, Math.max(watermark.getOrDefault(stream, 0L), tso));
        }

        private void checkFailures() {
            if (transientFailures > 0) {
                transientFailures--;
                throw new RuntimeException("瞬时故障(模拟 5xx)");
            }
            if (quotaFailures > 0) {
                quotaFailures--;
                throw new OutletApiException(OutletErrorCode.QUOTA_EXCEEDED, "QPS 超限");
            }
            if (throwOnce != null) {
                OutletApiException ex = throwOnce;
                throwOnce = null;
                throw ex;
            }
        }

        @Override
        public StreamStatus streamStatus(String streamId) {
            checkFailures();
            StreamStatus s = new StreamStatus();
            s.setStreamId(streamId);
            s.setEpoch(EPOCH);
            s.setEarliest(0);
            s.setLatest(watermark.getOrDefault(streamId, 0L));
            s.setWatermarkTs(watermark.get(streamId));
            s.setRetentionHours(168);
            return s;
        }

        @Override
        public SnapshotPage snapshot(String streamId, Long boundaryTso, String pageToken, int limit) {
            checkFailures();
            SnapshotPage p = new SnapshotPage();
            p.setStreamId(streamId);
            p.setEpoch(EPOCH);
            long boundary = boundaryTso == null ? Math.max(1L, System.currentTimeMillis()) : boundaryTso;
            lastBoundary = boundary;
            p.setBoundaryTso(boundary);
            p.setNextPosition(PositionKey.of(EPOCH, boundary));
            List<Map<String, Object>> all = rows.getOrDefault(streamId, List.of());
            int from = pageToken == null ? 0 : Integer.parseInt(pageToken);
            int to = Math.min(from + limit, all.size());
            p.setRows(new ArrayList<>(all.subList(from, to)));
            p.setRowsExported(to);
            p.setRowsEstimate((long) all.size());
            p.setDone(to >= all.size());
            p.setPageToken(p.isDone() ? null : String.valueOf(to));
            pageCall.incrementAndGet();
            return p;
        }

        @Override
        public ChangeBatch changes(String streamId, String afterPosition, int limit) {
            checkFailures();
            long after = afterPosition == null ? 0 : PositionKey.decode(afterPosition).getTso();
            ChangeBatch b = new ChangeBatch();
            b.setStreamId(streamId);
            b.setEpoch(EPOCH);
            b.setScannedFrom(after);
            long last = after;
            for (ChangeEvent e : events.getOrDefault(streamId, List.of())) {
                if (e.getTso() > after && b.getEvents().size() < limit) {
                    b.getEvents().add(e);
                    last = e.getTso();
                }
            }
            b.setScannedTo(last);
            b.setNextPosition(PositionKey.of(EPOCH, last));
            b.setWatermarkTs(watermark.get(streamId));
            return b;
        }

        @Override
        public Map<String, Object> keyLookup(String streamId, String bizKey, Long asOfTso) {
            return Map.of("id", bizKey);
        }

        @Override
        public Map<String, Object> positionAt(String streamId, long timeMs) {
            return Map.of("tso", timeMs << 18);
        }

        @Override
        public Map<String, Object> commit(String streamId, String position) {
            return Map.of("ok", true);
        }
    }

    /** 内存位点表(模拟消费方自己的 consumer_offset 表) */
    static class MemoryOffsets implements ConsumerOffsetStore {
        private final Map<String, long[]> store = new LinkedHashMap<>();

        @Override
        public long[] load(String consumerId, String streamId) {
            return store.get(consumerId + "::" + streamId);
        }

        @Override
        public void save(String consumerId, String streamId, int epoch, long positionTso) {
            store.put(consumerId + "::" + streamId, new long[]{epoch, positionTso});
        }
    }
}
