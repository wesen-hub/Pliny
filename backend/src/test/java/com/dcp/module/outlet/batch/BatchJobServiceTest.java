package com.dcp.module.outlet.batch;

import com.dcp.common.BizException;
import com.dcp.module.outlet.OutletErrorCode;
import com.dcp.module.outlet.entity.SnapshotManifestEntity;
import com.dcp.module.outlet.model.ChangeBatch;
import com.dcp.module.outlet.model.ChangeEvent;
import com.dcp.module.outlet.model.PositionKey;
import com.dcp.module.outlet.model.SnapshotPage;
import com.dcp.module.outlet.model.StreamStatus;
import com.dcp.module.outlet.sdk.ConsumerOffsetStore;
import com.dcp.module.outlet.sdk.OutletApi;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 执行器 B(批通道)的语义锁(设计 §6.3 / §6.4.1)。
 *
 * <p>锁七件事:bootstrap 发布+位点收口 / **重跑幂等**(不重复搬数) / 增量批 [lo,hi] / 无新数据短路 /
 * 边界三规则拦截 / **发布失败则位点不推进** / mixed = bootstrap + incr。
 */
class BatchJobServiceTest {

    private static final String STREAM = "t_order";
    private static final int EPOCH = 1;

    private FakeApi api;
    private LocalBatchExecutor executor;
    private ManifestService manifests;
    private MemoryOffsets offsets;
    private BatchJobService svc;

    @BeforeEach
    void setUp() {
        api = new FakeApi();
        executor = new LocalBatchExecutor(api);
        manifests = new ManifestService(null);
        offsets = new MemoryOffsets();
        svc = new BatchJobService(api, executor, manifests, offsets);
    }

    private BatchJobSpec spec(String jobType) {
        BatchJobSpec s = new BatchJobSpec();
        s.setConsumerId("demo-tag");
        s.setStreamId(STREAM);
        s.setJobType(jobType);
        s.setTarget("tag_fact_view");
        s.setSnapshotLimit(2);
        s.setBatchLimit(10);
        return s;
    }

    @Test
    @DisplayName("① bootstrap:产出 staging → manifest PUBLISHED → 位点 = T0")
    void bootstrapPublishesAndCommitsOffset() {
        BatchJobOutcome o = svc.run(spec("bootstrap"));
        assertTrue(o.isPublished());
        assertFalse(o.isReused());
        assertEquals(3, o.getRowsWritten(), "3 行全量物化");
        assertEquals(o.getBoundaryTso(), o.getOffsetSubmitted());
        SnapshotManifestEntity m = manifests.get(o.getManifestId());
        assertEquals(SnapshotManifestEntity.STATUS_PUBLISHED, m.getStatus());
        assertNotNull(m.getPublishTime());
        assertTrue(m.getStagingPath().contains("bootstrap"));
        assertEquals(o.getBoundaryTso(), offsets.load("demo-tag", STREAM)[1]);
    }

    @Test
    @DisplayName("② 重跑幂等:复用已发布产物,不重复搬数")
    void rerunIsIdempotent() {
        BatchJobOutcome first = svc.run(spec("bootstrap"));
        BatchJobOutcome second = svc.run(spec("bootstrap"));
        assertTrue(second.isReused());
        assertEquals(first.getManifestId(), second.getManifestId());
        assertEquals(1, executor.execCount(first.getStagingPath()), "第二次不应再执行物化");
        assertEquals(3, executor.stagingView().get(first.getStagingPath()).size(), "staging 不应被重复写入放大");
    }

    @Test
    @DisplayName("③ 增量批:lo 从位点续、hi 取 WATERMARK,发布后位点 = hi")
    void incrBatchUsesRangeAndWatermark() {
        svc.run(spec("bootstrap"));                       // 位点 = T0
        long t0 = offsets.load("demo-tag", STREAM)[1];
        api.append(1000L, "INSERT", Map.of("id", "4", "name", "d"));
        api.append(1010L, "UPDATE", Map.of("id", "1", "name", "a2"));
        api.watermark = 1010L;

        BatchJobOutcome o = svc.run(spec("incr_batch"));
        assertTrue(o.isPublished());
        assertEquals(1010L, o.getHiTso(), "hi 应取 WATERMARK");
        assertEquals(1010L, o.getOffsetSubmitted());
        // staging 里 id=1 已按版本收敛成新值,id=4 新增
        Map<String, Map<String, Object>> table = executor.stagingView().get(o.getStagingPath());
        assertEquals("a2", table.get("1").get("name"));
        assertTrue(table.containsKey("4"));
        assertTrue(o.getLoTso() >= t0);
    }

    @Test
    @DisplayName("④ 无新数据(lo ≥ hi)→ 短路,不产出 manifest")
    void noNewDataShortCircuit() {
        svc.run(spec("bootstrap"));
        api.watermark = offsets.load("demo-tag", STREAM)[1];   // 水位 = 当前位点
        BatchJobOutcome o = svc.run(spec("incr_batch"));
        assertFalse(o.isPublished());
        assertEquals(0, o.getRowsWritten());
        assertNull(o.getManifestId());
    }

    @Test
    @DisplayName("⑤ 边界三规则:T0 < earliest → BOUNDARY_BELOW_STREAM_START")
    void boundaryBelowEarliest() {
        api.earliest = 5000L;
        BatchJobSpec s = spec("bootstrap");
        s.setBoundaryTso(100L);
        BizException e = assertThrows(BizException.class, () -> svc.run(s));
        assertEquals(OutletErrorCode.BOUNDARY_BELOW_STREAM_START, e.getCode());
    }

    @Test
    @DisplayName("⑥ 发布失败 → manifest FAILED 且位点不推进")
    void publishFailureKeepsOffset() {
        BatchExecutor failing = new BatchExecutor() {
            @Override
            public ExecutionResult bootstrap(BatchJobSpec spec, long boundaryTso, int epoch) {
                throw new IllegalStateException("staging 写入失败");
            }

            @Override
            public ExecutionResult incrBatch(BatchJobSpec spec, long loTso, long hiTso, int epoch) {
                throw new IllegalStateException("staging 写入失败");
            }
        };
        BatchJobService svcFail = new BatchJobService(api, failing, manifests, offsets);
        assertThrows(IllegalStateException.class, () -> svcFail.run(spec("bootstrap")));
        assertNull(offsets.load("demo-tag", STREAM), "发布失败 → 位点不得推进(可安全重跑)");
    }

    @Test
    @DisplayName("⑦ mixed:bootstrap 打底(无新增量时以 bootstrap 结果为准);补数据后接力到 hi")
    void mixedJob() {
        // 第一阶段:bootstrap 打底,无新增量 → 返回 bootstrap 结果
        BatchJobOutcome o1 = svc.run(spec("mixed"));
        assertTrue(o1.isPublished());
        assertEquals(900L, o1.getOffsetSubmitted(), "位点收口到 bootstrap 的 T0");

        // 第二阶段:补一条变更 + 推进水位 → mixed 复用 bootstrap 产物,再接过增量到 hi
        api.append(2000L, "INSERT", Map.of("id", "9", "name", "z"));
        api.watermark = 2000L;
        BatchJobOutcome o2 = svc.run(spec("mixed"));
        assertTrue(o2.isPublished());
        assertEquals(SnapshotManifestEntity.TYPE_INCR_BATCH, o2.getJobType());
        assertEquals(2000L, o2.getOffsetSubmitted());
    }

    /* ------------------------------------------------------------------ 测试替身 */

    static class FakeApi implements OutletApi {
        final List<Map<String, Object>> rows = new ArrayList<>(List.of(
                Map.of("id", "1", "name", "a"), Map.of("id", "2", "name", "b"), Map.of("id", "3", "name", "c")));
        final List<ChangeEvent> events = new ArrayList<>();
        long earliest = 0L;
        long latest = 900L;
        Long watermark = null;

        void append(long tso, String op, Map<String, Object> row) {
            ChangeEvent e = new ChangeEvent();
            e.setOp(op);
            e.setStreamId(STREAM);
            e.setRow(new LinkedHashMap<>(row));
            Map<String, Object> key = new LinkedHashMap<>();
            key.put("id", row.get("id"));
            e.setKey(key);
            e.setTso(tso);
            e.setEpoch(EPOCH);
            events.add(e);
            latest = Math.max(latest, tso);
        }

        @Override
        public StreamStatus streamStatus(String streamId) {
            StreamStatus s = new StreamStatus();
            s.setStreamId(streamId);
            s.setEpoch(EPOCH);
            s.setEarliest(earliest);
            s.setLatest(latest);
            s.setWatermarkTs(watermark);
            s.setRetentionHours(168);
            return s;
        }

        @Override
        public SnapshotPage snapshot(String streamId, Long boundaryTso, String pageToken, int limit) {
            SnapshotPage p = new SnapshotPage();
            p.setStreamId(streamId);
            p.setEpoch(EPOCH);
            long boundary = boundaryTso == null ? latest : boundaryTso;
            p.setBoundaryTso(boundary);
            p.setNextPosition(PositionKey.of(EPOCH, boundary));
            int from = pageToken == null ? 0 : Integer.parseInt(pageToken);
            int to = Math.min(from + limit, rows.size());
            p.setRows(new ArrayList<>(rows.subList(from, to)));
            p.setRowsEstimate((long) rows.size());
            p.setDone(to >= rows.size());
            p.setPageToken(p.isDone() ? null : String.valueOf(to));
            return p;
        }

        @Override
        public ChangeBatch changes(String streamId, String afterPosition, int limit) {
            long after = afterPosition == null ? 0 : PositionKey.decode(afterPosition).getTso();
            ChangeBatch b = new ChangeBatch();
            b.setStreamId(streamId);
            b.setEpoch(EPOCH);
            b.setScannedFrom(after);
            long last = after;
            for (ChangeEvent e : events) {
                if (e.getTso() > after && b.getEvents().size() < limit) {
                    b.getEvents().add(e);
                    last = e.getTso();
                }
            }
            b.setScannedTo(last);
            b.setNextPosition(PositionKey.of(EPOCH, last));
            b.setWatermarkTs(watermark);
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
