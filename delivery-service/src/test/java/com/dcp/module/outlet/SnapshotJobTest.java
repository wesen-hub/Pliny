package com.dcp.module.outlet;

import com.dcp.common.BizException;
import com.dcp.module.outlet.entity.SnapshotJobEntity;
import com.dcp.module.outlet.source.MockChangeStreamSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * snapshot_job 登记与 TTL 的语义锁(设计 §6.5)。
 *
 * <p>锁四件事:①首页登记任务(boundary + TTL)②续页必须带 boundary 且任务存在
 * ③任务过期 → 明确报错(绝不返回半截错数据)④拉完置 DONE,再续拉返回空页 done。
 */
class SnapshotJobTest {

    private static final String STREAM = "t_order";

    private MockChangeStreamSource source;
    private SnapshotJobService jobs;
    private ChangeOutletService svc;

    @BeforeEach
    void setUp() {
        source = new MockChangeStreamSource();
        source.seed();
        OutletRegistry registry = new OutletRegistry(null, null, "memory");
        registry.init();
        OutletProperties props = new OutletProperties();
        jobs = new SnapshotJobService(null, props);
        svc = new ChangeOutletService(registry,
                new OutletPermissionEngine(null),
                new OutletQuotaLimiter(),
                new OutletAuditService(null),
                source,
                jobs);
    }

    @Test
    @DisplayName("① 首页:发放边界并登记任务(PAGING)")
    void firstPageRegistersJob() {
        var page = svc.snapshot("demo-sub", STREAM, null, null, 10);
        assertTrue(page.isDone());
        SnapshotJobEntity job = jobs.find("demo-sub", STREAM, page.getBoundaryTso());
        assertNotNull(job, "首页必须登记 snapshot_job");
        assertEquals(SnapshotJobEntity.STATUS_DONE, job.getStatus(), "一行页拉完 → 直接 DONE");
        assertTrue(job.getRowsExported() > 0);
    }

    @Test
    @DisplayName("② 分页:首页 PAGING,续页带 boundary,拉完 DONE")
    void pagingFlow() {
        var first = svc.snapshot("demo-sub", STREAM, null, null, 1);
        assertFalse(first.isDone());
        SnapshotJobEntity job = jobs.find("demo-sub", STREAM, first.getBoundaryTso());
        assertEquals(SnapshotJobEntity.STATUS_PAGING, job.getStatus());
        assertEquals(first.getPageToken(), job.getPageToken());
        assertEquals(1, job.getRowsExported());

        long boundary = first.getBoundaryTso();
        var second = svc.snapshot("demo-sub", STREAM, boundary, first.getPageToken(), 10);
        assertTrue(second.isDone());
        assertEquals(SnapshotJobEntity.STATUS_DONE, jobs.find("demo-sub", STREAM, boundary).getStatus());
    }

    @Test
    @DisplayName("③ 续页缺 boundary → 参数错误(不允许靠猜边界继续)")
    void continuationRequiresBoundary() {
        var first = svc.snapshot("demo-sub", STREAM, null, null, 1);
        BizException e = assertThrows(BizException.class,
                () -> svc.snapshot("demo-sub", STREAM, null, first.getPageToken(), 10));
        assertEquals(com.dcp.common.ErrorCode.PARAM_ERROR.getCode(), e.getCode());
    }

    @Test
    @DisplayName("④ 未登记 boundary 直接续拉 → SNAPSHOT_JOB_NOT_FOUND")
    void unknownJob() {
        var st = svc.streamStatus("demo-sub", STREAM);
        BizException e = assertThrows(BizException.class,
                () -> svc.snapshot("demo-sub", STREAM, st.getLatest(), "cursor-1", 10));
        assertEquals(OutletErrorCode.SNAPSHOT_JOB_NOT_FOUND, e.getCode());
    }

    @Test
    @DisplayName("⑤ 任务过期 → SNAPSHOT_JOB_EXPIRED(必须用新 T0 重来)")
    void expiredJob() {
        var first = svc.snapshot("demo-sub", STREAM, null, null, 1);
        assertFalse(first.isDone());
        // 人为把 TTL 推到过去(内存模式下 find 返回同一对象引用)
        SnapshotJobEntity job = jobs.find("demo-sub", STREAM, first.getBoundaryTso());
        job.setExpireAt(java.time.LocalDateTime.now().minusHours(1));

        BizException e = assertThrows(BizException.class,
                () -> svc.snapshot("demo-sub", STREAM, first.getBoundaryTso(), first.getPageToken(), 10));
        assertEquals(OutletErrorCode.SNAPSHOT_JOB_EXPIRED, e.getCode());
        assertEquals(SnapshotJobEntity.STATUS_EXPIRED, job.getStatus(), "过期任务应被标记 EXPIRED");
    }

    @Test
    @DisplayName("⑥ 拉完后再续拉 → 空页 done,不再重复出数")
    void resumeAfterDone() {
        var first = svc.snapshot("demo-sub", STREAM, null, null, 10);
        assertTrue(first.isDone());
        // DONE 后 pageToken 为 null → 客户端不会再续拉;若强行带上旧边界+任意 token:
        var again = svc.snapshot("demo-sub", STREAM, first.getBoundaryTso(), "some-cursor", 10);
        assertTrue(again.isDone());
        assertEquals(0, again.getRows().size());
    }
}
