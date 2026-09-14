package com.dcp.module.outlet;

import com.dcp.common.BizException;
import com.dcp.module.outlet.model.ChangeBatch;
import com.dcp.module.outlet.model.ConsumerAccount;
import com.dcp.module.outlet.model.PositionKey;
import com.dcp.module.outlet.source.MockChangeStreamSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 出口服务骨架的语义锁(骨架即验):
 * 边界三规则 / epoch 拦截 / 位点过期 / 越权列连 key 都不出现 / 脱敏 / 行条件(条件退出)/ 配额。
 *
 * <p>不启 Spring 上下文,全部用裸对象构造(AuditService 传 null → 审计降级为日志,不影响语义)。
 */
class OutletSkeletonTest {

    private static final String STREAM = "t_order";

    private MockChangeStreamSource source;
    private ChangeOutletService svc;

    @BeforeEach
    void setUp() {
        source = new MockChangeStreamSource();
        source.seed();
        OutletRegistry registry = new OutletRegistry();
        registry.seed();
        svc = new ChangeOutletService(registry,
                new OutletPermissionEngine(null),
                new OutletQuotaLimiter(),
                new OutletAuditService(null),
                source);
    }

    @Test
    @DisplayName("① stream-status:返回 epoch/earliest/latest/retentionHours")
    void streamStatus() {
        var st = svc.streamStatus("demo-sub", STREAM);
        assertEquals(1, st.getEpoch());
        assertTrue(st.getEarliest() > 0);
        assertTrue(st.getLatest() >= st.getEarliest());
        assertEquals(168, st.getRetentionHours());
    }

    @Test
    @DisplayName("② 边界规则:T0 < earliest → BOUNDARY_BELOW_STREAM_START")
    void boundaryBelowStart() {
        var st = svc.streamStatus("demo-sub", STREAM);
        BizException e = assertThrows(BizException.class,
                () -> svc.snapshot("demo-sub", STREAM, st.getEarliest() - 1, null, 10));
        assertEquals(OutletErrorCode.BOUNDARY_BELOW_STREAM_START, e.getCode());
    }

    @Test
    @DisplayName("② 边界规则:T0 > latest → BOUNDARY_IN_FUTURE")
    void boundaryInFuture() {
        var st = svc.streamStatus("demo-sub", STREAM);
        BizException e = assertThrows(BizException.class,
                () -> svc.snapshot("demo-sub", STREAM, st.getLatest() + 1_000_000L, null, 10));
        assertEquals(OutletErrorCode.BOUNDARY_IN_FUTURE, e.getCode());
    }

    @Test
    @DisplayName("② 边界合法:发放新 T0 → 分页拉全量,且 done")
    void snapshotOk() {
        var page = svc.snapshot("demo-sub", STREAM, null, null, 10);
        assertTrue(page.isDone());
        assertEquals(2, page.getRows().size(), "region=US 的行不应出现(行条件 region=CN)");
    }

    @Test
    @DisplayName("③ epoch 不一致 → EPOCH_MISMATCH(绝不静默续)")
    void epochMismatch() {
        var st = svc.streamStatus("demo-sub", STREAM);
        String bad = PositionKey.of(st.getEpoch() + 1, st.getEarliest());
        BizException e = assertThrows(BizException.class, () -> svc.changes("demo-sub", STREAM, bad, 10));
        assertEquals(OutletErrorCode.EPOCH_MISMATCH, e.getCode());
    }

    @Test
    @DisplayName("④ 位点早于 earliest → POSITION_EXPIRED(应回落重快照)")
    void positionExpired() {
        var st = svc.streamStatus("demo-sub", STREAM);
        String old = PositionKey.of(st.getEpoch(), Math.max(1, st.getEarliest() - 10));
        BizException e = assertThrows(BizException.class, () -> svc.changes("demo-sub", STREAM, old, 10));
        assertEquals(OutletErrorCode.POSITION_EXPIRED, e.getCode());
    }

    @Test
    @DisplayName("⑤ 权限投影:越权列(region)连 key 都不出现 + phone 脱敏")
    void projectionTrimsAndMasks() {
        ChangeBatch b = svc.changes("demo-sub", STREAM, null, 50);
        assertFalse(b.getEvents().isEmpty());
        b.getEvents().stream()
                .filter(e -> !"DELETE".equals(e.getOp()))
                .forEach(e -> {
                    assertFalse(e.getRow().containsKey("region"), "越权列 region 不应出现");
                    assertFalse(e.getRow().containsKey("_pk_id"), "内部列不应出现");
                    Object phone = e.getRow().get("phone");
                    assertNotNull(phone);
                    assertTrue(String.valueOf(phone).contains("****"), "phone 应已脱敏: " + phone);
                });
    }

    @Test
    @DisplayName("⑥ 行条件(条件退出):region=US 的行 → 对消费方等价删除")
    void rowConditionBecomesDelete() {
        source.append(STREAM, "INSERT", Map.of("id", "7", "name", "us-row",
                "amount", "1.00", "region", "US", "phone", "13900000007"), null);
        ChangeBatch b = svc.changes("demo-sub", STREAM, null, 100);
        boolean usTurnedDelete = b.getEvents().stream()
                .anyMatch(e -> "DELETE".equals(e.getOp()) && "7".equals(String.valueOf(e.getKey().get("id"))));
        assertTrue(usTurnedDelete, "不满足契约行条件的行应转为 DELETE(墓碑语义)");
    }

    @Test
    @DisplayName("⑦ 配额:QPS 超限 → QUOTA_EXCEEDED")
    void quotaExceeded() {
        OutletQuotaLimiter limiter = new OutletQuotaLimiter();
        ConsumerAccount c = new ConsumerAccount();
        c.setConsumerId("t-quota");
        c.setQuotaQps(1);
        c.setQuotaConcurrent(1);
        limiter.acquire(c);
        BizException e = assertThrows(BizException.class, () -> limiter.acquire(c));
        assertEquals(OutletErrorCode.QUOTA_EXCEEDED, e.getCode());
    }

    @Test
    @DisplayName("⑧ 位点可编码/解码(不透明串往返)")
    void positionRoundTrip() {
        String p = PositionKey.of(3, 469072825121505282L);
        PositionKey k = PositionKey.decode(p);
        assertEquals(3, k.getEpoch());
        assertEquals(469072825121505282L, k.getTso());
    }

    @Test
    @DisplayName("⑨ watermark 随批次透出(供消费方判追平/切批)")
    void watermarkExposed() {
        ChangeBatch b = svc.changes("demo-sub", STREAM, null, 50);
        assertNotNull(b.getWatermarkTs(), "骨架数据源的 watermark = 已发出事件最大 TSO");
    }
}
