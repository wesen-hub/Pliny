package com.perm.pdp.sync;

import com.perm.common.compile.PolicyCompiler;
import com.perm.common.dto.PolicyDsl;
import com.perm.pdp.cache.PlanCache;
import com.perm.pdp.policy.PolicySnapshotLoader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 快照同步测试：版本比对 / 编译安装 / 版本防回退 / 降级保活
 */
class SnapshotSyncServiceTest {

    private PolicySnapshotLoader snapshotLoader;
    private PlanCache planCache;
    private SnapshotSyncService syncService;
    private AtomicInteger fetchCount;

    @BeforeEach
    void setUp() {
        snapshotLoader = new PolicySnapshotLoader();
        planCache = new PlanCache();
        fetchCount = new AtomicInteger();
    }

    private SnapshotSyncService withFetcher(AdminSnapshotDto dto, RuntimeException failure) {
        SnapshotFetcher fetcher = () -> {
            fetchCount.incrementAndGet();
            if (failure != null) {
                throw failure;
            }
            return dto;
        };
        return new SnapshotSyncService(snapshotLoader, planCache, new PolicyCompiler(), fetcher);
    }

    private PolicyDsl grantDsl(String id) {
        PolicyDsl dsl = new PolicyDsl();
        dsl.setPolicyId(id);
        dsl.setPolicyName("同步策略-" + id);
        dsl.setEffect("GRANT");
        dsl.setSubjectType("role");
        dsl.setSubjectIds(List.of("regional_manager"));
        dsl.setResourceId("sdm.sales_order");
        dsl.setActions(List.of("select"));
        return dsl;
    }

    private AdminSnapshotDto snapshot(int version, List<PolicyDsl> policies) {
        AdminSnapshotDto dto = new AdminSnapshotDto();
        dto.setVersion(version);
        dto.setPolicyCount(policies.size());
        dto.setPolicies(policies);
        return dto;
    }

    @Test
    @DisplayName("版本前进：拉取 v5 快照 → 编译安装 → 本地版本对齐")
    void syncInstallsNewerVersion() {
        AdminSnapshotDto remote = snapshot(5, List.of(grantDsl("pol_sync_001")));
        syncService = withFetcher(remote, null);

        syncService.sync();

        assertEquals(5, snapshotLoader.current().version(), "本地快照版本应与管理面对齐");
        assertEquals(1, snapshotLoader.current().policies().size());
        assertEquals("pol_sync_001", snapshotLoader.current().policies().get(0).policyId());
    }

    @Test
    @DisplayName("版本相同或回退：不安装（防回退）")
    void syncIgnoresOldOrEqualVersion() {
        AdminSnapshotDto remote = snapshot(1, List.of(grantDsl("pol_sync_002")));
        syncService = withFetcher(remote, null);

        syncService.sync();

        assertEquals(1, snapshotLoader.current().version(), "内置快照版本 1，不因同版本覆盖");
        assertEquals(4, snapshotLoader.current().policies().size(), "内置 4 条策略保持");
    }

    @Test
    @DisplayName("降级保活：拉取失败保持本地快照，不中断")
    void syncDegradesGracefully() {
        syncService = withFetcher(null, new RuntimeException("connection refused"));

        syncService.sync(); // 不应抛出

        assertEquals(1, snapshotLoader.current().version());
        assertEquals(4, snapshotLoader.current().policies().size(), "降级时保留内置快照");
        assertEquals(1, fetchCount.get());
    }

    @Test
    @DisplayName("空策略快照：ACTIVE 全下线后拉取到空列表（deny-by-default 生效）")
    void syncInstallsEmptySnapshot() {
        AdminSnapshotDto remote = snapshot(9, List.of());
        syncService = withFetcher(remote, null);

        syncService.sync();

        assertEquals(9, snapshotLoader.current().version());
        assertTrue(snapshotLoader.current().policies().isEmpty(),
                "管理面全下线 → PDP 空快照（默认拒绝）");
    }
}
