package com.perm.pdp.service;

import com.perm.common.signature.Ed25519Keys;
import com.perm.common.signature.Ed25519Signer;
import com.perm.common.signature.Ed25519Verifier;
import com.perm.pdp.audit.AuditRecorder;
import com.perm.pdp.audit.InMemoryAuditRecorder;
import com.perm.pdp.cache.L2ArtifactCache;
import com.perm.pdp.cache.PlanCache;
import com.perm.pdp.dto.ArtifactResponseDto;
import com.perm.pdp.dto.AuthorizeRequestDto;
import com.perm.pdp.policy.PolicySnapshotLoader;
import com.perm.pdp.render.ArtifactRenderer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * PDP 判定编排测试（纯单元，无 Spring 上下文）
 * 覆盖：判定链路贯通 / 缓存命中 / 单飞 / 指纹稳定性 / 验签 / 审计义务 / 标签模式 A / 黑名单 DENY
 */
class PdpServiceTest {

    private PdpService service;
    private PolicySnapshotLoader snapshotLoader;
    private AuditRecorder auditRecorder;

    @BeforeEach
    void setUp() {
        snapshotLoader = new PolicySnapshotLoader();
        auditRecorder = new InMemoryAuditRecorder();
        java.security.KeyPair kp = Ed25519Keys.generate();
        service = new PdpService(snapshotLoader, new PlanCache(), new ArtifactRenderer(),
                auditRecorder,
                new Ed25519Signer(kp.getPrivate()),
                new Ed25519Verifier(kp.getPublic()),
                java.util.Optional.empty());
    }

    private AuthorizeRequestDto regionManagerRequest() {
        AuthorizeRequestDto req = new AuthorizeRequestDto();
        req.setUserId("lisi");
        req.setRoles(List.of("regional_manager"));
        req.setAttributes(Map.of("region", "华东", "allowedStatus", List.of("PAID", "SHIPPED")));
        req.setResourceId("sdm.sales_order");
        req.setAction("select");
        req.setDialect("mysql");
        return req;
    }

    @Test
    @DisplayName("S1 区域经理：PERMIT + 模板无值 + 占位符 ref + 掩码 + 验签通过")
    void regionManagerPermit() {
        ArtifactResponseDto dto = service.authorize(regionManagerRequest());

        assertEquals("PERMIT", dto.getDecision());
        assertNull(dto.getDenyReason());
        assertNotNull(dto.getRowPredicateTemplate());
        assertTrue(dto.getRowPredicateTemplate().contains("region"));
        assertTrue(dto.getRowPredicateTemplate().contains("status"));
        // 铁律 C1：模板无值（占位符不带字面量，值由 ref 绑定）
        List<ArtifactResponseDto.PlaceholderDto> placeholders = dto.getPlaceholders();
        assertFalse(placeholders.isEmpty());
        assertTrue(placeholders.stream().anyMatch(p -> "subject.region".equals(p.ref())));
        // 掩码（R-D9）
        assertTrue(dto.getMasks().stream()
                .anyMatch(m -> "amount".equals(m.column()) && "HASH".equals(m.type())));
        // 列裁剪（R-D8）
        assertTrue(dto.getVisibleColumns().contains("order_no"));
        // 审计义务
        assertEquals(List.of("pol_region_001"), dto.getMatchedPolicies());
        // 验签（R-Sec1）
        assertTrue(service.verify(dto));
    }

    @Test
    @DisplayName("S2 黑名单：DENY-overrides 且验签通过、无行谓词")
    void denyOverrides() {
        AuthorizeRequestDto req = regionManagerRequest();
        req.setUserId("zhangsan");

        ArtifactResponseDto dto = service.authorize(req);
        assertEquals("DENY", dto.getDecision());
        assertNotNull(dto.getDenyReason());
        assertNull(dto.getRowPredicateTemplate());
        assertTrue(service.verify(dto));
    }

    @Test
    @DisplayName("S3 零权限：fail-closed DENY")
    void noPolicyDeny() {
        AuthorizeRequestDto req = new AuthorizeRequestDto();
        req.setUserId("wangwu");
        req.setResourceId("sdm.sales_order");
        req.setAction("select");
        req.setDialect("mysql");

        ArtifactResponseDto dto = service.authorize(req);
        assertEquals("DENY", dto.getDecision());
    }

    @Test
    @DisplayName("S4 标签模式 A：LIST 下推 tagScope + 实体列表")
    void tagListPushdown() {
        AuthorizeRequestDto req = new AuthorizeRequestDto();
        req.setUserId("zhaoliu");
        req.setRoles(List.of("marketing_analyst"));
        req.setTagLevels(Map.of("业务域/客户价值/高价值", 3));
        req.setResourceId("sdm.sales_order");
        req.setAction("select");
        req.setDialect("mysql");

        ArtifactResponseDto dto = service.authorize(req);
        assertEquals("PERMIT", dto.getDecision());
        assertNotNull(dto.getTagScope());
        assertEquals("LIST", dto.getTagScope().expandMode());
        assertEquals("customer_id", dto.getTagScope().targetKey());
        assertEquals(5, dto.getTagScope().entities().size());
    }

    @Test
    @DisplayName("缓存命中：相同键二次判定不重算（decisionCount 不变）")
    void cacheHit() {
        service.authorize(regionManagerRequest());
        long count = service.decisionCount();

        service.authorize(regionManagerRequest());
        assertEquals(count, service.decisionCount(), "相同键应命中 L1 缓存，不重算");
        // 审计义务每次判定路径都落（含缓存命中）
        assertEquals(2, auditRecorder.size());
    }

    @Test
    @DisplayName("指纹稳定性：requestId 等不稳定属性不改变缓存键")
    void unstableAttrsExcluded() {
        AuthorizeRequestDto req1 = regionManagerRequest();
        req1.setAttributes(Map.of("region", "华东", "allowedStatus", List.of("PAID", "SHIPPED"),
                "requestId", "req-001"));
        AuthorizeRequestDto req2 = regionManagerRequest();
        req2.setAttributes(Map.of("region", "华东", "allowedStatus", List.of("PAID", "SHIPPED"),
                "requestId", "req-002", "traceId", "trace-xyz"));

        assertEquals(PdpService.fingerprint(req1), PdpService.fingerprint(req2));
        service.authorize(req1);
        long count = service.decisionCount();
        service.authorize(req2);
        assertEquals(count, service.decisionCount(), "不稳定属性变化不应产生新缓存键");
    }

    @Test
    @DisplayName("单飞：并发相同键只执行一次判定链")
    void singleFlight() throws Exception {
        int threads = 8;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch ready = new CountDownLatch(threads);
        CountDownLatch start = new CountDownLatch(1);
        java.util.concurrent.CopyOnWriteArrayList<ArtifactResponseDto> results =
                new java.util.concurrent.CopyOnWriteArrayList<>();

        for (int i = 0; i < threads; i++) {
            pool.submit(() -> {
                try {
                    ready.countDown();
                    start.await();
                    results.add(service.authorize(regionManagerRequest()));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }
        ready.await();
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS));

        assertEquals(threads, results.size());
        assertEquals(1, service.decisionCount(), "并发相同键应单飞（判定链只执行一次）");
        // 每次请求都有独立签名
        assertTrue(results.stream().allMatch(r -> service.verify(r)));
    }

    @Test
    @DisplayName("验签：篡改产物后验签失败")
    void tamperedArtifactFailsVerify() {
        ArtifactResponseDto dto = service.authorize(regionManagerRequest());
        assertTrue(service.verify(dto));

        dto.setDecision("PERMIT");
        dto.setRowPredicateTemplate(dto.getRowPredicateTemplate() + " OR 1=1");
        assertFalse(service.verify(dto), "篡改行谓词模板后验签必须失败");
    }

    @Test
    @DisplayName("签名响应独立：缓存对象不被 mutate（两次响应 ts/signature 可不 同且各自可验）")
    void cachedObjectNotMutated() {
        ArtifactResponseDto first = service.authorize(regionManagerRequest());
        String firstSig = first.getSignature();
        ArtifactResponseDto second = service.authorize(regionManagerRequest());

        assertTrue(service.verify(first), "第一次响应验签（缓存对象未被后续请求 mutate）");
        assertTrue(service.verify(second), "第二次响应验签");
    }

    @Test
    @DisplayName("L2 缓存：命中时跳过判定链，miss 时回填")
    void l2CacheHitSkipsDecision() {
        var l2 = new InMemoryL2();
        service = new PdpService(snapshotLoader, new PlanCache(), new ArtifactRenderer(),
                auditRecorder,
                new Ed25519Signer(Ed25519Keys.generate().getPrivate()),
                new Ed25519Verifier(Ed25519Keys.generate().getPublic()),
                java.util.Optional.of(l2));

        // 首次：判定链执行 + 回填 L2
        service.authorize(regionManagerRequest());
        assertEquals(1, service.decisionCount());
        assertEquals(1, l2.size());

        // 新实例（模拟另一副本进程，共享 L2）：命中 L2 跳过判定链
        service = new PdpService(snapshotLoader, new PlanCache(), new ArtifactRenderer(),
                auditRecorder,
                new Ed25519Signer(Ed25519Keys.generate().getPrivate()),
                new Ed25519Verifier(Ed25519Keys.generate().getPublic()),
                java.util.Optional.of(l2));
        ArtifactResponseDto dto = service.authorize(regionManagerRequest());
        assertEquals(0, service.decisionCount(), "L2 命中不应执行判定链");
        assertEquals("PERMIT", dto.getDecision());
    }

    /** 内存 L2（测试替身，与 RedisL2ArtifactCache 同接口） */
    static class InMemoryL2 implements L2ArtifactCache {
        private final java.util.Map<String, ArtifactResponseDto> store = new java.util.concurrent.ConcurrentHashMap<>();

        @Override
        public java.util.Optional<ArtifactResponseDto> get(String key) {
            return java.util.Optional.ofNullable(store.get(key));
        }

        @Override
        public void put(String key, ArtifactResponseDto dto) {
            store.put(key, dto);
        }

        int size() {
            return store.size();
        }
    }
}
