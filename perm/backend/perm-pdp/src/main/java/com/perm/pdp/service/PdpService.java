package com.perm.pdp.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.perm.common.decision.DecisionEngine;
import com.perm.common.model.Artifact;
import com.perm.common.model.AuthorizationContext;
import com.perm.common.model.AuthorizationRequest;
import com.perm.common.signature.ArtifactSigner;
import com.perm.common.signature.ArtifactVerifier;
import com.perm.pdp.audit.AuditRecorder;
import com.perm.pdp.cache.L2ArtifactCache;
import com.perm.pdp.cache.PlanCache;
import com.perm.pdp.dto.ArtifactResponseDto;
import com.perm.pdp.dto.AuthorizeRequestDto;
import com.perm.pdp.policy.PolicySnapshotLoader;
import com.perm.pdp.render.ArtifactRenderer;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * PDP 判定编排（请求链核心）
 * 指纹（稳定属性）→ L1 单飞缓存 → 判定链 → 方言渲染 → 审计义务 → Ed25519 签名（R-Sec6）
 * 顺序与《权限校验全链路》一致；绑定/注入由 PEP 侧执行（本服务只产出模板无值产物，铁律 C1）。
 */
@Service
public class PdpService {

    private final PolicySnapshotLoader snapshotLoader;
    private final PlanCache planCache;
    private final ArtifactRenderer renderer;
    private final ArtifactSigner signer;
    private final ArtifactVerifier verifier;
    private final AuditRecorder auditRecorder;
    /** L2 共享缓存（perm.redis.enabled=true 时装配；false 时判定链直落） */
    private final Optional<L2ArtifactCache> l2Cache;
    private final ObjectMapper objectMapper = new ObjectMapper();
    /** 判定链实际执行次数（单飞命中时不自增；指标 + 测试断言用） */
    private final java.util.concurrent.atomic.AtomicLong decisionCount = new java.util.concurrent.atomic.AtomicLong();

    /** 不稳定属性（指纹排除项） */
    private static final List<String> UNSTABLE_ATTRS = List.of("requestId", "traceId", "ts");

    public PdpService(PolicySnapshotLoader snapshotLoader,
                      PlanCache planCache,
                      ArtifactRenderer renderer,
                      AuditRecorder auditRecorder,
                      ArtifactSigner signer,
                      ArtifactVerifier verifier,
                      Optional<L2ArtifactCache> l2Cache) {
        this.snapshotLoader = snapshotLoader;
        this.planCache = planCache;
        this.renderer = renderer;
        this.signer = signer;
        this.verifier = verifier;
        this.auditRecorder = auditRecorder;
        this.l2Cache = l2Cache;
    }

    /**
     * 判定入口（同步阻塞返回）
     * 缓存键 = planVersion#resourceId#action#指纹（键空间版本化 + 稳定属性指纹）
     * 分层：L1 单飞（进程内）→ L2 共享缓存（Redis，可选）→ 判定链 → 回填 L2/L1
     */
    public ArtifactResponseDto authorize(AuthorizeRequestDto req) {
        PolicySnapshotLoader.Snapshot snapshot = snapshotLoader.current();
        String key = cacheKey(snapshot.version(), req);
        try {
            CompletableFuture<ArtifactResponseDto> future = planCache.get(key, () -> {
                // L2 先查（模板无值产物；Redis 不可达返回空回源判定链）
                ArtifactResponseDto l2Hit = l2Cache.flatMap(c -> c.get(key)).orElse(null);
                if (l2Hit != null) {
                    return l2Hit;
                }
                ArtifactResponseDto computed = compute(snapshot, req);
                l2Cache.ifPresent(c -> c.put(key, computed));
                return computed;
            });
            // 缓存对象永不 mutate：拷贝后执行义务 + 签名（ts/signature 属响应级字段）
            ArtifactResponseDto dto = future.get().copy();
            executeObligations(req, dto);
            return sign(dto);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("判定被中断", e);
        } catch (ExecutionException e) {
            throw new IllegalStateException("判定计算失败", e.getCause());
        }
    }

    /** 判定链实际执行次数（单飞验证用） */
    public long decisionCount() {
        return decisionCount.get();
    }

    /** 缓存键：planVersion#resourceId#action#指纹 */
    private String cacheKey(int planVersion, AuthorizeRequestDto req) {
        return planVersion + "#" + req.getResourceId() + "#" + req.getAction() + "#" + fingerprint(req);
    }

    /**
     * 稳定属性指纹（缓存键三段式的指纹段）
     * roles 排序 + attributes（排除不稳定键）排序 + tagLevels 排序 → SHA-256 截断
     */
    static String fingerprint(AuthorizeRequestDto req) {
        Map<String, Object> stable = new TreeMap<>();
        stable.put("userId", req.getUserId());
        stable.put("roles", req.getRoles() == null ? List.of() : req.getRoles().stream().sorted().toList());
        Map<String, Object> attrs = new TreeMap<>();
        if (req.getAttributes() != null) {
            for (Map.Entry<String, Object> e : req.getAttributes().entrySet()) {
                if (!UNSTABLE_ATTRS.contains(e.getKey())) {
                    attrs.put(e.getKey(), e.getValue());
                }
            }
        }
        stable.put("attributes", attrs);
        stable.put("tagLevels", req.getTagLevels() == null ? Map.of() : new TreeMap<>(req.getTagLevels()));
        String json = stable.toString();
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256")
                    .digest(json.getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < 8; i++) {
                sb.append(String.format("%02x", digest[i]));
            }
            return sb.toString();
        } catch (Exception e) {
            throw new IllegalStateException("指纹计算失败", e);
        }
    }

    /** 判定 + 渲染（缓存未命中时的计算路径） */
    private ArtifactResponseDto compute(PolicySnapshotLoader.Snapshot snapshot, AuthorizeRequestDto req) {
        decisionCount.incrementAndGet();
        AuthorizationContext ctx = toContext(req);
        AuthorizationRequest request = AuthorizationRequest.builder()
                .context(ctx)
                .resourceId(req.getResourceId())
                .action(req.getAction())
                .dialect(req.getDialect())
                .build();
        Artifact artifact = DecisionEngine.decide(request, snapshot.policies());
        return renderer.toDto(artifact, req.getDialect());
    }

    /** 义务执行（R-A 审计义务：AUDIT sync 判定路径内落） */
    private void executeObligations(AuthorizeRequestDto req, ArtifactResponseDto dto) {
        if (dto.getObligations() == null) {
            return;
        }
        for (ArtifactResponseDto.ObligationDto o : dto.getObligations()) {
            if ("AUDIT".equals(o.type())) {
                auditRecorder.record(req.getUserId(), req.getResourceId(), req.getAction(),
                        dto.getDecision(), o.mode(), dto.getMatchedPolicies());
            }
        }
    }

    /**
     * 签名（Ed25519，R-Sec6）。
     * payload = DTO 去掉 signature 字段后的 JSON + "&ts=" + ts（ts 字段保留在 JSON 内）。
     * 与 PEP SDK 验签侧完全对称：两侧都用 valueToTree + remove("signature") + node.toString()，
     * 避免 writeValueAsString 把 null 序列化为 "signature":null 导致跨端 payload 不一致。
     */
    private ArtifactResponseDto sign(ArtifactResponseDto dto) {
        long ts = System.currentTimeMillis() / 1000;
        dto.setTs(ts);
        dto.setSignature(null);
        try {
            JsonNode node = objectMapper.valueToTree(dto);
            ((ObjectNode) node).remove("signature");
            String payload = node.toString() + "&ts=" + ts;
            dto.setSignature(signer.sign(payload));
        } catch (Exception e) {
            throw new IllegalStateException("产物序列化失败", e);
        }
        return dto;
    }

    /** 验签（管理面自检 / PEP 侧验签参考实现）：与 sign 同一 payload 契约 */
    public boolean verify(ArtifactResponseDto dto) {
        String signature = dto.getSignature();
        dto.setSignature(null);
        try {
            JsonNode node = objectMapper.valueToTree(dto);
            ((ObjectNode) node).remove("signature");
            String payload = node.toString() + "&ts=" + dto.getTs();
            boolean ok = verifier.verify(payload, signature);
            dto.setSignature(signature);
            return ok;
        } catch (Exception e) {
            dto.setSignature(signature);
            return false;
        }
    }

    private AuthorizationContext toContext(AuthorizeRequestDto req) {
        AuthorizationContext.Builder builder = AuthorizationContext.builder()
                .userId(req.getUserId());
        if (req.getRoles() != null) {
            req.getRoles().forEach(builder::role);
        }
        if (req.getAttributes() != null) {
            req.getAttributes().forEach(builder::attribute);
        }
        if (req.getTagLevels() != null) {
            req.getTagLevels().forEach(builder::tagLevel);
        }
        return builder.build();
    }
}
