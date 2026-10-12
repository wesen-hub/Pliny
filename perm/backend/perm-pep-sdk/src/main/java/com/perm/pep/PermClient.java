package com.perm.pep;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.perm.common.signature.ArtifactVerifier;
import com.perm.common.signature.Ed25519Keys;
import com.perm.common.signature.Ed25519Verifier;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * PEP 业务接入客户端（判定链最后一公里）
 * 流程：L1 本地缓存（Caffeine，键=planVersion#resourceId#action#稳定属性指纹）
 *       → PDP 授权调用（JDK HttpClient）
 *       → Ed25519 公钥验签 + ts 防重放窗口（R-Sec6/R-Sec7）
 *       → 缓存回填。
 * 降级（fail-closed）：PDP 不可达/响应异常/验签失败 → 抛 PepDeniedException，业务侧拒绝执行。
 */
public final class PermClient {

    private final String pdpBaseUrl;
    private final ArtifactVerifier verifier;
    private final Cache<String, VerifiedArtifact> l1;
    private final ObjectMapper mapper = new ObjectMapper();
    private final HttpClient httpClient;
    private final long replayWindowSeconds;

    /** 不稳定属性（指纹排除项，与 PDP 侧一致） */
    private static final List<String> UNSTABLE_ATTRS = List.of("requestId", "traceId", "ts");

    private PermClient(Builder builder) {
        this.pdpBaseUrl = builder.pdpBaseUrl;
        this.verifier = new Ed25519Verifier(Ed25519Keys.parsePublic(builder.publicKeyBase64));
        this.l1 = Caffeine.newBuilder()
                .maximumSize(builder.l1MaxSize)
                .expireAfterWrite(Duration.ofSeconds(builder.l1TtlSeconds))
                .build();
        this.replayWindowSeconds = builder.replayWindowSeconds;
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(builder.connectTimeoutSeconds))
                .build();
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * 授权判定入口：返回验签通过的产物（不可变）。
     * 产物为模板无值（C1），绑定/注入由 BoundSqlRewriter 执行。
     */
    public VerifiedArtifact authorize(PepContext ctx) {
        String key = cacheKey(ctx);
        VerifiedArtifact cached = l1.getIfPresent(key);
        if (cached != null) {
            return cached;
        }
        VerifiedArtifact artifact = fetchAndVerify(ctx);
        l1.put(key, artifact);
        return artifact;
    }

    /** 远程判定 + 验签 + 防重放 */
    private VerifiedArtifact fetchAndVerify(PepContext ctx) {
        String body;
        int status;
        try {
            String reqJson = mapper.writeValueAsString(Map.of(
                    "userId", ctx.userId(),
                    "roles", ctx.roles(),
                    "attributes", ctx.attributes(),
                    "tagLevels", ctx.tagLevels(),
                    "resourceId", ctx.resourceId(),
                    "action", ctx.action(),
                    "dialect", ctx.dialect()));
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(pdpBaseUrl + "/api/v1/authorize"))
                    .header("Content-Type", "application/json")
                    .timeout(Duration.ofSeconds(10))
                    .POST(HttpRequest.BodyPublishers.ofString(reqJson, StandardCharsets.UTF_8))
                    .build();
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            status = response.statusCode();
            body = response.body();
        } catch (Exception e) {
            throw new PepDeniedException("PDP 不可达，fail-closed 拒绝执行", e);
        }
        if (status != 200) {
            throw new PepDeniedException("PDP 判定失败（HTTP " + status + "），fail-closed 拒绝执行");
        }
        return verifyAndParse(body);
    }

    /** 验签 + 防重放 + 解析为不可变产物 */
    private VerifiedArtifact verifyAndParse(String body) {
        try {
            JsonNode node = mapper.readTree(body);
            String signature = node.path("signature").asText(null);
            long ts = node.path("ts").asLong(0);

            // 防重放窗口（R-Sec7）：本地时钟与签名 ts 偏差超窗 → 拒绝
            long now = System.currentTimeMillis() / 1000;
            if (Math.abs(now - ts) > replayWindowSeconds) {
                throw new PepDeniedException("产物时间戳超出防重放窗口（ts=" + ts + "），fail-closed 拒绝执行");
            }

            // payload = 响应体去掉 signature 字段（ts 字段保留）后的序列化 + "&ts=" + ts
            // 与 PDP 签名侧一致：Jackson 默认不序列化 null 的 signature，ts 为数值正常写出
            JsonNode payloadNode = node.deepCopy();
            ((com.fasterxml.jackson.databind.node.ObjectNode) payloadNode).remove("signature");
            String payload = payloadNode.toString() + "&ts=" + ts;

            if (!verifier.verify(payload, signature)) {
                throw new PepDeniedException("产物验签失败（签名非法/被篡改），fail-closed 拒绝执行");
            }

            JsonNode placeholders = node.path("placeholders");
            List<VerifiedArtifact.Placeholder> phs = new java.util.ArrayList<>();
            for (JsonNode p : placeholders) {
                phs.add(new VerifiedArtifact.Placeholder(
                        p.path("name").asText(),
                        p.path("ref").isNull() ? null : p.path("ref").asText(),
                        p.path("kind").asText(null),
                        p.path("value").isNull() ? null : p.path("value").asText()));
            }
            List<VerifiedArtifact.Mask> masks = new java.util.ArrayList<>();
            for (JsonNode m : node.path("masks")) {
                masks.add(new VerifiedArtifact.Mask(m.path("column").asText(), m.path("type").asText()));
            }
            List<String> visible = mapper.convertValue(node.path("visibleColumns"), mapper.getTypeFactory()
                    .constructCollectionType(List.class, String.class));
            List<String> matched = mapper.convertValue(node.path("matchedPolicies"), mapper.getTypeFactory()
                    .constructCollectionType(List.class, String.class));
            return new VerifiedArtifact(
                    node.path("decision").asText(),
                    node.path("denyReason").isNull() ? null : node.path("denyReason").asText(),
                    node.path("planVersion").asInt(),
                    node.path("rowPredicateTemplate").isNull() ? null : node.path("rowPredicateTemplate").asText(),
                    phs,
                    visible,
                    masks,
                    null, // tagScope：演示级透传占位（标签模式在 PEP 侧按子查询/快照语义由适配器实现）
                    List.of(),
                    matched);
        } catch (PepDeniedException e) {
            throw e;
        } catch (Exception e) {
            throw new PepDeniedException("产物解析失败，fail-closed 拒绝执行", e);
        }
    }

    /**
     * 缓存键：resourceId#action#稳定属性指纹。
     * 键不含 planVersion（PEP 侧无版本感知）：策略变更后旧产物最多存活一个 L1 TTL（默认 60s），
     * 正式形态由策略变更事件（Kafka 主通道）驱动 PEP 定点失效；演示级边界 = TTL 内版本滞后。
     */
    private String cacheKey(PepContext ctx) {
        return ctx.resourceId() + "#" + ctx.action() + "#" + fingerprint(ctx);
    }

    /** 稳定属性指纹（与 PDP 侧算法一致：排序 → SHA-256 截断 8 字节） */
    private String fingerprint(PepContext ctx) {
        Map<String, Object> stable = new TreeMap<>();
        stable.put("userId", ctx.userId());
        stable.put("roles", ctx.roles().stream().sorted().toList());
        Map<String, Object> attrs = new TreeMap<>();
        ctx.attributes().forEach((k, v) -> {
            if (!UNSTABLE_ATTRS.contains(k)) {
                attrs.put(k, v);
            }
        });
        stable.put("attributes", attrs);
        stable.put("tagLevels", new TreeMap<>(ctx.tagLevels()));
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

    public static final class Builder {
        private String pdpBaseUrl = "http://localhost:8081";
        /** 演示公钥（与 PDP 演示私钥配套）；生产形态由部署环境注入 */
        private String publicKeyBase64 = "MCowBQYDK2VwAyEAw3xYYpGY/C4gbWd/bV6jZebqAPqGsD/993Y6kBPI/ag=";
        private int l1MaxSize = 10_000;
        private int l1TtlSeconds = 60;
        private long replayWindowSeconds = 300;
        private int connectTimeoutSeconds = 3;

        public Builder pdpBaseUrl(String pdpBaseUrl) {
            this.pdpBaseUrl = pdpBaseUrl;
            return this;
        }

        public Builder publicKeyBase64(String publicKeyBase64) {
            this.publicKeyBase64 = publicKeyBase64;
            return this;
        }

        public Builder l1MaxSize(int l1MaxSize) {
            this.l1MaxSize = l1MaxSize;
            return this;
        }

        public Builder l1TtlSeconds(int l1TtlSeconds) {
            this.l1TtlSeconds = l1TtlSeconds;
            return this;
        }

        public Builder replayWindowSeconds(long replayWindowSeconds) {
            this.replayWindowSeconds = replayWindowSeconds;
            return this;
        }

        public Builder connectTimeoutSeconds(int connectTimeoutSeconds) {
            this.connectTimeoutSeconds = connectTimeoutSeconds;
            return this;
        }

        public PermClient build() {
            return new PermClient(this);
        }
    }
}
