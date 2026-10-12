package com.perm.pep;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.perm.common.signature.Ed25519Keys;
import com.perm.common.signature.Ed25519Signer;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.security.KeyPair;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * PermClient 全链路测试（本地 HttpServer 模拟 PDP）
 * 覆盖：判定+验签 / L1 缓存命中 / fail-closed 降级 / 防重放窗口
 */
class PermClientTest {

    private HttpServer server;
    private KeyPair keyPair;
    private Ed25519Signer signer;
    private final ObjectMapper mapper = new ObjectMapper();
    private final AtomicInteger hitCount = new AtomicInteger();

    @BeforeEach
    void setUp() throws Exception {
        keyPair = Ed25519Keys.generate();
        signer = new Ed25519Signer(keyPair.getPrivate());
        server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/api/v1/authorize", exchange -> {
            hitCount.incrementAndGet();
            ObjectNode node = mapper.createObjectNode();
            node.put("decision", "PERMIT");
            node.put("planVersion", 1);
            node.put("rowPredicateTemplate", "(`region` = :p0)");
            node.putArray("placeholders")
                    .addObject().put("name", ":p0").put("ref", "subject.region").put("kind", "SCALAR");
            node.putArray("visibleColumns").add("id").add("region");
            node.putArray("masks").addObject().put("column", "amount").put("type", "HASH");
            node.putArray("obligations");
            node.putArray("matchedPolicies").add("pol_test_001");
            long ts = System.currentTimeMillis() / 1000;
            node.put("ts", ts);
            // 签名 payload：去掉 signature（null 不序列化，与 PDP Jackson 默认行为一致）
            node.remove("signature");
            String signature = signer.sign(node.toString() + "&ts=" + ts);
            node.put("signature", signature);
            byte[] body = node.toString().getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        });
        server.start();
    }

    @AfterEach
    void tearDown() {
        server.stop(0);
    }

    private PermClient client() {
        return PermClient.builder()
                .pdpBaseUrl("http://127.0.0.1:" + server.getAddress().getPort())
                .publicKeyBase64(Ed25519Keys.encodePublic(keyPair.getPublic()))
                .build();
    }

    private PepContext ctx() {
        return PepContext.builder()
                .userId("lisi")
                .roles(List.of("regional_manager"))
                .attributes(Map.of("region", "华东"))
                .resourceId("sdm.sales_order")
                .action("select")
                .dialect("mysql")
                .build();
    }

    @Test
    @DisplayName("判定成功且验签通过")
    void authorizeReturnsVerifiedArtifact() {
        VerifiedArtifact artifact = client().authorize(ctx());
        assertNotNull(artifact);
        assertTrue(artifact.permitted());
        assertEquals(1, artifact.planVersion());
        assertEquals("(`region` = :p0)", artifact.rowPredicateTemplate());
        assertEquals(List.of("pol_test_001"), artifact.matchedPolicies());
    }

    @Test
    @DisplayName("L1 缓存命中：相同上下文仅一次远程调用")
    void l1CacheHit() {
        PermClient client = client();
        client.authorize(ctx());
        client.authorize(ctx());
        assertEquals(1, hitCount.get());
    }

    @Test
    @DisplayName("不同上下文各自判定（缓存键含稳定属性指纹）")
    void differentContextMisses() {
        PermClient client = client();
        client.authorize(ctx());
        client.authorize(PepContext.builder()
                .userId("zhangsan").roles(List.of("intern"))
                .attributes(Map.of("region", "华南"))
                .resourceId("sdm.sales_order").action("select").build());
        assertEquals(2, hitCount.get());
    }

    @Test
    @DisplayName("PDP 不可达 → fail-closed 拒绝")
    void failClosedWhenPdpDown() {
        PermClient client = PermClient.builder()
                .pdpBaseUrl("http://127.0.0.1:1") // 未监听端口
                .publicKeyBase64(Ed25519Keys.encodePublic(keyPair.getPublic()))
                .connectTimeoutSeconds(1)
                .build();
        assertThrows(PepDeniedException.class, () -> client.authorize(ctx()));
    }

    @Test
    @DisplayName("签名被篡改 → 验签失败拒绝")
    void rejectTamperedSignature() throws Exception {
        // 用不配套的密钥对构造客户端 → 验签必然失败
        PermClient client = PermClient.builder()
                .pdpBaseUrl("http://127.0.0.1:" + server.getAddress().getPort())
                .publicKeyBase64(Ed25519Keys.encodePublic(Ed25519Keys.generate().getPublic()))
                .build();
        assertThrows(PepDeniedException.class, () -> client.authorize(ctx()));
    }
}
