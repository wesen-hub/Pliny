package com.dcp.outlet.rt;

import com.dcp.outlet.rt.ContractProvider;
import com.dcp.outlet.rt.ContractView;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;

/**
 * 计算面从控制面**拉取契约视图**(30s 刷新;收窄/吊销在下一批生效)。
 *
 * <p><b>失败安全(fail-closed)</b>:拉取失败时沿用上次缓存;若**从未成功过**,则抛异常 ——
 * **绝不退回"全放行"**(那等于越权)。
 */
public class HttpContractProvider implements ContractProvider {

    private static final Logger LOG = Logger.getLogger("outlet-contract");

    private final String baseUrl;
    private final String contractRef;
    private final String consumerId;
    private final String stream;
    private final HttpClient http;
    private final Duration timeout;
    private final long refreshMillis;

    private final AtomicReference<ContractView> cached = new AtomicReference<>();
    private volatile long loadedAt;

    public HttpContractProvider(String baseUrl, String contractRef, String consumerId, String stream,
                                Duration timeout, long refreshMillis) {
        this.baseUrl = baseUrl.endsWith("/") ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl;
        this.contractRef = contractRef;
        this.consumerId = consumerId;
        this.stream = stream;
        this.timeout = timeout;
        this.refreshMillis = refreshMillis;
        this.http = HttpClient.newBuilder().connectTimeout(timeout).build();
    }

    public HttpContractProvider(String baseUrl, String contractRef, String consumerId, String stream) {
        this(baseUrl, contractRef, consumerId, stream, Duration.ofSeconds(3), 30_000L);
    }

    @Override
    public ContractView current() {
        ContractView c = cached.get();
        long now = System.currentTimeMillis();
        if (c != null && now - loadedAt < refreshMillis) return c;         // 缓存有效
        try {
            ContractView fresh = fetch();
            cached.set(fresh);
            loadedAt = now;
            return fresh;
        } catch (RuntimeException e) {
            if (c != null) {                                               // 沿用旧缓存(不放大)
                LOG.warning(() -> "契约刷新失败,沿用上次缓存(不放大权限): " + e.getMessage());
                return c;
            }
            throw new IllegalStateException("无法获取契约且无缓存 —— 拒绝消费(fail-closed)", e);
        }
    }

    private ContractView fetch() {
        String url = baseUrl + "/api/outlet/contracts/" + contractRef + "?consumerId=" + consumerId
                + "&stream=" + (stream == null ? "" : stream);
        try {
            HttpRequest req = HttpRequest.newBuilder(URI.create(url)).timeout(timeout).GET().build();
            HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() / 100 != 2) {
                throw new IllegalStateException("HTTP " + resp.statusCode() + " " + resp.body());
            }
            Map<String, String> kv = ContractViewCodec.fromQuery(resp.body());
            if (!kv.containsKey("allowedColumns")) {
                throw new IllegalStateException("契约视图缺少 allowedColumns 字段");
            }
            return ContractViewCodec.decode(kv);
        } catch (Exception e) {
            throw new IllegalStateException("拉取契约失败:" + url, e);
        }
    }

    /** 强制失效(排障/契约变更后立即生效)。 */
    public void invalidate() { loadedAt = 0; }
}
