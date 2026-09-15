package com.dcp.outlet.adapters;

import com.dcp.outlet.worker.Heartbeat;
import com.dcp.outlet.worker.LeaseGuard;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.logging.Logger;

/**
 * 计算面 → 控制面的**上报通道**:心跳(进度)与租约续约。
 *
 * <p>续约失败(HTTP 非 2xx 或网络不通)→ 返回 false,单元据此**自停**,防双消费。
 */
public class HttpControlPlaneClient implements Heartbeat, LeaseGuard {

    private static final Logger LOG = Logger.getLogger("outlet-heartbeat");

    private final String baseUrl;
    private final HttpClient http;
    private final Duration timeout;

    public HttpControlPlaneClient(String baseUrl, Duration timeout) {
        this.baseUrl = baseUrl.endsWith("/") ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl;
        this.timeout = timeout;
        this.http = HttpClient.newBuilder().connectTimeout(timeout).build();
    }

    public HttpControlPlaneClient(String baseUrl) { this(baseUrl, Duration.ofSeconds(3)); }

    @Override
    public void send(String unitId, int partition, long offset, long tso) {
        call("/api/units/" + unitId + "/heartbeat?partition=" + partition
                + "&offset=" + offset + "&tso=" + tso);
    }

    @Override
    public boolean renew(String unitId) {
        try {
            String body = call("/api/units/" + unitId + "/lease/renew");
            return body != null && body.contains("\"renewed\":true");
        } catch (RuntimeException e) {
            LOG.warning(() -> "租约续约失败(单元将自停): " + unitId + " · " + e.getMessage());
            return false;
        }
    }

    private String call(String path) {
        try {
            HttpRequest req = HttpRequest.newBuilder(URI.create(baseUrl + path))
                    .timeout(timeout)
                    .POST(HttpRequest.BodyPublishers.noBody())
                    .build();
            HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() / 100 != 2) {
                throw new IllegalStateException("HTTP " + resp.statusCode() + " " + resp.body());
            }
            return resp.body();
        } catch (Exception e) {
            throw new IllegalStateException("控制面不可达:" + path, e);
        }
    }
}
