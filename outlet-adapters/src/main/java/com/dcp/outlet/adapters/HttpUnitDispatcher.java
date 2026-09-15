package com.dcp.outlet.adapters;

import com.dcp.outlet.control.DispatchRequest;
import com.dcp.outlet.control.UnitDispatcher;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;
import java.util.logging.Logger;

/**
 * 控制面 → 计算面的**真实 HTTP 下发**(D25:两个部署单元)。
 *
 * <p>换成同进程部署时,只需把本类替换为 {@code UnitDispatcherAdapter};两侧代码不变。
 * 幂等由 {@code dispatchId}/{@code unitId} 保证:重复下发无副作用。
 */
public class HttpUnitDispatcher implements UnitDispatcher {

    private static final Logger LOG = Logger.getLogger("outlet-dispatcher");

    private final String baseUrl;
    private final HttpClient http;
    private final Duration timeout;

    public HttpUnitDispatcher(String baseUrl, Duration timeout) {
        this.baseUrl = baseUrl.endsWith("/") ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl;
        this.timeout = timeout;
        this.http = HttpClient.newBuilder().connectTimeout(timeout).build();
    }

    public HttpUnitDispatcher(String baseUrl) {
        this(baseUrl, Duration.ofSeconds(5));
    }

    @Override
    public void dispatch(DispatchRequest r) {
        Map<String, String> kv = UnitCommandCodec.encode(r);
        String url = baseUrl + "/units?" + UnitCommandCodec.toQuery(kv);
        try {
            HttpRequest req = HttpRequest.newBuilder(URI.create(url))
                    .timeout(timeout)
                    .POST(HttpRequest.BodyPublishers.noBody())
                    .build();
            HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() / 100 != 2) {
                throw new IllegalStateException("CreateUnit failed: HTTP " + resp.statusCode() + " " + resp.body());
            }
            LOG.info(() -> "dispatched unit=" + r.unitId() + " → " + resp.statusCode());
        } catch (IllegalStateException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException("CreateUnit 下发失败(worker 不可达?):" + r.unitId(), e);
        }
    }

    /** 优雅停机:DELETE /units/{unitId}。 */
    public void stop(String unitId) {
        try {
            HttpRequest req = HttpRequest.newBuilder(URI.create(baseUrl + "/units/" + unitId))
                    .timeout(timeout).DELETE().build();
            http.send(req, HttpResponse.BodyHandlers.ofString());
        } catch (Exception e) {
            throw new IllegalStateException("StopUnit 下发失败:" + unitId, e);
        }
    }
}
