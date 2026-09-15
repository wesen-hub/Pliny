package com.dcp.outlet.adapters;

import com.dcp.outlet.control.T0Selector;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.logging.Logger;

/**
 * R1 适配器:changefeed 起始 TSO = TiCDC OpenAPI v2 的 {@code start_ts}。
 *
 * <p>{@code GET {base}/api/v2/changefeeds/{id}} 返回 {@code "start_ts": <TSO>}。
 * start_ts 在 changefeed 创建时固定、永不变,故**只查一次并缓存**。
 *
 * <p>本平台当前是单一 changefeed({@code chg-target-db-v4}),changefeedId 由配置指定;
 * 若未来多 changefeed,需把 topic → changefeed 映射改成动态解析(按 sink dispatcher 的 topic 模板匹配)。
 */
public class TiCdcChangefeedStartTs implements T0Selector.ChangefeedStartTs {

    private static final Logger LOG = Logger.getLogger("outlet-ticdc");

    private final String baseUrl;
    private final String changefeedId;
    private final HttpClient http;
    private final Duration timeout;
    private volatile long cachedStartTs = -1L;

    public TiCdcChangefeedStartTs(String baseUrl, String changefeedId, Duration timeout) {
        this.baseUrl = baseUrl.endsWith("/") ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl;
        this.changefeedId = changefeedId;
        this.timeout = timeout;
        this.http = HttpClient.newBuilder().connectTimeout(timeout).build();
    }

    public TiCdcChangefeedStartTs(String baseUrl, String changefeedId) {
        this(baseUrl, changefeedId, Duration.ofSeconds(5));
    }

    @Override
    public long startTs(String topic) {
        long cached = cachedStartTs;
        if (cached >= 0) return cached;
        String body = get(baseUrl + "/api/v2/changefeeds/" + changefeedId);
        String v = FlatJson.string(body, "start_ts");
        if (v == null || v.isBlank()) {
            throw new IllegalStateException("TiCDC changefeed " + changefeedId + " 响应缺 start_ts:" + body);
        }
        long ts;
        try {
            ts = Long.parseLong(v.trim());
        } catch (NumberFormatException e) {
            throw new IllegalStateException("TiCDC start_ts 非法:" + v, e);
        }
        cachedStartTs = ts;
        LOG.info(() -> "changefeed " + changefeedId + " start_ts=" + ts + " (topic=" + topic + ")");
        return ts;
    }

    private String get(String url) {
        try {
            HttpRequest req = HttpRequest.newBuilder(URI.create(url)).timeout(timeout).GET().build();
            HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() / 100 != 2) {
                throw new IllegalStateException("HTTP " + resp.statusCode() + " " + resp.body());
            }
            return resp.body();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("TiCDC 查询中断:" + url, e);
        } catch (Exception e) {
            throw new IllegalStateException("TiCDC 查询失败:" + url, e);
        }
    }
}
