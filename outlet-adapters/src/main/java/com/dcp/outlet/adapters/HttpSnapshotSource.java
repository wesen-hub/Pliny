package com.dcp.outlet.adapters;

import com.dcp.outlet.control.Slice;
import com.dcp.outlet.control.SnapshotSource;
import com.dcp.outlet.sdk.ChangeRecord;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

/**
 * 真实快照源:走**平台既有快照读路径**(它已精确支持 {@code boundaryTso} 与分页),
 * 而不是自己拼 `AS OF TIMESTAMP` —— 后者靠墙钟换算会引入毫秒级偏差,可能造成批/流之间的空洞。
 *
 * <p>接口:{@code GET {base}/api/chg/{stream}/snapshot?consumerId=&boundaryTso=&pageToken=&limit=}
 *
 * <p><b>身份(批/增量一致性)</b>:consumerId 是<b>每次调用显式传入</b>的任务契约身份
 * ({@code contract.consumer_id},调用方由任务 contractRef 解析),与增量段同一契约 ——
 * 不再使用服务级默认身份;空值直接拒绝(fail-closed),绝不静默用错身份读。
 */
public class HttpSnapshotSource implements SnapshotSource {

    private static final Logger LOG = Logger.getLogger("outlet-snapshot");

    private final String baseUrl;
    private final HttpClient http;
    private final int pageSize;
    private final Duration timeout;

    public HttpSnapshotSource(String baseUrl, int pageSize, Duration timeout) {
        this.baseUrl = baseUrl.endsWith("/") ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl;
        this.pageSize = pageSize;
        this.timeout = timeout;
        this.http = HttpClient.newBuilder().connectTimeout(timeout).build();
    }

    public HttpSnapshotSource(String baseUrl) {
        this(baseUrl, 1000, Duration.ofSeconds(30));
    }

    /** 身份必须显式传入(任务契约的 consumer_id);空值 fail-closed。 */
    private static String requireIdentity(String consumerId) {
        if (consumerId == null || consumerId.isBlank()) {
            throw new IllegalStateException(
                    "快照读取缺契约身份(consumerId)——任务契约缺失/未注册消费方,拒绝读取(fail-closed)");
        }
        return consumerId;
    }

    /**
     * 行数估算:调平台 `GET /api/chg/{stream}/count?consumerId=&boundaryTso=`。
     * 未知 / 失败一律返回 -1(切片据此退化,不猜)。
     */
    @Override
    public long estimateRows(String consumerId, String stream, long tso) {
        String id = requireIdentity(consumerId);
        String url = baseUrl + "/api/chg/" + stream + "/count?consumerId=" + enc(id) + "&boundaryTso=" + tso;
        try {
            String body = get(url);
            String v = FlatJson.string(body, "data");
            if (v == null || v.isBlank()) return -1L;
            return Long.parseLong(v.trim());
        } catch (Exception e) {
            LOG.warning(() -> "行数估算失败(返回 -1): " + e.getMessage());
            return -1L;
        }
    }

    @Override
    public List<ChangeRecord> readSlice(String consumerId, String stream, long tso, Slice slice) {
        String id = requireIdentity(consumerId);
        List<ChangeRecord> out = new ArrayList<>();
        String pageToken = null;
        int pages = 0;
        do {
            String url = baseUrl + "/api/chg/" + stream + "/snapshot?consumerId=" + enc(id)
                    + "&boundaryTso=" + tso + "&limit=" + pageSize
                    + (pageToken == null ? "" : "&pageToken=" + enc(pageToken));
            String body = get(url);
            List<Map<String, String>> rows = FlatJson.rows(body);
            for (Map<String, String> r : rows) {
                Map<String, Object> row = new LinkedHashMap<>(r);
                out.add(new SnapshotRecord(row, tso, stream));
            }
            pageToken = FlatJson.string(body, "nextPageToken");
            if (pageToken != null && (pageToken.isBlank() || "null".equals(pageToken))) pageToken = null;
            pages++;
            if (pages > 100_000) {                      // 防御:分页 token 异常时不死循环
                throw new IllegalStateException("快照分页异常:页数超过上限");
            }
        } while (pageToken != null);
        int pagesDone = pages;
        int rowCount = out.size();
        LOG.info(() -> "snapshot slice " + slice + " rows=" + rowCount + " pages=" + pagesDone);
        return out;
    }

    private String get(String url) {
        try {
            HttpRequest req = HttpRequest.newBuilder(URI.create(url)).timeout(timeout).GET().build();
            HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() / 100 != 2) {
                throw new IllegalStateException("HTTP " + resp.statusCode() + " " + resp.body());
            }
            return resp.body();
        } catch (Exception e) {
            throw new IllegalStateException("快照读取失败:" + url, e);
        }
    }

    private static String enc(String s) {
        return URLEncoder.encode(s, StandardCharsets.UTF_8);
    }
}
